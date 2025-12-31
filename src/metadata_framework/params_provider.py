import os
import psycopg2
import psycopg2.extras
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

class JobParamsProvider:
    def __init__(self, base_dir: Optional[Path] = None):
        # Load environment variables if base_dir is provided
        if base_dir:
            env_path = base_dir / ".env"
            if env_path.exists():
                load_dotenv(env_path)
        
        self.db_params = {
            "dbname": os.environ.get("POSTGRES_DB"),
            "user": os.environ.get("POSTGRES_USER"),
            "password": os.environ.get("POSTGRES_PASSWORD"),
            "host": os.environ.get("POSTGRES_HOST", "localhost"),
            "port": os.environ.get("POSTGRES_PORT", "5432")
        }

    def _get_connection(self):
        if not self.db_params.get("password"):
            raise ValueError("POSTGRES_PASSWORD is not set in environment")
        return psycopg2.connect(**self.db_params)

    def get_job_params(self, job_nm: str, instance_id: str, team_nm: Optional[str] = None) -> Dict[str, Any]:
        """
        Fetches and merges job parameters from the new normalized schema.
        Priority: Global -> Connection -> Job Instance
        """
        params = {}
        # 🟢 Internal logic: If job_nm is generic, we fall back to instance_id only lookup.
        is_generic = job_nm == "__ASSET_JOB" or not job_nm
        
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                # 1. Global Parameters
                cur.execute("SELECT parm_nm, parm_value FROM etl_parameter")
                for nm, val in cur.fetchall():
                    params[nm] = val

                # 2. Resolve Instance and Definition
                if is_generic:
                    sql = """
                        SELECT ji.id, jd.job_nm, ji.job_definition_id
                        FROM etl_job_instance ji
                        JOIN etl_job_definition jd ON ji.job_definition_id = jd.id
                        JOIN etl_team t ON jd.team_id = t.id
                        WHERE ji.instance_id = %s AND ji.actv_ind = TRUE
                    """
                    params_list = [instance_id]
                    if team_nm:
                        sql += " AND t.team_nm = %s"
                        params_list.append(team_nm)
                    sql += " ORDER BY ji.creat_dttm DESC LIMIT 1"
                    cur.execute(sql, tuple(params_list))
                else:
                    sql = """
                        SELECT ji.id, jd.job_nm, ji.job_definition_id
                        FROM etl_job_instance ji
                        JOIN etl_job_definition jd ON ji.job_definition_id = jd.id
                        JOIN etl_team t ON jd.team_id = t.id
                        WHERE jd.job_nm = %s AND ji.instance_id = %s AND ji.actv_ind = TRUE
                    """
                    params_list = [job_nm, instance_id]
                    if team_nm:
                        sql += " AND t.team_nm = %s"
                        params_list.append(team_nm)
                    cur.execute(sql, tuple(params_list))
                
                job_row = cur.fetchone()
                if not job_row:
                    return params

                ji_id, final_job_nm, jd_id = job_row
                params["_job_instance_id"] = ji_id
                params["_job_definition_id"] = jd_id
                params["_job_nm"] = final_job_nm

                # 3. Fetch Instance Specific Parameters (Highest Priority)
                cur.execute("SELECT config_json FROM etl_job_parameter WHERE job_instance_id = %s", (ji_id,))
                job_param_row = cur.fetchone()
                if job_param_row:
                    params.update(job_param_row[0])

        finally:
            conn.close()

        return params

    def get_active_schedules(self, team_nm: Optional[str] = None) -> list[Dict[str, Any]]:
        """
        Fetches all active job instances that have a valid cron schedule.
        """
        schedules = []
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                sql = """
                    SELECT jd.job_nm, ji.instance_id, ji.cron_schedule, ji.partition_start_dt 
                    FROM etl_job_instance ji
                    JOIN etl_job_definition jd ON ji.job_definition_id = jd.id
                    JOIN etl_team t ON jd.team_id = t.id
                    WHERE ji.actv_ind = TRUE AND ji.cron_schedule IS NOT NULL
                """
                params_list = []
                if team_nm:
                    sql += " AND t.team_nm = %s"
                    params_list.append(team_nm)
                
                cur.execute(sql, tuple(params_list))
                for job_nm, instance_id, cron, start_dt in cur.fetchall():
                    schedules.append({
                        "job_nm": job_nm,
                        "instance_id": instance_id,
                        "cron_schedule": cron,
                        "partition_start_dt": start_dt
                    })
        finally:
            conn.close()
        return schedules

    def get_batch_schedules(self, team_nm: Optional[str] = None) -> list[Dict[str, Any]]:
        """
        Fetches centralized heartbeats (etl_schedule) and their linked job instances.
        """
        schedules = {}
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                sql = """
                    SELECT s.slug, s.cron, s.timezone, jd.job_nm, ji.instance_id
                    FROM etl_schedule s
                    JOIN etl_job_instance ji ON s.id = ji.schedule_id
                    JOIN etl_job_definition jd ON ji.job_definition_id = jd.id
                    JOIN etl_team t ON jd.team_id = t.id
                    WHERE s.actv_ind = TRUE AND ji.actv_ind = TRUE
                """
                params_list = []
                if team_nm:
                    sql += " AND t.team_nm = %s"
                    params_list.append(team_nm)
                
                cur.execute(sql, tuple(params_list))
                
                for slug, cron, tz, job_nm, instance_id in cur.fetchall():
                    if slug not in schedules:
                        schedules[slug] = {
                            "name": f"nexus_heartbeat_{slug.lower()}",
                            "cron": cron,
                            "timezone": tz,
                            "jobs": []
                        }
                    schedules[slug]["jobs"].append({
                        "name": job_nm,
                        "tags": {"instance_id": instance_id, "job_nm": job_nm}
                    })
        finally:
            conn.close()
            
        return list(schedules.values())

    def upsert_job_definition(
        self, 
        job_nm: str, 
        yaml_def: Dict[str, Any],
        params_schema: Dict[str, Any],
        asset_selection: Optional[List[str]] = None,
        description: Optional[str] = None,
        team_nm: Optional[str] = None,
        location_nm: Optional[str] = None,
        by_nm: str = "ParamsDagsterFactory.Sync"
    ):
        """
        Upserts a full Job Definition into the registry.
        Authoritative from YAML.
        """
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                # 🟢 Resolve scoping IDs
                team_id = None
                org_id = None
                if team_nm:
                    cur.execute("SELECT id, org_id FROM etl_team WHERE team_nm = %s", (team_nm,))
                    row = cur.fetchone()
                    if row:
                        team_id, org_id = row
                
                code_location_id = None
                if location_nm and team_id:
                    cur.execute(
                        "SELECT id FROM etl_code_location WHERE team_id = %s AND location_nm = %s", 
                        (team_id, location_nm)
                    )
                    row = cur.fetchone()
                    if row:
                        code_location_id = row[0]

                cur.execute("""
                    INSERT INTO etl_job_definition 
                                (job_nm, yaml_def, params_schema, asset_selection, description, team_id, org_id, code_location_id, 
                                 creat_by_nm, creat_dttm, updt_by_nm, updt_dttm)
                    VALUES 
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (job_nm, team_id, code_location_id) DO UPDATE SET
                        yaml_def = EXCLUDED.yaml_def,
                        params_schema = EXCLUDED.params_schema,
                        asset_selection = EXCLUDED.asset_selection,
                        description = EXCLUDED.description,
                        org_id = EXCLUDED.org_id,
                        updt_by_nm = EXCLUDED.creat_by_nm,
                        updt_dttm = EXCLUDED.creat_dttm
                """, (
                    job_nm, 
                    psycopg2.extras.Json(yaml_def),
                    psycopg2.extras.Json(params_schema),
                    psycopg2.extras.Json(asset_selection) if asset_selection else None,
                    description, 
                    team_id,
                    org_id,
                    code_location_id,
                    by_nm, 
                    datetime.utcnow(),
                    by_nm,
                    datetime.utcnow()
                ))
            conn.commit()
        finally:
            conn.close()
