import os
import psycopg2
import psycopg2.extras
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
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

    def get_job_params(self, job_nm: str, invok_id: str, team_nm: Optional[str] = None) -> Dict[str, Any]:
        """
        Fetches and merges job parameters.
        Priority: Global -> Connection -> Job
        """
        params = {}
        # 🟢 Internal logic: If job_nm is generic, we fall back to invok_id only lookup.
        # But if job_nm is specific, we MUST match both.
        is_generic = job_nm == "__ASSET_JOB" or not job_nm
        
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                # 1. Global
                cur.execute("SELECT parm_nm, parm_value FROM etl_parameter")
                for nm, val in cur.fetchall():
                    params[nm] = val

                # 2. Job + ID
                if is_generic:
                    sql = """
                        SELECT j.id, j.job_nm 
                        FROM etl_job j
                        JOIN etl_team t ON j.team_id = t.id
                        WHERE j.invok_id = %s AND j.actv_ind = TRUE
                    """
                    params_list = [invok_id]
                    if team_nm:
                        sql += " AND t.team_nm = %s"
                        params_list.append(team_nm)
                    sql += " ORDER BY j.creat_dttm DESC LIMIT 1"
                    cur.execute(sql, tuple(params_list))
                else:
                    sql = """
                        SELECT j.id, j.job_nm 
                        FROM etl_job j
                        JOIN etl_team t ON j.team_id = t.id
                        WHERE j.job_nm = %s AND j.invok_id = %s AND j.actv_ind = TRUE
                    """
                    params_list = [job_nm, invok_id]
                    if team_nm:
                        sql += " AND t.team_nm = %s"
                        params_list.append(team_nm)
                    cur.execute(sql, tuple(params_list))
                
                job_row = cur.fetchone()
                if not job_row:
                    return params

                # We include the internal ID and Job Name in the parameters for traceability
                job_id, final_job_nm = job_row
                params["_job_id"] = job_id
                params["_job_nm"] = final_job_nm

                # 4. Fetch Job Specific Parameters (Highest Priority)
                cur.execute("SELECT config_json FROM etl_job_parameter WHERE etl_job_id = %s", (job_id,))
                job_param_row = cur.fetchone()
                if job_param_row:
                    params.update(job_param_row[0])

        finally:
            conn.close()

        return params

    def get_active_schedules(self, team_nm: Optional[str] = None) -> list[Dict[str, Any]]:
        """
        Fetches all active jobs that have a valid cron schedule.
        """
        schedules = []
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                sql = """
                    SELECT j.job_nm, j.invok_id, j.cron_schedule, j.partition_start_dt 
                    FROM etl_job j
                    JOIN etl_team t ON j.team_id = t.id
                    WHERE j.actv_ind = TRUE AND j.cron_schedule IS NOT NULL
                """
                params_list = []
                if team_nm:
                    sql += " AND t.team_nm = %s"
                    params_list.append(team_nm)
                
                cur.execute(sql, tuple(params_list))
                for job_nm, invok_id, cron, start_dt in cur.fetchall():
                    schedules.append({
                        "job_nm": job_nm,
                        "invok_id": invok_id,
                        "cron_schedule": cron,
                        "partition_start_dt": start_dt
                    })
        finally:
            conn.close()
        return schedules

    def get_batch_schedules(self, team_nm: Optional[str] = None) -> list[Dict[str, Any]]:
        """
        Fetches centralized heartbeats (etl_schedule) and their linked jobs.
        Returns a list of dicts: {slug, cron, timezone, jobs: [{name, tags}]}
        """
        schedules = {}
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                # 1. Fetch all active schedules and their linked active jobs in one join
                sql = """
                    SELECT s.slug, s.cron, s.timezone, j.job_nm, j.invok_id
                    FROM etl_schedule s
                    JOIN etl_job j ON s.id = j.schedule_id
                    JOIN etl_team t ON j.team_id = t.id
                    WHERE s.actv_ind = TRUE AND j.actv_ind = TRUE
                """
                params_list = []
                if team_nm:
                    sql += " AND t.team_nm = %s"
                    params_list.append(team_nm)
                
                cur.execute(sql, tuple(params_list))
                
                for slug, cron, tz, job_nm, invok_id in cur.fetchall():
                    if slug not in schedules:
                        schedules[slug] = {
                            "name": f"nexus_heartbeat_{slug.lower()}",
                            "cron": cron,
                            "timezone": tz,
                            "jobs": []
                        }
                    schedules[slug]["jobs"].append({
                        "name": job_nm,
                        "tags": {"invok_id": invok_id, "job_nm": job_nm}
                    })
        finally:
            conn.close()
            
        return list(schedules.values())

    def upsert_params_schema(
        self, 
        job_nm: str, 
        json_schema: Dict[str, Any], 
        description: Optional[str] = None,
        is_strict: bool = False,
        team_nm: Optional[str] = None,
        location_nm: Optional[str] = None,
        by_nm: str = "ParamsDagsterFactory.Sync"
    ):
        """
        Upserts a developer contract (params schema) into the database.
        Uses explicit attribution for the audit trail.
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
                    INSERT INTO etl_params_schema 
                                (job_nm, json_schema, description, is_strict, team_id, org_id, code_location_id, 
                                 creat_by_nm, creat_dttm, updt_by_nm, updt_dttm)
                    VALUES 
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (job_nm, team_id, code_location_id) DO UPDATE SET
                        json_schema = EXCLUDED.json_schema,
                        description = EXCLUDED.description,
                        is_strict = EXCLUDED.is_strict,
                        org_id = EXCLUDED.org_id,
                        updt_by_nm = EXCLUDED.creat_by_nm,
                        updt_dttm = EXCLUDED.creat_dttm
                """, (
                    job_nm, 
                    psycopg2.extras.Json(json_schema), 
                    description, 
                    is_strict, 
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
