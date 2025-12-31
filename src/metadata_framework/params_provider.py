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

    def get_job_params_by_name(self, job_nm: str, team_nm: Optional[str] = None) -> Dict[str, Any]:
        """
        Fetches runtime parameters for a job by name (Collapsed Architecture).
        Checks both etl_job_instance and etl_job_definition.
        """
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                # 1. Try etl_job_instance first (UI-Created instances)
                sql = """
                    SELECT ji.params_json
                    FROM etl_job_instance ji
                    JOIN etl_team t ON ji.team_id = t.id
                    WHERE ji.instance_nm = %s AND ji.actv_ind = TRUE
                """
                params_list = [job_nm]
                if team_nm:
                    sql += " AND t.team_nm = %s"
                    params_list.append(team_nm)
                
                cur.execute(sql, tuple(params_list))
                row = cur.fetchone()
                if row:
                    return row[0] or {}

                # 2. Fallback to etl_job_definition (YAML singletons)
                sql = """
                    SELECT jd.yaml_def, jp.config_json
                    FROM etl_job_definition jd
                    JOIN etl_team t ON jd.team_id = t.id
                    LEFT JOIN etl_job_parameter jp ON jd.id = jp.job_definition_id
                    WHERE jd.job_nm = %s AND jd.actv_ind = TRUE
                """
                cur.execute(sql, tuple(params_list))
                row = cur.fetchone()
                if row:
                    # For singletons, parameters can come from YAML (rendered) or overrides table
                    # The factory handles YAML rendering, so we just return the overrides here
                    return row[1] or {}
                    
                return {}
        finally:
            conn.close()

    def get_active_schedules(self, location_nm: str, team_nm: Optional[str] = None) -> list[Dict[str, Any]]:
        """
        Fetches all active job definitions AND instances that have a valid cron schedule for a specific location.
        """
        schedules = []
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                # 1. Singletons from etl_job_definition
                sql = """
                    SELECT jd.job_nm, jd.cron_schedule, jd.partition_start_dt, jd.id
                    FROM etl_job_definition jd
                    JOIN etl_code_location cl ON jd.code_location_id = cl.id
                    JOIN etl_team t ON jd.team_id = t.id
                    WHERE cl.location_nm = %s AND jd.actv_ind = TRUE AND jd.cron_schedule IS NOT NULL
                """
                params = [location_nm]
                if team_nm:
                    sql += " AND t.team_nm = %s"
                    params.append(team_nm)
                
                cur.execute(sql, tuple(params))
                for job_nm, cron, start_dt, jd_id in cur.fetchall():
                    schedules.append({
                        "job_nm": job_nm,
                        "instance_id": f"def_{jd_id}", 
                        "cron_schedule": cron,
                        "partition_start_dt": start_dt,
                        "type": "singleton"
                    })

                # 2. Instances from etl_job_instance
                sql = """
                    SELECT ji.instance_nm, ji.cron_schedule, ji.partition_start_dt, ji.id, jt.template_nm
                    FROM etl_job_instance ji
                    JOIN etl_job_template jt ON ji.template_id = jt.id
                    JOIN etl_code_location cl ON ji.code_location_id = cl.id
                    JOIN etl_team t ON ji.team_id = t.id
                    WHERE cl.location_nm = %s AND ji.actv_ind = TRUE AND ji.cron_schedule IS NOT NULL
                """
                cur.execute(sql, tuple(params))
                for inst_nm, cron, start_dt, ji_id, templ_nm in cur.fetchall():
                    schedules.append({
                        "job_nm": inst_nm,  # The display name
                        "template_job_nm": templ_nm, # The logical job to trigger
                        "instance_id": str(ji_id),
                        "cron_schedule": cron,
                        "partition_start_dt": start_dt,
                        "type": "instance"
                    })
        finally:
            conn.close()
        return schedules

    def upsert_job_template(
        self,
        template_nm: str,
        yaml_def: Dict[str, Any],
        params_schema: Dict[str, Any],
        asset_selection: Optional[List[str]] = None,
        description: Optional[str] = None,
        team_nm: Optional[str] = None,
        location_nm: Optional[str] = None,
        by_nm: str = "ParamsDagsterFactory.Sync"
    ):
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                team_id, org_id, cl_id = self._resolve_scoping(cur, team_nm, location_nm)
                if not team_id or not cl_id:
                    raise ValueError(f"Cannot upsert template '{template_nm}': team_id={team_id}, cl_id={cl_id}")

                cur.execute("""
                    INSERT INTO etl_job_template 
                                (template_nm, yaml_def, params_schema, asset_selection, description, team_id, org_id, code_location_id, 
                                 creat_by_nm, creat_dttm, updt_by_nm, updt_dttm)
                    VALUES 
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (template_nm, team_id, code_location_id) DO UPDATE SET
                        yaml_def = EXCLUDED.yaml_def,
                        params_schema = EXCLUDED.params_schema,
                        asset_selection = EXCLUDED.asset_selection,
                        description = EXCLUDED.description,
                        updt_by_nm = EXCLUDED.creat_by_nm,
                        updt_dttm = EXCLUDED.creat_dttm
                """, (
                    template_nm, 
                    psycopg2.extras.Json(yaml_def),
                    psycopg2.extras.Json(params_schema),
                    psycopg2.extras.Json(asset_selection) if asset_selection else None,
                    description, team_id, org_id, cl_id,
                    by_nm, datetime.utcnow(), by_nm, datetime.utcnow()
                ))
            conn.commit()
        finally:
            conn.close()

    def upsert_job_definition(
        self, 
        job_nm: str, 
        yaml_def: Dict[str, Any],
        params_schema: Dict[str, Any],
        asset_selection: Optional[List[str]] = None,
        description: Optional[str] = None,
        cron_schedule: Optional[str] = None,
        team_nm: Optional[str] = None,
        location_nm: Optional[str] = None,
        is_singleton: bool = True,
        by_nm: str = "ParamsDagsterFactory.Sync"
    ):
        """Upsert a YAML-defined singleton."""
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                team_id, org_id, cl_id = self._resolve_scoping(cur, team_nm, location_nm)
                if not team_id or not cl_id:
                    raise ValueError(f"Cannot upsert job '{job_nm}': team_id={team_id}, cl_id={cl_id}")

                cur.execute("""
                    INSERT INTO etl_job_definition 
                                (job_nm, yaml_def, params_schema, asset_selection, description, cron_schedule, 
                                 team_id, org_id, code_location_id, is_singleton, creat_by_nm, creat_dttm, updt_by_nm, updt_dttm, actv_ind)
                    VALUES 
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE)
                    ON CONFLICT (job_nm, team_id, code_location_id) DO UPDATE SET
                        yaml_def = EXCLUDED.yaml_def,
                        params_schema = EXCLUDED.params_schema,
                        asset_selection = EXCLUDED.asset_selection,
                        description = EXCLUDED.description,
                        cron_schedule = EXCLUDED.cron_schedule,
                        is_singleton = EXCLUDED.is_singleton,
                        updt_by_nm = EXCLUDED.creat_by_nm,
                        updt_dttm = EXCLUDED.creat_dttm,
                        actv_ind = TRUE
                """, (
                    job_nm, 
                    psycopg2.extras.Json(yaml_def),
                    psycopg2.extras.Json(params_schema),
                    psycopg2.extras.Json(asset_selection) if asset_selection else None,
                    description, cron_schedule, team_id, org_id, cl_id, is_singleton,
                    by_nm, datetime.utcnow(), by_nm, datetime.utcnow()
                ))
            conn.commit()
        finally:
            conn.close()

    def upsert_job_instance(
        self,
        instance_nm: str,
        template_id: int,
        params_json: Dict[str, Any],
        description: Optional[str] = None,
        cron_schedule: Optional[str] = None,
        partition_start_dt: Optional[datetime] = None,
        team_id: Optional[int] = None,
        org_id: Optional[int] = None,
        cl_id: Optional[int] = None,
        by_nm: str = "Dashboard"
    ):
        """Register a user-created instance of a template."""
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO etl_job_instance 
                                (instance_nm, template_id, params_json, description, cron_schedule, partition_start_dt, 
                                 team_id, org_id, code_location_id, creat_by_nm, creat_dttm, updt_by_nm, updt_dttm, actv_ind)
                    VALUES 
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE)
                    ON CONFLICT (instance_nm, team_id, code_location_id) DO UPDATE SET
                        template_id = EXCLUDED.template_id,
                        params_json = EXCLUDED.params_json,
                        description = EXCLUDED.description,
                        cron_schedule = EXCLUDED.cron_schedule,
                        partition_start_dt = EXCLUDED.partition_start_dt,
                        updt_by_nm = EXCLUDED.creat_by_nm,
                        updt_dttm = EXCLUDED.creat_dttm,
                        actv_ind = TRUE
                    RETURNING id
                """, (
                    instance_nm, template_id, psycopg2.extras.Json(params_json),
                    description, cron_schedule, partition_start_dt,
                    team_id, org_id, cl_id,
                    by_nm, datetime.utcnow(), by_nm, datetime.utcnow()
                ))
                ji_id = cur.fetchone()[0]
            conn.commit()
            return ji_id
        finally:
            conn.close()

    def _resolve_scoping(self, cur, team_nm, location_nm):
        team_id = org_id = cl_id = None
        if team_nm:
            cur.execute("SELECT id, org_id FROM etl_team WHERE team_nm = %s", (team_nm,))
            row = cur.fetchone()
            if row: team_id, org_id = row
        
        if location_nm and team_id:
            cur.execute("SELECT id FROM etl_code_location WHERE team_id = %s AND location_nm = %s", (team_id, location_nm))
            row = cur.fetchone()
            if row: cl_id = row[0]
            
        return team_id, org_id, cl_id
