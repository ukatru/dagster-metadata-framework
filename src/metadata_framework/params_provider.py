import os
import psycopg2
import psycopg2.extras
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List
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

    def get_job_params(
        self, 
        job_nm: str, 
        instance_id: Optional[str] = None, 
        team_nm: Optional[str] = None,
        is_static: bool = False
    ) -> Dict[str, Any]:
        """
        Two-Tier Parameter Hydration.
        Priority: Global -> Static Override (if static) -> Instance Parameter (if instance)
        """
        params = {}
        conn = self._get_connection()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                # 1. Tier 0: Global Parameters (Catch-all defaults)
                cur.execute("SELECT parm_nm, parm_value FROM etl_parameter")
                for row in cur.fetchall():
                    params[row['parm_nm']] = row['parm_value']

                # 2. Tier 1: Static Overrides (Metadata-Driven 1:1)
                # Matches by Job Definition ID.
                if is_static and team_nm:
                    sql = """
                        SELECT p.config_json 
                        FROM etl_job_parameter p
                        JOIN etl_job_definition d ON p.job_definition_id = d.id
                        JOIN etl_team t ON d.team_id = t.id
                        WHERE d.job_nm = %s AND t.team_nm = %s
                    """
                    cur.execute(sql, (job_nm, team_nm))
                    row = cur.fetchone()
                    if row:
                        params.update(row['config_json'])

                # 3. Tier 2: Instance Parameters (Blueprint Invocations 1:N)
                # Matches by String instance_id.
                if instance_id:
                    sql = """
                        SELECT p.config_json, i.id as instance_pk
                        FROM etl_instance_parameter p
                        JOIN etl_job_instance i ON p.instance_pk = i.id
                        JOIN etl_team t ON i.team_id = t.id
                        WHERE i.instance_id = %s
                    """
                    params_list = [instance_id]
                    if team_nm:
                        sql += " AND t.team_nm = %s"
                        params_list.append(team_nm)
                    
                    cur.execute(sql, tuple(params_list))
                    row = cur.fetchone()
                    if row:
                        params.update(row['config_json'])
                        params["_instance_pk"] = row['instance_pk']
                        params["_instance_id"] = instance_id

                # Traceability
                params["_job_nm"] = job_nm

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
                # 1. Fetch from Instances (Shared patterns)
                sql_instances = """
                    SELECT d.job_nm, j.instance_id, j.cron_schedule, j.partition_start_dt 
                    FROM etl_job_instance j
                    JOIN etl_job_definition d ON j.job_definition_id = d.id
                    JOIN etl_team t ON j.team_id = t.id
                    WHERE j.actv_ind = TRUE AND j.cron_schedule IS NOT NULL
                """
                
                # 2. Fetch from Static Overrides (1:1 Jobs)
                sql_static = """
                    SELECT d.job_nm, 'STATIC' as instance_id, p.cron_schedule, p.partition_start_dt
                    FROM etl_job_parameter p
                    JOIN etl_job_definition d ON p.job_definition_id = d.id
                    JOIN etl_team t ON d.team_id = t.id
                    WHERE p.cron_schedule IS NOT NULL
                """
                
                sql = f"({sql_instances}) UNION ALL ({sql_static})"
                params_list = []
                if team_nm:
                    sql = f"SELECT * FROM ({sql}) as combined WHERE team_nm = %s"
                    # Wait, team_nm is not in the select list of the subqueries. Let's fix.
                    
                # Refined SQL with team filtering
                sql = """
                    SELECT * FROM (
                        SELECT d.job_nm, j.instance_id, j.cron_schedule, j.partition_start_dt, t.team_nm
                        FROM etl_job_instance j
                        JOIN etl_job_definition d ON j.job_definition_id = d.id
                        JOIN etl_team t ON j.team_id = t.id
                        WHERE j.actv_ind = TRUE AND j.cron_schedule IS NOT NULL
                        UNION ALL
                        SELECT d.job_nm, 'STATIC' as instance_id, p.cron_schedule, p.partition_start_dt, t.team_nm
                        FROM etl_job_parameter p
                        JOIN etl_job_definition d ON p.job_definition_id = d.id
                        JOIN etl_team t ON d.team_id = t.id
                        WHERE p.cron_schedule IS NOT NULL
                    ) as combined
                """
                if team_nm:
                    sql += " WHERE team_nm = %s"
                    params_list.append(team_nm)
                
                cur.execute(sql, tuple(params_list))
                for job_nm, instance_id, cron, start_dt, t_nm in cur.fetchall():
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
        Fetches centralized heartbeats (etl_schedule) and their linked jobs.
        Returns a list of dicts: {slug, cron, timezone, jobs: [{name, tags}]}
        """
        schedules = {}
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                # 1. Fetch all active schedules and their linked active jobs in one join
                sql = """
                    SELECT s.slug, s.cron, s.timezone, d.job_nm, j.instance_id
                    FROM etl_schedule s
                    JOIN etl_job_instance j ON s.id = j.schedule_id
                    JOIN etl_job_definition d ON j.job_definition_id = d.id
                    JOIN etl_team t ON j.team_id = t.id
                    WHERE s.actv_ind = TRUE AND j.actv_ind = TRUE
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
        file_loc: str,
        file_hash: str,
        yaml_content: str,
        description: Optional[str] = None,
        params_schema: Optional[Dict[str, Any]] = None,
        asset_selection: Optional[List[str]] = None,
        team_nm: Optional[str] = None,
        location_nm: Optional[str] = None,
        blueprint_ind: bool = False,
        by_nm: str = "ParamsDagsterFactory.Sync"
    ) -> int:
        """
        Upserts a job definition (Static or Blueprint) into the database.
        Returns the ID of the record.
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
                                (job_nm, yaml_def, file_loc, file_hash, yaml_content, 
                                 description, params_schema, asset_selection, 
                                 team_id, org_id, code_location_id, actv_ind, blueprint_ind,
                                 creat_by_nm, creat_dttm, updt_by_nm, updt_dttm)
                    VALUES 
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, %s, %s, %s, %s, %s)
                    ON CONFLICT (job_nm, team_id, code_location_id) DO UPDATE SET
                        yaml_def = EXCLUDED.yaml_def,
                        file_loc = EXCLUDED.file_loc,
                        file_hash = EXCLUDED.file_hash,
                        yaml_content = EXCLUDED.yaml_content,
                        description = EXCLUDED.description,
                        params_schema = EXCLUDED.params_schema,
                        asset_selection = EXCLUDED.asset_selection,
                        blueprint_ind = EXCLUDED.blueprint_ind,
                        actv_ind = TRUE,
                        updt_by_nm = EXCLUDED.creat_by_nm,
                        updt_dttm = EXCLUDED.creat_dttm
                    RETURNING id
                """, (
                    job_nm, 
                    psycopg2.extras.Json(yaml_def),
                    file_loc,
                    file_hash,
                    yaml_content,
                    description,
                    psycopg2.extras.Json(params_schema) if params_schema else None,
                    psycopg2.extras.Json(asset_selection) if asset_selection else None,
                    team_id,
                    org_id,
                    code_location_id,
                    blueprint_ind,
                    by_nm, 
                    datetime.utcnow(),
                    by_nm,
                    datetime.utcnow()
                ))
                return_id = cur.fetchone()[0]
            conn.commit()
            return return_id
        finally:
            conn.close()

    def upsert_params_schema_by_id(
        self, 
        job_definition_id: int, 
        json_schema: Dict[str, Any], 
        description: Optional[str] = None,
        is_strict: bool = False,
        team_nm: Optional[str] = None,
        location_nm: Optional[str] = None,
        by_nm: str = "ParamsDagsterFactory.Sync"
    ):
        """
        Upserts a developer contract linked by ID.
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
                                (job_definition_id, json_schema, description, is_strict, team_id, org_id, code_location_id, 
                                 creat_by_nm, creat_dttm, updt_by_nm, updt_dttm)
                    VALUES 
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (job_definition_id) DO UPDATE SET
                        json_schema = EXCLUDED.json_schema,
                        description = EXCLUDED.description,
                        is_strict = EXCLUDED.is_strict,
                        org_id = EXCLUDED.org_id,
                        updt_by_nm = EXCLUDED.creat_by_nm,
                        updt_dttm = EXCLUDED.creat_dttm
                """, (
                    job_definition_id, 
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

