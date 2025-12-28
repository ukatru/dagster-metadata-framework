import os
import psycopg2
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

    def get_job_params(self, job_nm: str, invok_id: str) -> Dict[str, Any]:
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
                    cur.execute("""
                        SELECT id, job_nm, source_conn_nm, target_conn_nm 
                        FROM etl_job 
                        WHERE invok_id = %s AND actv_ind = TRUE
                        ORDER BY created_at DESC LIMIT 1
                    """, (invok_id,))
                else:
                    cur.execute("""
                        SELECT id, job_nm, source_conn_nm, target_conn_nm 
                        FROM etl_job 
                        WHERE job_nm = %s AND invok_id = %s AND actv_ind = TRUE
                    """, (job_nm, invok_id))
                
                job_row = cur.fetchone()
                if not job_row:
                    return params

                # We include the internal ID and Job Name in the parameters for traceability
                job_id, final_job_nm, src_conn, tgt_conn = job_row
                params["_job_id"] = job_id
                params["_job_nm"] = final_job_nm
                params["source_conn_nm"] = src_conn
                params["target_conn_nm"] = tgt_conn

                # 4. Fetch Job Specific Parameters (Highest Priority)
                cur.execute("SELECT config_json FROM etl_job_parameter WHERE etl_job_id = %s", (job_id,))
                job_param_row = cur.fetchone()
                if job_param_row:
                    params.update(job_param_row[0])

        finally:
            conn.close()

        return params

    def get_active_schedules(self) -> list[Dict[str, Any]]:
        """
        Fetches all active jobs that have a valid cron schedule.
        """
        schedules = []
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT job_nm, invok_id, cron_schedule 
                    FROM etl_job 
                    WHERE actv_ind = TRUE AND cron_schedule IS NOT NULL
                """)
                for job_nm, invok_id, cron in cur.fetchall():
                    schedules.append({
                        "job_nm": job_nm,
                        "invok_id": invok_id,
                        "cron_schedule": cron
                    })
        finally:
            conn.close()
        return schedules
