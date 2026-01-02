import os
from pathlib import Path
from metadata_framework.params_provider import JobParamsProvider

provider = JobParamsProvider(Path.cwd())
schedules = provider.get_active_schedules()
print(f"Found {len(schedules)} active schedules:")
for s in schedules:
    print(f" - {s['job_nm']} (Instance: {s['instance_id']}): {s['cron_schedule']}")

# Also check raw DB content for instances
import psycopg2
conn = psycopg2.connect(host=os.getenv('PGHOST', 'localhost'),
                        database=os.getenv('PGDATABASE', 'niagara'),
                        user=os.getenv('PGUSER', 'niagara_user'),
                        password=os.getenv('PGPASSWORD', 'niagara_pass'))
with conn.cursor() as cur:
    cur.execute("SELECT id, job_nm, blueprint_ind FROM etl_job_definition")
    defs = cur.fetchall()
    print("\nJob Definitions:")
    for d in defs:
        print(f" - ID: {d[0]}, Name: {d[1]}, Blueprint: {d[2]}")
        
    cur.execute("SELECT id, job_definition_id, instance_id, cron_schedule, actv_ind FROM etl_job_instance")
    insts = cur.fetchall()
    print("\nJob Instances:")
    for i in insts:
        print(f" - ID: {i[0]}, DefID: {i[1]}, InstanceID: {i[2]}, Cron: {i[3]}, Active: {i[4]}")
