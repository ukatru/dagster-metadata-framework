import os
import psycopg2
from pathlib import Path
from dotenv import load_dotenv

# Load env
load_dotenv(Path(__file__).parent.parent / ".env")

def get_connection():
    return psycopg2.connect(
        dbname=os.environ.get("POSTGRES_DB"),
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD"),
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432")
    )

def fix_schedules():
    conn = get_connection()
    cur = conn.cursor()

    try:
        # We target 11:00 AM Local (19:00 UTC) for testing
        # Standardize TEST_001 -> 11:00 AM Local
        cur.execute("""
            INSERT INTO etl_schedule (slug, cron, timezone) 
            VALUES (%s, %s, %s) 
            ON CONFLICT (slug) DO UPDATE SET cron = EXCLUDED.cron, timezone = EXCLUDED.timezone
            RETURNING id;
        """, ("LIVE_TEST_1100AM_LOCAL", "0 11 29 12 *", "America/Los_Angeles"))
        sched_1100_local_id = cur.fetchone()[0]

        # Standardize TEST_002 -> 7:00 PM UTC (which is also 11:00 AM Local)
        # 7:00 PM = 19:00
        cur.execute("""
            INSERT INTO etl_schedule (slug, cron, timezone) 
            VALUES (%s, %s, %s) 
            ON CONFLICT (slug) DO UPDATE SET cron = EXCLUDED.cron, timezone = EXCLUDED.timezone
            RETURNING id;
        """, ("HEARTBEAT_700PM_UTC", "0 19 29 12 *", "UTC"))
        sched_1900_utc_id = cur.fetchone()[0]

        # Assign TEST_001 to Local
        cur.execute("""
            UPDATE etl_job SET schedule_id = %s 
            WHERE job_nm = 'cross_ref_test_job' AND invok_id = 'TEST_001';
        """, (sched_1100_local_id,))

        # Assign TEST_002 to UTC
        cur.execute("""
            UPDATE etl_job SET schedule_id = %s 
            WHERE job_nm = 'cross_ref_test_job' AND invok_id = 'TEST_002';
        """, (sched_1900_utc_id,))

        # Clean up the old confusing ones if needed, or just let them be.
        # Let's deactivate them.
        cur.execute("UPDATE etl_schedule SET actv_ind = FALSE WHERE slug NOT IN ('LIVE_TEST_1100AM_LOCAL', 'HEARTBEAT_700PM_UTC');")

        conn.commit()
        print("✅ Schedules fixed and standardized!")
        print("   TEST_001 -> 11:00 AM America/Los_Angeles (cron: '0 11')")
        print("   TEST_002 -> 7:00 PM UTC (cron: '0 19')")
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Fix failed: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    fix_schedules()
