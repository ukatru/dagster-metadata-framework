import os
import psycopg2
from dotenv import load_dotenv
from pathlib import Path

def get_db_conn():
    load_dotenv(Path(__file__).parent.parent / ".env")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "postgres")
    return psycopg2.connect(
        dbname=db, user=user, password=password, host=host, port=port
    )

def verify_tables():
    conn = get_db_conn()
    cur = conn.cursor()
    
    tables = [
        "etl_connection", "etl_schedule", "etl_job", 
        "etl_job_parameter", "etl_parameter", "etl_params_schema",
        "etl_job_status", "etl_asset_status"
    ]
    
    mandatory_columns = ["creat_by_nm", "creat_dttm", "updt_by_nm", "updt_dttm"]
    
    print("Verifying database schema...")
    
    for table in tables:
        print(f"\nTable: {table}")
        cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}'")
        cols = {row[0]: row[1] for row in cur.fetchall()}
        
        for m_col in mandatory_columns:
            if m_col in cols:
                print(f"  ✅ {m_col} ({cols[m_col]})")
            else:
                print(f"  ❌ {m_col} MISSING")
                
    cur.close()
    conn.close()

if __name__ == "__main__":
    verify_tables()
