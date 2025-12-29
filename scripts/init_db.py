import os
import psycopg2
from psycopg2.extras import Json
from pathlib import Path

# Load .env manually to avoid dependencies
def load_env(path):
    print(f"DEBUG: Reading .env from {path}")
    if not os.path.exists(path):
        print(f"⚠️  Warning: .env file not found at {path}")
        return
    content = Path(path).read_text()
    print(f"DEBUG: File content length: {len(content)}")
    lines = content.splitlines()
    print(f"DEBUG: Number of lines: {len(lines)}")
    for line in lines:
        clean_line = line.strip()
        print(f"DEBUG: Line starts with: {clean_line[:5]}... (len: {len(clean_line)})")
        if "=" in clean_line and not clean_line.startswith("#"):
            k, v = clean_line.split("=", 1)
            os.environ[k.strip()] = v.strip().strip('"').strip("'")
            print(f"DEBUG: Processed key: {k.strip()}")

# Load environment
env_path = Path(__file__).parent.parent / ".env"
load_env(env_path)

print(f"DEBUG: Loaded keys: {[k for k in ['POSTGRES_DB', 'POSTGRES_USER', 'POSTGRES_PASSWORD', 'POSTGRES_HOST', 'POSTGRES_PORT'] if k in os.environ]}")

def get_connection():
    # Print if password is missing (don't print the password itself)
    if not os.environ.get("POSTGRES_PASSWORD"):
        print("❌ POSTGRES_PASSWORD is missing or empty in environment")
    return psycopg2.connect(
        dbname=os.environ.get("POSTGRES_DB"),
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD"),
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432")
    )

def init_db():
    conn = get_connection()
    cur = conn.cursor()

    print("🚀 Initializing Decoupled JSONB Schema...")

    # 1. DROP (For Clean POC)
    cur.execute("DROP TABLE IF EXISTS etl_job_parameter CASCADE;")
    cur.execute("DROP TABLE IF EXISTS etl_job CASCADE;")
    cur.execute("DROP TABLE IF EXISTS etl_schedule CASCADE;") # New schedule table
    cur.execute("DROP TABLE IF EXISTS etl_connection CASCADE;")
    cur.execute("DROP TABLE IF EXISTS etl_asset_status CASCADE;")
    cur.execute("DROP TABLE IF EXISTS etl_job_status CASCADE;")
    cur.execute("DROP TABLE IF EXISTS etl_parameter CASCADE;")

    # 2. CREATE
    cur.execute("""
        CREATE TABLE etl_connection (
            id SERIAL PRIMARY KEY,
            conn_nm VARCHAR(255) UNIQUE NOT NULL,
            conn_type VARCHAR(50) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    cur.execute("""
        CREATE TABLE etl_schedule (
            id SERIAL PRIMARY KEY,
            slug VARCHAR(255) UNIQUE NOT NULL,
            cron VARCHAR(100) NOT NULL,
            timezone VARCHAR(100),
            actv_ind BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    cur.execute("""
        CREATE TABLE etl_job (
            id SERIAL PRIMARY KEY,
            job_nm VARCHAR(255) NOT NULL,
            invok_id VARCHAR(255) NOT NULL,
            source_conn_nm VARCHAR(255) REFERENCES etl_connection(conn_nm),
            target_conn_nm VARCHAR(255) REFERENCES etl_connection(conn_nm),
            schedule_id INTEGER REFERENCES etl_schedule(id), -- Linked to centralized schedule
            cron_schedule VARCHAR(100), -- Legacy
            partition_start_dt DATE,
            actv_ind BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uq_job_invok UNIQUE (job_nm, invok_id)
        );
    """)

    cur.execute("""
        CREATE TABLE etl_job_parameter (
            id SERIAL PRIMARY KEY,
            etl_job_id INTEGER UNIQUE REFERENCES etl_job(id) ON DELETE CASCADE,
            config_json JSONB NOT NULL DEFAULT '{}',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    cur.execute("""
        CREATE TABLE etl_parameter (
            id SERIAL PRIMARY KEY,
            parm_nm VARCHAR(255) UNIQUE NOT NULL,
            parm_value TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    cur.execute("""
        CREATE TABLE etl_job_status (
            btch_nbr BIGSERIAL PRIMARY KEY,
            run_id VARCHAR(64) UNIQUE NOT NULL,
            job_nm VARCHAR(256) NOT NULL,
            invok_id VARCHAR(255),
            strt_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            end_dttm TIMESTAMP,
            btch_sts_cd CHAR(1) DEFAULT 'R',
            run_mde_txt VARCHAR(50) NOT NULL,
            updt_by_nm VARCHAR(30),
            updt_dttm TIMESTAMP,
            creat_by_nm VARCHAR(30) DEFAULT 'DAGSTER',
            creat_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    cur.execute("""
        CREATE TABLE etl_asset_status (
            id BIGSERIAL PRIMARY KEY,
            btch_nbr BIGINT REFERENCES etl_job_status(btch_nbr) ON DELETE CASCADE,
            asset_nm VARCHAR(256) NOT NULL,
            parent_assets JSONB,
            config_json JSONB,
            partition_key VARCHAR(255),
            dagster_event_type VARCHAR(50),
            strt_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            end_dttm TIMESTAMP,
            asset_sts_cd CHAR(1) DEFAULT 'R',
            err_msg_txt TEXT,
            creat_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("✨ Database successfully initialized!")

if __name__ == "__main__":
    try:
        init_db()
    except Exception as e:
        print(f"❌ Error initializing database: {e}")
