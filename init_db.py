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
env_path = Path("/home/ukatru/github/dagster-metadata-framework/.env")
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
    cur.execute("DROP TABLE IF EXISTS etl_connection_config CASCADE;")
    cur.execute("DROP TABLE IF EXISTS etl_job CASCADE;")
    cur.execute("DROP TABLE IF EXISTS etl_connection CASCADE;")
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
        CREATE TABLE etl_connection_config (
            id SERIAL PRIMARY KEY,
            etl_connection_id INTEGER UNIQUE REFERENCES etl_connection(id) ON DELETE CASCADE,
            config_json JSONB NOT NULL DEFAULT '{}',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    cur.execute("""
        CREATE TABLE etl_job (
            id SERIAL PRIMARY KEY,
            job_nm VARCHAR(255) NOT NULL,
            invok_id VARCHAR(255) NOT NULL,
            source_conn_nm VARCHAR(255) REFERENCES etl_connection(conn_nm),
            target_conn_nm VARCHAR(255) REFERENCES etl_connection(conn_nm),
            cron_schedule VARCHAR(100),
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

    print("✅ Tables created. Seeding data for 'cross_ref_test_job'...")

    # 3. SEED CONNECTIONS
    # SFTP PROD
    cur.execute("INSERT INTO etl_connection (conn_nm, conn_type) VALUES (%s, %s) RETURNING id;", ("sftp_prod", "SFTP"))
    sftp_id = cur.fetchone()[0]
    cur.execute("INSERT INTO etl_connection_config (etl_connection_id, config_json) VALUES (%s, %s);", 
                (sftp_id, Json({"port": 22})))

    # S3 PROD
    cur.execute("INSERT INTO etl_connection (conn_nm, conn_type) VALUES (%s, %s) RETURNING id;", ("s3_prod", "S3"))
    s3_id = cur.fetchone()[0]
    cur.execute("INSERT INTO etl_connection_config (etl_connection_id, config_json) VALUES (%s, %s);", 
                (s3_id, Json({"region_name": "us-west-2", "endpoint_url": "http://localhost:9000"})))

    # 4. SEED JOB (cross_ref_test_job for a test invoice)
    # Note: We use the JOB_NM from the YAML
    cur.execute("""
        INSERT INTO etl_job (job_nm, invok_id, source_conn_nm, target_conn_nm, cron_schedule)
        VALUES (%s, %s, %s, %s, %s) RETURNING id;
    """, ("cross_ref_test_job", "TEST_INVOICE_001", "sftp_prod", "s3_prod", "0 5 * * *"))
    job_id = cur.fetchone()[0]

    # 5. SEED JOB PARAMETERS
    # These match the {{ metadata.X }} handles in the dynamic YAML
    cur.execute("""
        INSERT INTO etl_job_parameter (etl_job_id, config_json)
        VALUES (%s, %s);
    """, (job_id, Json({
        "source_path": "/home/ukatru/data",
        "source_pattern": ".*\\.csv",
        "target_bucket": "my-dagster-poc",
        "target_key_pattern": "backups{{ source.path }}/{{ source.item.file_name }}"
    })))

    conn.commit()
    cur.close()
    conn.close()
    print("✨ Database successfully initialized and seeded!")

if __name__ == "__main__":
    try:
        init_db()
    except Exception as e:
        print(f"❌ Error initializing database: {e}")
