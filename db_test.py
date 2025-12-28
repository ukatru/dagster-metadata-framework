import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Add src to path
sys.path.append(str(Path("/home/ukatru/github/dagster-metadata-framework/src")))

def test_db_connection():
    env_path = Path("/home/ukatru/github/dagster-metadata-framework/.env")
    print(f"Loading env from: {env_path}")
    load_dotenv(dotenv_path=env_path)
    
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    db = os.getenv("POSTGRES_DB")
    
    db_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    print(f"Connecting to: postgresql://{user}:****@{host}:{port}/{db}")
    
    try:
        engine = create_engine(db_url)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT count(*) FROM etl_job_status"))
            count = result.scalar()
            print(f"✅ Connection Successful! Current row count in etl_job_status: {count}")
            
            print("\nRecent records:")
            result = conn.execute(text("SELECT btch_nbr, run_id, job_nm, strt_dttm, btch_sts_cd FROM etl_job_status ORDER BY strt_dttm DESC LIMIT 5"))
            for row in result:
                print(row)
    except Exception as e:
        print(f"❌ Connection Failed: {e}")

if __name__ == "__main__":
    test_db_connection()
