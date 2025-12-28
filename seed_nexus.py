import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from pathlib import Path

from metadata_framework.models import (
    Base, ETLConnection, ETLJob, ETLJobParameter, ETLParameter
)

# Load env
load_dotenv(Path(__file__).parent / ".env")

def seed():
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "postgres")
    db_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"

    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # 1. Connections
        sftp_prod = ETLConnection(conn_nm="sftp_prod", conn_type="SFTP")
        s3_prod = ETLConnection(conn_nm="s3_prod", conn_type="S3")
        sqlserver_prod = ETLConnection(conn_nm="sqlserver_prod", conn_type="SQLSERVER")
        snowflake_prod = ETLConnection(conn_nm="snowflake_conn", conn_type="SNOWFLAKE")
        sqlserver_mkt = ETLConnection(conn_nm="sqlserver_mkt", conn_type="SQLSERVER")
        
        session.add_all([sftp_prod, s3_prod, sqlserver_prod, snowflake_prod, sqlserver_mkt])
        session.flush()

        # 3. Global Parameters
        global_env = ETLParameter(parm_nm="env", parm_value="production")
        global_bucket = ETLParameter(parm_nm="target_bucket", parm_value="my-dagster-poc")
        session.add_all([global_env, global_bucket])

        # 4. Jobs
        
        # Pipeline 1: cross_ref_test_job
        job1 = ETLJob(
            job_nm="cross_ref_test_job",
            invok_id="TEST_001",
            source_conn_nm="sftp_prod",
            target_conn_nm="s3_prod",
            cron_schedule="18 20 * * *",
            actv_ind=True
        )
        
        # Pipeline 2: sqlserver_snowflake_incremental_job
        job2 = ETLJob(
            job_nm="sqlserver_snowflake_incremental_job",
            invok_id="INC_PROD_01",
            source_conn_nm="sqlserver_prod",
            target_conn_nm="snowflake_conn",
            cron_schedule="30 2 * * *",
            actv_ind=True
        )
        
        # Pipeline 3: multi_partition_job
        job3 = ETLJob(
            job_nm="multi_partition_job",
            invok_id="REGIONAL_DAILY",
            source_conn_nm="sqlserver_mkt",
            target_conn_nm="s3_prod",
            cron_schedule="0 0 * * *",
            actv_ind=True
        )
        
        session.add_all([job1, job2, job3])
        session.flush()

        # 5. Job Specific Parameters
        job1_param = ETLJobParameter(
            etl_job_id=job1.id,
            config_json={
                "source_path": "/home/ukatru/data",
                "target_key": "orders/{{ metadata._job_id }}/data.csv"
            }
        )
        
        job2_param = ETLJobParameter(
            etl_job_id=job2.id,
            config_json={
                "rows_chunk": 50000,
                "target_table": "FACT_SALES_INCREMENTAL"
            }
        )
        
        job3_param = ETLJobParameter(
            etl_job_id=job3.id,
            config_json={
                "source_schema": "mkt_schema",
                "retention_days": 365,
                "market_db_connect": "sqlserver_mkt"
            }
        )
        
        session.add_all([job1_param, job2_param, job3_param])
        session.commit()
        print("✅ Nexus-DB seeded successfully via SQLAlchemy!")

    except Exception as e:
        session.rollback()
        print(f"❌ Seeding failed: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    seed()
