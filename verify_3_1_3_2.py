import os
import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path("/home/ukatru/github/dagster-metadata-framework/src")))

from metadata_framework.status_provider import NexusStatusProvider
from metadata_framework.models import ETLJobStatus
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()

def verify_3_1_foundation():
    print("🧪 Verifying 3.1: Status Provider Foundation...")
    provider = NexusStatusProvider()
    
    # Test Run ID
    mock_run_id = "test-run-12345"
    
    # 1. Log Start
    print("   -> Calling log_job_start")
    batch_nbr = provider.log_job_start(
        run_id=mock_run_id,
        job_nm="verify_job",
        invok_id="VERIFY_001",
        run_mode="MANUAL"
    )
    
    if batch_nbr:
        print(f"   ✅ Created Batch Number: {batch_nbr}")
    else:
        print("   ❌ Failed to create batch record.")
        return False

    # 2. Check DB
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "postgres")
    db_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    status = session.query(ETLJobStatus).filter_by(run_id=mock_run_id).first()
    if status and status.btch_sts_cd == 'R':
        print(f"   ✅ DB Record Found in 'R' (Running) status.")
    else:
        print(f"   ❌ DB Record not found or in wrong status.")
        return False

    # 3. Log End (Success)
    print("   -> Calling log_job_end (SUCCESS)")
    provider.log_job_end(mock_run_id, status_cd='C')
    
    session.expire_all()
    status = session.query(ETLJobStatus).filter_by(run_id=mock_run_id).first()
    if status and status.btch_sts_cd == 'C':
        print(f"   ✅ DB Record updated to 'C' (Complete).")
    else:
        print(f"   ❌ DB Record update failed.")
        return False

    print("✨ Foundation (3.1) and Job Tracking (3.2) Logic Verified! ✨")
    return True

if __name__ == "__main__":
    if verify_3_1_foundation():
        sys.exit(0)
    else:
        sys.exit(1)
