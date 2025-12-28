import os
import uuid
import logging
from pathlib import Path
from dotenv import load_dotenv
from dagster import build_asset_context, DagsterInstance

from metadata_framework.factory import MetadataDagsterFactory
from metadata_framework.extensions.observability import NexusObservability
from metadata_framework.models import ETLJobStatus, ETLAssetStatus
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Setup logging
logging.basicConfig(level=logging.INFO)
load_dotenv(Path(__file__).parent / ".env")

def verify_nexus():
    print("🚀 Starting Nexus End-to-End Verification...")
    
    # 1. Initialize Nexus Observability
    obs = NexusObservability()
    
    # 2. Build Factory with Observability
    # Use the local pipelines directory (assuming current context)
    pipelines_dir = Path("/home/ukatru/github/example-pipelines")
    factory = MetadataDagsterFactory(str(pipelines_dir), observability=obs)
    
    # 3. Build Definitions
    defs = factory.build_definitions()
    print(f"✅ Built definitions with {len(defs.assets)} assets.")

    # 4. Simulate a Run of 'cross_ref_test_job'
    # We'll use the 'cross_ref_test_asset' and track it manually as if it were in a run
    run_id = str(uuid.uuid4())
    job_nm = "cross_ref_test_job"
    invok_id = "TEST_001"
    
    print(f"🏃 Simulating Run: {run_id} for {job_nm}...")
    
    # A) Log Job Start (Normally done by RunStatusSensor)
    obs.provider.log_job_start(run_id, job_nm, invok_id, "MANUAL")
    
    # B) Execute the asset (which is wrapped by Nexus)
    asset_name = "cross_ref_test_asset"
    target_asset = next(a for a in defs.assets if a.key.to_user_string() == asset_name)
    
    # We need a context with the correct tags
    # Dagster's materialize() helper executes the compute function.
    # Because we wrapped the operator.execute in MetadataAssetFactory, 
    # it should log the asset status automatically.
    
    try:
        instance = DagsterInstance.ephemeral()
        # Mock tags for the materialization
        from dagster import AssetKey, materialize
        
        result = materialize(
            [target_asset], 
            instance=instance,
            tags={"invok_id": invok_id, "job_nm": job_nm},
            run_id=run_id # Force the run_id
        )
        
        if result.success:
            print("✅ Dagster materialization successful!")
            obs.provider.log_job_end(run_id, status_cd='C')
        else:
            print("❌ Dagster materialization failed!")
            obs.provider.log_job_end(run_id, status_cd='A')

    except Exception as e:
        print(f"❌ Execution error: {e}")
        obs.provider.log_job_end(run_id, status_cd='A')

    # 5. Verify Postgres Records
    print("\n📊 Verification Results (Postgres):")
    engine = create_engine(obs.provider.engine.url)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        job_status = session.query(ETLJobStatus).filter_by(run_id=run_id).first()
        if job_status:
            print(f"  [Job Status] BTCH: {job_status.btch_nbr} | Job: {job_status.job_nm} | Status: {job_status.btch_sts_cd}")
        else:
            print("  ❌ ERROR: No Job Status record found!")

        asset_status = session.query(ETLAssetStatus).filter_by(btch_nbr=job_status.btch_nbr if job_status else None).first()
        if asset_status:
            print(f"  [Asset Status] ID: {asset_status.id} | Asset: {asset_status.asset_nm} | Status: {asset_status.asset_sts_cd}")
            print(f"  [Config Snapshot] (Keys): {list(asset_status.config_json.keys())}")
        else:
            print("  ❌ ERROR: No Asset Status record found!")

    finally:
        session.close()

if __name__ == "__main__":
    verify_nexus()
