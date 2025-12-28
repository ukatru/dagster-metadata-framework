import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from dagster import build_asset_context, DagsterInstance, AssetKey

# Load env from framework dir
FRAMEWORK_DIR = Path("/home/ukatru/github/dagster-metadata-framework")
load_dotenv(FRAMEWORK_DIR / ".env")

# Add paths
sys.path.append(str(FRAMEWORK_DIR / "src"))
sys.path.append("/home/ukatru/github/dagster-dag-factory/src")

from metadata_framework.factory import MetadataDagsterFactory
from metadata_framework.metadata_provider import MetadataProvider

def verify_phase2():
    print("📋 Starting Multi-Pipeline Enterprise Verification...")
    
    pipelines_dir = Path("/home/ukatru/github/example-pipelines")
    factory = MetadataDagsterFactory(str(pipelines_dir))
    
    # Build definitions once
    print("\n📍 Step 1: Building Definitions & Dynamic Schedules...")
    defs = factory.build_definitions()
    print(f"✅ Definitions built with {len(defs.jobs)} jobs and {len(defs.schedules)} schedules.")

    # List of test cases: (job_nm, invok_id, expected_meta_keys)
    test_cases = [
        ("cross_ref_test_job", "TEST_001", ["source_path", "target_key", "target_bucket", "source_conn_nm", "target_conn_nm"]),
        ("sqlserver_snowflake_incremental_job", "INC_PROD_01", ["rows_chunk", "target_table", "source_conn_nm", "target_conn_nm"]),
        ("multi_partition_job", "REGIONAL_DAILY", ["source_schema", "retention_days", "target_bucket", "market_db_connect"])
    ]

    for job_nm, invok_id, req_keys in test_cases:
        print(f"\n🚀 Testing Pipeline: {job_nm} (Invok: {invok_id})")
        
        # A) Check Schedule Discovery
        sched_name = f"{job_nm}_{invok_id}_schedule"
        schedule = next((s for s in defs.schedules if s.name == sched_name), None)
        if schedule:
            print(f"   ✅ Schedule Found: {schedule.name} (Cron: {schedule.cron_schedule})")
        else:
            print(f"   ❌ FAILED: Schedule {sched_name} not found.")
            continue

        # B) Check Hydration (Flat Merge)
        from unittest.mock import MagicMock
        context = MagicMock()
        context.run.tags = {"invok_id": invok_id, "job_nm": job_nm}
        context.job_name = job_nm
        context.log = MagicMock()
        
        template_vars = factory.asset_factory._get_template_vars(context)
        
        if "metadata" in template_vars:
            metadata = template_vars["metadata"]
            print(f"   ✅ Metadata Hydrated: {len(metadata)} keys.")
            
            missing = [k for k in req_keys if k not in metadata]
            if missing:
                print(f"   ❌ FAILED: Missing keys: {missing}")
            else:
                print(f"   ✨ All required keys present: {req_keys}")
                
            # C) Test a Jinja snippet if applicable
            if "_job_nm" in metadata:
                 print(f"   🔗 Internal Traceability: _job_id={metadata['_job_id']}, _job_nm={metadata['_job_nm']}")
        else:
            print("   ❌ FAILED: 'metadata' key missing from template_vars.")

    print("\n✨ MULTI-PIPELINE PHASE 2 VERIFICATION COMPLETE. ✨")

if __name__ == "__main__":
    try:
        verify_phase2()
    except Exception as e:
        print(f"🔥 Verification crashed: {e}")
        import traceback
        traceback.print_exc()
