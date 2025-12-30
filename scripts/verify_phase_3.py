import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv

# Add relevant paths to sys.path
sys.path.append("/home/ukatru/github/dagster-dag-factory/src")
sys.path.append("/home/ukatru/github/dagster-metadata-framework/src")

from metadata_framework.factory import ParamsDagsterFactory
from metadata_framework.params_provider import JobParamsProvider

def verify_phase_3_sync():
    print("Testing Phase 3: Framework Sync & Active Audit...")
    
    base_dir = Path("/home/ukatru/github/example-pipelines")
    load_dotenv(base_dir / ".env")
    
    # 1. Instantiate the Factory (This triggers _sync_params_schemas via _apply_overrides)
    # We call super().build_definitions() which calls _load_all_configs then _apply_overrides
    print("🚀 Triggering Factory Build (Discovery Phase)...")
    factory = ParamsDagsterFactory(base_dir=base_dir)
    factory.build_definitions()
    
    # 2. Verify Database Content
    print("🔍 Verifying 'etl_params_schema' table...")
    provider = JobParamsProvider(base_dir)
    conn = provider._get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT job_nm, schema_json, description, is_strict, creat_by_nm, updt_by_nm 
                FROM etl_params_schema 
                WHERE job_nm = 'cross_ref_test_job'
            """)
            row = cur.fetchone()
            if not row:
                print("❌ FAIL: No entry found for 'cross_ref_test_job'")
                return
            
            job_nm, schema_json, desc, is_strict, creat_by, updt_by = row
            print(f"✅ Found job: {job_nm}")
            print(f"✅ description: {desc}")
            print(f"✅ is_strict: {is_strict}")
            print(f"✅ creat_by_nm: {creat_by}")
            print(f"✅ updt_by_nm: {updt_by}")
            
            # Print pretty JSON
            print(f"✅ Parsed JSON Schema:\n{json.dumps(schema_json, indent=2)}")
            
            # Assertions
            assert job_nm == "cross_ref_test_job"
            assert is_strict is True
            assert creat_by == "ParamsDagsterFactory.Sync"
            assert "source_path" in schema_json["properties"]
            assert schema_json["properties"]["source_path"]["default"] == "/upload"
            assert "source_path" in schema_json["required"]
            
    finally:
        conn.close()
        
    print("\n🚀 Phase 3 Sync & Active Audit Successful!")

if __name__ == "__main__":
    verify_phase_3_sync()
