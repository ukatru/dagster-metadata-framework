import sys
import os
from pathlib import Path
from dagster import AssetExecutionContext, build_asset_context, DagsterRun

# Add the framework and factory to path
sys.path.append("/home/ukatru/github/dagster-metadata-framework/src")
sys.path.append("/home/ukatru/github/dagster-dag-factory/src")

from metadata_framework import MetadataDagsterFactory

def verify_hydration():
    print("🚀 Starting Metadata Verification (POC)...")
    
    # 1. Initialize Metadata-Aware Factory using the isolated POC project
    base_dir = Path("/home/ukatru/github/dagster-metadata-framework/poc_project")
    factory = MetadataDagsterFactory(base_dir)
    
    # 2. Build definitions
    defs = factory.build_definitions()
    print("✅ Definitions built successfully.")
    
    # 3. Find the 'cross_ref_test_asset'
    asset_name = "cross_ref_test_asset"
    asset_def = None
    # Definitions in recent Dagster are iterable or have .assets
    assets = defs.assets if hasattr(defs, 'assets') else []
    for asset in assets:
        if hasattr(asset, 'key') and asset.key.to_user_string() == asset_name:
            asset_def = asset
            break
            
    if not asset_def:
        print(f"❌ Could not find asset {asset_name}")
        return

    # 4. Simulate a Run with 'invok_id' tag
    # In a real run, Dagster provides this context automatically
    run = DagsterRun(
        run_id="test_run",
        job_name="cross_ref_test_job",
        tags={"invok_id": "TEST_INVOICE_001"}
    )
    
    # We need to reach into the factory's logic directly for verification
    # because actually executing the asset requires SFTP/S3 connections
    print(f"🧐 Validating hydration logic for job: {run.job_name}, invok: {run.tags['invok_id']}")
    
    # Manually trigger the _get_template_vars override
    from unittest.mock import MagicMock
    context = MagicMock()
    # Simulate a run with BOTH tags (the new standard)
    context.run.tags = {"invok_id": "TEST_INVOICE_001", "job_nm": "cross_ref_test_job"}
    context.job_name = "__ASSET_JOB" # Simulate Asset UI
    
    template_vars = factory.asset_factory._get_template_vars(context)
    
    # 5. ASSERTS
    if "metadata" not in template_vars:
        print("❌ FAILED: 'metadata' key missing from template vars.")
        return
        
    metadata = template_vars["metadata"]
    print(f"📊 Hydrated Metadata: {metadata}")
    
    if "_job_id" not in metadata:
        print("❌ FAILED: internal '_job_id' missing from metadata.")
        return

    print(f"✨ SUCCESS: Job ID {metadata['_job_id']} and Name {metadata['_job_nm']} correctly resolved!")

if __name__ == "__main__":
    # Ensure env is loaded for the provider
    from dotenv import load_dotenv
    load_dotenv("/home/ukatru/github/dagster-metadata-framework/.env")
    
    try:
        verify_hydration()
    except Exception as e:
        print(f"❌ Error during verification: {e}")
        import traceback
        traceback.print_exc()
