import os
import sys
from pathlib import Path

# Setup paths
BASE_DIR = Path("/home/ukatru/github/example-pipelines")
DAG_FACTORY_SRC = Path("/home/ukatru/github/dagster-dag-factory/src")
METADATA_SRC = Path("/home/ukatru/github/dagster-metadata-framework/src")

sys.path.append(str(DAG_FACTORY_SRC))
sys.path.append(str(METADATA_SRC))

from metadata_framework.factory import ParamsDagsterFactory

def test_static_tagging():
    print("🚀 Initializing ParamsDagsterFactory...")
    factory = ParamsDagsterFactory(base_dir=BASE_DIR)
    
    print("🔍 Building Definitions...")
    defs = factory.build_definitions()
    
    print("\n--- Job Tags Verification ---")
    target_job = "cross_ref_test_job"
    found = False
    
    for job in defs.jobs:
        if job.name == target_job:
            found = True
            print(f"✅ Found Job: {job.name}")
            print(f"🏷️ Tags: {job.tags}")
            
            if job.tags.get("platform/type") == "static":
                print("✨ SUCCESS: platform/type: static tag found!")
            else:
                print("❌ FAILURE: platform/type: static tag MISSING!")
    
    if not found:
        print(f"❌ FAILURE: {target_job} NOT found in definitions!")

if __name__ == "__main__":
    test_static_tagging()
