import os
import sys
import json
import psycopg2
from pathlib import Path
from datetime import datetime
from unittest.mock import MagicMock

# Setup paths
BASE_DIR = Path("/home/ukatru/github/example-pipelines")
DAG_FACTORY_SRC = Path("/home/ukatru/github/dagster-dag-factory/src")
METADATA_SRC = Path("/home/ukatru/github/dagster-metadata-framework/src")

sys.path.append(str(DAG_FACTORY_SRC))
sys.path.append(str(METADATA_SRC))

from metadata_framework.factory import ParamsAssetFactory

def setup_mock_db():
    print("🗄️ Setting up mock parameter in DB...")
    from dotenv import load_dotenv
    load_dotenv()
    
    conn = psycopg2.connect(
        dbname=os.getenv('POSTGRES_DB'), 
        user=os.getenv('POSTGRES_USER'), 
        password=os.getenv('POSTGRES_PASSWORD'), 
        host=os.getenv('POSTGRES_HOST'), 
        port=os.getenv('POSTGRES_PORT')
    )
    cur = conn.cursor()
    
    # Clean up existing
    cur.execute("DELETE FROM etl_job_parameter WHERE job_nm = 'cross_ref_test_job'")
    
    # Insert new override
    mock_config = {"overridden_param": "SUCCESS_VIA_DB", "nested": {"key": 123}}
    cur.execute("""
        INSERT INTO etl_job_parameter (job_nm, team_id, org_id, config_json, creat_by_nm, creat_dttm)
        VALUES ('cross_ref_test_job', 1, 1, %s, 'test_script', %s)
    """, (json.dumps(mock_config), datetime.now()))
    
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Mock parameter inserted.")

def test_hydration():
    print("🚀 Testing Hydration...")
    asset_factory = ParamsAssetFactory(base_dir=BASE_DIR)
    
    # Mock Dagster context
    mock_context = MagicMock()
    mock_context.run.tags = {
        "platform/type": "static",
        "job_nm": "cross_ref_test_job",
        "team": "Marketplace"
    }
    mock_context.run.run_config = {}
    mock_context.log.info = print
    
    # Simulate _get_template_vars
    template_vars = asset_factory._get_template_vars(mock_context)
    
    print("\n--- Hydration Results ---")
    params = template_vars.get("params", {})
    print(f"📦 Resulting Params: {params}")
    
    if params.get("overridden_param") == "SUCCESS_VIA_DB":
        print("✨ SUCCESS: Database override correctly hydrated!")
    else:
        print("❌ FAILURE: Database override NOT found in hydrated params!")

if __name__ == "__main__":
    setup_mock_db()
    test_hydration()
