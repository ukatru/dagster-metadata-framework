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

def setup_mock_instance_db():
    print("🗄️ Setting up mock Blueprint Instance in DB...")
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
    
    # 1. Clean up
    cur.execute("DELETE FROM etl_instance_parameter")
    cur.execute("DELETE FROM etl_job_instance")
    
    # 2. Re-fetch Blueprint ID (synced earlier)
    cur.execute("SELECT id FROM etl_blueprint WHERE blueprint_nm = 'sftp_to_s3_template'")
    blueprint_id = cur.fetchone()[0]
    
    # 3. Insert Job Instance (The 'Invocation')
    # Using instance_id = 'INST-001'
    cur.execute("""
        INSERT INTO etl_job_instance (blueprint_id, instance_id, team_id, org_id, creat_by_nm, creat_dttm)
        VALUES (%s, 'INST-001', 1, 1, 'test_script', %s)
        RETURNING id
    """, (blueprint_id, datetime.now()))
    instance_pk = cur.fetchone()[0]
    
    # 4. Insert Instance Parameters
    mock_config = {"blueprint_param": "SUCCESS_VIA_INSTANCE", "mode": "blueprint-test"}
    cur.execute("""
        INSERT INTO etl_instance_parameter (instance_pk, team_id, org_id, config_json, creat_by_nm, creat_dttm)
        VALUES (%s, 1, 1, %s, 'test_script', %s)
    """, (instance_pk, json.dumps(mock_config), datetime.now()))
    
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Mock instance and parameters inserted.")

def test_instance_hydration():
    print("🚀 Testing Instance Hydration...")
    asset_factory = ParamsAssetFactory(base_dir=BASE_DIR)
    
    # Mock Dagster context
    mock_context = MagicMock()
    mock_context.job_name = "sftp_to_s3_job"
    # Tags representing a Blueprint Invocation
    mock_context.run.tags = {
        "instance_id": "INST-001",
        "team": "Marketplace"
    }
    mock_context.run.run_config = {}
    mock_context.log.info = print
    
    # Simulate _get_template_vars
    template_vars = asset_factory._get_template_vars(mock_context)
    params = template_vars.get("params", {})
    
    print("\n--- Hydration Results ---")
    print(f"📦 Resulting Params: {params}")
    
    if params.get("blueprint_param") == "SUCCESS_VIA_INSTANCE":
        print("✨ SUCCESS: Blueprint instance parameters correctly hydrated!")
    else:
        print("❌ FAILURE: Blueprint instance parameters NOT found!")

if __name__ == "__main__":
    setup_mock_instance_db()
    test_instance_hydration()
