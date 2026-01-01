import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add src to path
sys.path.append(str(Path.cwd() / "src"))

from metadata_framework.factory import ParamsDagsterFactory
from metadata_framework.params_provider import JobParamsProvider

def verify_sync():
    # Force sync enabled
    os.environ["NEXUS_SYNC_ENABLED"] = "TRUE"
    
    base_dir = Path.cwd()
    factory = ParamsDagsterFactory(base_dir, location_name="verification_loc")
    
    # Mock some configs
    all_configs = [
        {
            "config": {
                "name": "static_pipeline_test",
                "description": "A static pipeline",
                "jobs": [{"name": "static_job", "selection": ["asset1"]}]
            },
            "file": Path("pipelines/static_pipeline.yaml")
        },
        {
            "config": {
                "blueprint": True,
                "name": "blueprint_template_test",
                "description": "A logic template",
                "params_schema": {"p1": "string!"},
                "assets": [{"name": "template_asset"}]
            },
            "file": Path("pipelines/blueprint_template.yaml")
        }
    ]
    
    # Create dummy files
    Path("pipelines").mkdir(exist_ok=True)
    for item in all_configs:
        file_path = item["file"]
        content = item["config"]
        import yaml
        with open(file_path, "w") as f:
            yaml.dump(content, f)

    print("🚀 Starting Sync Verification...")
    print(f"Configs to sync: {[c['config'].get('name') for c in all_configs]}")
    factory._sync_job_definitions(all_configs)
    print("✅ Sync logic executed.")
    
    # Clean up dummy files
    for item in all_configs:
        if item["file"].exists():
            item["file"].unlink()
    
    # Verify DB state
    provider = JobParamsProvider(base_dir)
    print(f"Connecting to DB: {provider.db_params['dbname']} at {provider.db_params['host']}")
    conn = provider._get_connection()
    try:
        with conn.cursor() as cur:
            # Check all definitions
            cur.execute("SELECT job_nm, team_id FROM etl_job_definition")
            defs = cur.fetchall()
            print(f"Current Job Definitions in DB: {defs}")
            
            cur.execute("SELECT blueprint_nm, team_id FROM etl_blueprint")
            blueprints = cur.fetchall()
            print(f"Current Blueprints in DB: {blueprints}")

            # Check static pipeline
            cur.execute("SELECT job_nm FROM etl_job_definition WHERE job_nm = 'static_job'")
            res = cur.fetchone()
            print(f"Static Job in DB: {res}")
            assert res is not None, "Static job not found in etl_job_definition"
            
            # Check blueprint
            cur.execute("SELECT blueprint_nm FROM etl_blueprint WHERE blueprint_nm = 'blueprint_template_test'")
            res = cur.fetchone()
            print(f"Blueprint in DB: {res}")
            assert res is not None, "Blueprint not found in etl_blueprint"
            
            # Check for cross-contamination
            cur.execute("SELECT job_nm FROM etl_job_definition WHERE job_nm = 'blueprint_template_test'")
            res = cur.fetchone()
            assert res is None, "Blueprint found in etl_job_definition (it shouldn't be!)"
            
    finally:
        conn.close()
    
    print("✨ Verification SUCCESS!")

if __name__ == "__main__":
    verify_sync()
