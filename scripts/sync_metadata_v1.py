import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add framework path
sys.path.append(str(Path(__file__).parent.parent / "src"))
from metadata_framework.factory import ParamsDagsterFactory

# Load env
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(env_path)

def sync_pipelines(pipelines_dir: str):
    print(f"🔄 Syncing pipelines from: {pipelines_dir}")
    
    # Initialize the factory
    # Note: ParamsDagsterFactory's constructor will automatically call _sync_job_definitions 
    # if it's rigged into the build_definitions or if we call it explicitly.
    # In our current factory.py, _sync_job_definitions is called inside _apply_overrides 
    # which is likely called during definition building.
    
    factory = ParamsDagsterFactory(
        base_dir=Path(pipelines_dir),
        location_name="example-pipelines"
    )
    
    print("🔍 Discovering YAML files...")
    all_configs = factory._load_all_configs(show_logs=True)
    
    # Note: In our current factory.py, we have _sync_job_definitions(self, all_configs: list)
    print("🚀 Triggering Unified Registry Sync...")
    factory._sync_job_definitions(all_configs)
    
    print("\n✅ Metadata Synchronization Completed Successfully!")

if __name__ == "__main__":
    # Target the example-pipelines directory
    pipelines_root = "/home/ukatru/github/example-pipelines"
    sync_pipelines(pipelines_root)
