import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path("/home/ukatru/github/dagster-metadata-framework/src")))

from metadata_framework.factory import MetadataDagsterFactory

def verify_sensors_in_factory():
    print("🧪 Verifying Sensors in MetadataDagsterFactory...")
    factory = MetadataDagsterFactory(Path("/home/ukatru/github/example-pipelines"))
    defs = factory.build_definitions()
    
    sensor_names = [s.name for s in defs.sensors]
    print(f"   Found Sensors: {sensor_names}")
    
    expected = ['nexus_job_started_sensor', 'nexus_job_success_sensor', 'nexus_job_failure_sensor']
    all_found = all(s in sensor_names for s in expected)
    
    if all_found:
        print("   ✅ All Nexus Job Tracking sensors are registered in the Definitions.")
        return True
    else:
        print(f"   ❌ Missing sensors. Expected: {expected}")
        return False

if __name__ == "__main__":
    if verify_sensors_in_factory():
        sys.exit(0)
    else:
        sys.exit(1)
