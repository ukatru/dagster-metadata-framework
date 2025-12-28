import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path("/home/ukatru/github/dagster-metadata-framework/src")))

from metadata_framework.factory import MetadataDagsterFactory

def verify_observability_sensors():
    print("🧪 Verifying Observability Sensors in MetadataDagsterFactory...")
    factory = MetadataDagsterFactory(Path("/home/ukatru/github/example-pipelines"))
    defs = factory.build_definitions()
    
    # 1. Sensors check (Should have EXACTLY 3 Nexus sensors)
    sensor_names = [s.name for s in defs.sensors]
    print(f"   Found Sensors: {sensor_names}")
    
    nexus_sensors = [s for s in sensor_names if 'nexus' in s]
    required = {'nexus_job_started_sensor', 'nexus_job_success_sensor', 'nexus_job_failure_sensor'}
    
    if required.issubset(set(nexus_sensors)):
        print("   ✅ SUCCESS: All 3 Nexus job-tracking sensors are present.")
    else:
        print(f"   ❌ FAILURE: Missing sensors. Found: {nexus_sensors}, Required: {required}")
        return False

    print("   ✅ SUCCESS: Sensors registered successfully.")
    return True

if __name__ == "__main__":
    if verify_observability_sensors():
        sys.exit(0)
    else:
        sys.exit(1)
