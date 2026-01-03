import json
import argparse
from pathlib import Path
from dagster_dag_factory.factory.schema import PipelineConfig

def export_schema(output_path: str):
    """
    Exports the Framework's Pydantic models to a standard JSON Schema file.
    This enables IDE autocompletion and syntax validation for analysts.
    """
    # 🟢 Pydantic converts the entire tree (Assets, Jobs, Schedules) to JSON Schema
    # Note: Using model_json_schema() for Pydantic V2 compatibility
    schema = PipelineConfig.model_json_schema()
    
    # Add a friendly description to the root
    schema["description"] = "Nexus Data Platform Pipeline Manifest Schema"
    
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, "w") as f:
        json.dump(schema, f, indent=2)
        
    print(f"✅ Nexus Schema exported to: {output_file}")

def main():
    parser = argparse.ArgumentParser(description="Nexus Platform CLI")
    parser.add_argument("--export-schema", type=str, help="Path to save the JSON schema file")
    
    args = parser.parse_args()
    
    if args.export_schema:
        export_schema(args.export_schema)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
