# Nexus Params Framework

This framework provides dynamic parameter injection, database-driven scheduling, and advanced observability for Dagster pipelines. It extends the core `dagster-dag-factory` with runtime configuration hydration.

## Features
- **Dynamic Param Hydration**: Inject runtime values using `{{ params.X }}` in YAML configurations.
- **Topological Overrides**: Apply database-driven schedule and partition overrides across the entire repository.
- **Deep Observability**: Automatic logging of job and asset status, timings, and configuration snapshots to Postgres.

## Repository Structure
- `src/`: Core framework logic and Dagster factory extensions.
- `scripts/`: Utility scripts for database initialization (`init_db.py`) and seeding (`seed_nexus.py`).
- `tests/`: Integrated test suite for verifying factory logic and parameter hydration.
- `alembic/`: Database migration scripts and schema definitions.

## Getting Started

### 1. Installation
```bash
pip install -e .
```

### 2. Database Provisioning
Configure your `.env` and initialize the schema:
```bash
python scripts/init_db.py
```

### 3. Usage
```python
from metadata_framework import ParamsDagsterFactory
from pathlib import Path

defs = ParamsDagsterFactory(Path(__file__).parent).build_definitions()
```

## Testing
Run the test suite to verify your setup:
```bash
export PYTHONPATH=$PYTHONPATH:./src:../dagster-dag-factory/src
pytest tests/
```
