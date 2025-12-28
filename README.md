# Nexus Metadata Framework

This framework provides dynamic parameter injection, database-driven scheduling, and advanced observability for Dagster pipelines.

## Getting Started

### 1. Installation
Install the required dependencies in your environment:
```bash
pip install -e .
```

### 2. Database Provisioning
The framework requires a PostgreSQL database to store parameters and logs. To initialize the schema, follow the instructions in the [Alembic README](alembic/README.md):
- Configure your `.env` file.
- Run `alembic upgrade head`.

### 3. Usage with Dagster
To use the metadata-aware factory in your Dagster project:
```python
from metadata_framework.factory import ParamsDagsterFactory
from pathlib import Path

defs = ParamsDagsterFactory(Path(__file__).parent).build_definitions()
```

## Documentation
- [Database Migrations & Provisioning](alembic/README.md)
- [Architecture Walkthrough](../.gemini/antigravity/brain/eaa27061-5673-4367-ac78-b990189eda0e/walkthrough.md)
