# Alembic Database Migrations

This directory contains the [Alembic](https://alembic.sqlalchemy.org/) migration scripts for the Nexus Params Framework. It manages the schema for parameter injection, connection management, and observability logging.

## Initial Repository Provisioning

To initialize the database for the first time in a new environment, follow these steps:

1.  **Configure Environment Variables**:
    Ensure your `.env` file in the root directory has the following PostgreSQL connection details:
    ```env
    POSTGRES_USER=your_user
    POSTGRES_PASSWORD=your_password
    POSTGRES_HOST=localhost
    POSTGRES_PORT=5432
    POSTGRES_DB=your_db
    ```

2.  **Run Migrations**:
    Apply the schema to the database by upgrading to the latest revision (head):
    ```bash
    alembic upgrade head
    ```

3.  **Seed Data (Optional)**:
    If you want to populate the database with initial POC data (connections and a test job), you can run the bootstrap script:
    > [!WARNING]
    > Running `init_db.py` will drop existing tables. Use it only for fresh prototyping.
    ```bash
    python scripts/init_db.py
    ```

## Managing Migrations

### Creating a New Migration
When you modify the models in `src/metadata_framework/models.py`, generate a new migration script:
```bash
alembic revision --autogenerate -m "description of changes"
```

### Applying Migrations
To apply any new migrations to your local or target database:
```bash
alembic upgrade head
```

### Reverting Migrations
To roll back the last migration:
```bash
alembic downgrade -1
```

## Schema Overview
The migrations manage the following core tables:
- `etl_connection`: Registry of database and cloud connections.
- `etl_job`: Definition of jobs and their active parameters/triggers.
- `etl_job_parameter`: JSONB storage for dynamic configuration overrides.
- `etl_job_status`: High-level run tracking.
- `etl_asset_status`: Granular asset-level logging and error tracking.
