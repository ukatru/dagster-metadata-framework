import sys
import os
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from dotenv import load_dotenv

# Add framework path
sys.path.append("/home/ukatru/github/dagster-metadata-framework/src")
from metadata_framework import models

# Load .env
load_dotenv("/home/ukatru/github/dagster-metadata-framework/.env")

# Database URL helper (copied from env.py)
def get_url():
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "postgres")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"

# Standard Connection Schemas (copied from connections_metadata.py)
STANDARD_SCHEMAS = {
    "PostgreSQL": {
        "type": "object",
        "required": ["host", "user", "password", "database"],
        "properties": {
            "host": {"type": "string", "title": "Host"},
            "port": {"type": "integer", "title": "Port", "default": 5432},
            "user": {"type": "string", "title": "Username"},
            "password": {"type": "string", "title": "Password", "format": "password"},
            "database": {"type": "string", "title": "Database Name"},
        }
    },
    "Snowflake": {
        "type": "object",
        "required": ["account", "user", "warehouse", "database"],
        "properties": {
            "account": {"type": "string", "title": "Account Identifier"},
            "user": {"type": "string", "title": "Username"},
            "password": {"type": "string", "title": "Password", "format": "password"},
            "warehouse": {"type": "string", "title": "Warehouse"},
            "database": {"type": "string", "title": "Database"},
            "schema": {"type": "string", "title": "Schema", "default": "PUBLIC"},
            "role": {"type": "string", "title": "Role"},
            "private_key": {"type": "string", "title": "Private Key (PEM)", "format": "textarea"},
            "private_key_passphrase": {"type": "string", "title": "PK Passphrase", "format": "password"},
        }
    },
    "S3": {
        "type": "object",
        "required": ["region_name"],
        "properties": {
            "region_name": {"type": "string", "title": "AWS Region", "default": "us-east-1"},
            "bucket_name": {"type": "string", "title": "Default Bucket"},
            "aws_access_key_id": {"type": "string", "title": "Access Key ID"},
            "aws_secret_access_key": {"type": "string", "title": "Secret Access Key", "format": "password"},
            "endpoint_url": {"type": "string", "title": "Custom Endpoint URL (Minio/Localstack)"},
        }
    },
    "SFTP": {
        "type": "object",
        "required": ["host", "username"],
        "properties": {
            "host": {"type": "string", "title": "SFTP Host"},
            "port": {"type": "integer", "title": "Port", "default": 22},
            "username": {"type": "string", "title": "Username"},
            "password": {"type": "string", "title": "Password", "format": "password"},
            "private_key": {"type": "string", "title": "Private Key (B64)", "format": "textarea"},
            "key_type": {
                "type": "string", 
                "title": "Key Type", 
                "enum": ["RSA", "ECDSA", "ED25519"], 
                "default": "RSA"
            },
        }
    },
    "SQLServer": {
        "type": "object",
        "required": ["host", "database"],
        "properties": {
            "host": {"type": "string", "title": "Host"},
            "port": {"type": "integer", "title": "Port", "default": 1433},
            "user": {"type": "string", "title": "Username"},
            "password": {"type": "string", "title": "Password", "format": "password"},
            "database": {"type": "string", "title": "Database Name"},
            "driver": {"type": "string", "title": "ODBC Driver", "default": "ODBC Driver 18 for SQL Server"},
            "encrypt": {"type": "boolean", "title": "Encrypt", "default": True},
            "trust_server_certificate": {"type": "boolean", "title": "Trust Server Cert", "default": True},
        }
    }
}

def seed():
    engine = create_engine(get_url())
    with Session(engine) as db:
        seeded = []
        for ctype, sjson in STANDARD_SCHEMAS.items():
            existing = db.query(models.ETLConnTypeSchema).filter(models.ETLConnTypeSchema.conn_type == ctype).first()
            if not existing:
                db_schema = models.ETLConnTypeSchema(
                    conn_type=ctype,
                    schema_json=sjson,
                    description=f"Standard schema for {ctype} connections",
                    creat_by_nm="SYSTEM"
                )
                db.add(db_schema)
                seeded.append(ctype)
        db.commit()
        print(f"Successfully seeded: {', '.join(seeded) if seeded else 'None (already exists)'}")

if __name__ == "__main__":
    seed()
