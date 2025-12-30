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

# Database URL helper
def get_url():
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "postgres")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"

ROLES = [
    {
        "role_nm": "DPE_PLATFORM_ADMIN",
        "description": "Full platform administration: Manage Users, Roles, and global configs."
    },
    {
        "role_nm": "DPE_DEVELOPER",
        "description": "Data Engineering access: Create/Edit pipelines and test connections."
    },
    {
        "role_nm": "DPE_DATA_ANALYST",
        "description": "Viewer access: Monitor pipeline status and view configurations."
    }
]

def seed():
    engine = create_engine(get_url())
    with Session(engine) as db:
        seeded = []
        for role_data in ROLES:
            existing = db.query(models.ETLRole).filter(models.ETLRole.role_nm == role_data["role_nm"]).first()
            if not existing:
                db_role = models.ETLRole(
                    role_nm=role_data["role_nm"],
                    description=role_data["description"],
                    creat_by_nm="SYSTEM"
                )
                db.add(db_role)
                seeded.append(role_data["role_nm"])
        db.commit()
        print(f"Successfully seeded roles: {', '.join(seeded) if seeded else 'None (already exists)'}")

if __name__ == "__main__":
    seed()
