import sys
import os
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from dotenv import load_dotenv
import bcrypt
import hashlib

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

def create_admin(username, password, full_nm):
    engine = create_engine(get_url())
    with Session(engine) as db:
        # Get Admin Role ID
        admin_role = db.query(models.ETLRole).filter(models.ETLRole.role_nm == "DPE_PLATFORM_ADMIN").first()
        if not admin_role:
            print("Error: DPE_PLATFORM_ADMIN role not found. Please run seed_rbac.py first.")
            return

        # Check if user exists
        existing = db.query(models.ETLUser).filter(models.ETLUser.username == username).first()
        if existing:
            print(f"User {username} already exists.")
            return

        # Pre-hash with SHA-256 to handle bcrypt 72-char limit
        password_hash = hashlib.sha256(password.encode()).hexdigest().encode()
        hashed_pwd = bcrypt.hashpw(password_hash, bcrypt.gensalt(rounds=12)).decode('utf-8')
        
        db_user = models.ETLUser(
            username=username,
            hashed_password=hashed_pwd,
            full_nm=full_nm,
            role_id=admin_role.id,
            creat_by_nm="SYSTEM"
        )
        db.add(db_user)
        db.commit()
        print(f"Successfully created admin user: {username}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Create an initial admin user.")
    parser.add_argument("--username", default="admin", help="Admin username")
    parser.add_argument("--password", required=True, help="Admin password")
    parser.add_argument("--name", default="System Administrator", help="Full name")
    
    args = parser.parse_args()
    create_admin(args.username, args.password, args.name)
