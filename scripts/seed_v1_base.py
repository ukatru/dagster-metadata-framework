import os
import sys
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from dotenv import load_dotenv

# Add framework path
sys.path.append(str(Path(__file__).parent.parent / "src"))
from metadata_framework import models

# Load env
load_dotenv(Path(__file__).parent.parent / ".env")

def get_url():
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "postgres")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"

def seed():
    engine = create_engine(get_url())
    with Session(engine) as db:
        # 1. Organization
        eds = db.query(models.ETLOrg).filter_by(org_code="EDS").first()
        if not eds:
            eds = models.ETLOrg(
                org_nm="Enterprise Data Services",
                org_code="EDS",
                description="Primary Data Platform Organization"
            )
            db.add(eds)
            db.flush()
            print(f"✅ Seeded Org: {eds.org_nm}")

        # 2. Teams
        mkt = db.query(models.ETLTeam).filter_by(team_nm="Marketplace").first()
        if not mkt:
            mkt = models.ETLTeam(
                org_id=eds.id,
                team_nm="Marketplace",
                description="B2C Marketplace Data Team"
            )
            db.add(mkt)
            print(f"✅ Seeded Team: {mkt.team_nm}")

        edw = db.query(models.ETLTeam).filter_by(team_nm="EDW").first()
        if not edw:
            edw = models.ETLTeam(
                org_id=eds.id,
                team_nm="EDW",
                description="Enterprise Data Warehouse Team"
            )
            db.add(edw)
            print(f"✅ Seeded Team: {edw.team_nm}")
        
        db.flush()

        # 3. Generic Role Templates
        GENERIC_ROLES = [
            ("OrgAdmin", "Full organization management."),
            ("TeamAdmin", "Full team management."),
            ("Editor", "Can create and edit pipelines/connections."),
            ("Viewer", "Read-only access to status and config."),
            ("Catalog Viewer", "Restricted access to data catalogs.")
        ]
        
        for name, desc in GENERIC_ROLES:
            role = db.query(models.ETLRole).filter_by(role_nm=name, team_id=None).first()
            if not role:
                role = models.ETLRole(role_nm=name, description=desc)
                db.add(role)
                print(f"✅ Seeded Role: {name}")

        db.flush()

        # 4. Code Locations
        cl = db.query(models.ETLCodeLocation).filter_by(location_nm="example-pipelines").first()
        if not cl:
            cl = models.ETLCodeLocation(
                team_id=mkt.id,
                location_nm="example-pipelines",
                repo_url="https://github.com/ukatru/example-pipelines"
            )
            db.add(cl)
            print(f"✅ Seeded Code Location: {cl.location_nm}")

        # 5. Admin User (ukatru)
        admin_role = db.query(models.ETLRole).filter_by(role_nm="OrgAdmin").first()
        user = db.query(models.ETLUser).filter_by(username="ukatru").first()
        if not user:
            from passlib.hash import bcrypt
            user = models.ETLUser(
                username="ukatru",
                full_nm="Uday Katru",
                email="ukatru@example.com",
                hashed_password=bcrypt.hash("admin123"), # Default password
                role_id=admin_role.id,
                org_id=eds.id,
                default_team_id=mkt.id
            )
            db.add(user)
            db.flush()
            
            # Team Membership
            membership = models.ETLTeamMember(
                user_id=user.id,
                team_id=mkt.id,
                role_id=admin_role.id
            )
            db.add(membership)
            print(f"✅ Seeded User: {user.username}")

        # 6. Global Parameters
        params = [
            ("env", "production"),
            ("target_bucket", "nexus-data-lake-prod")
        ]
        for nm, val in params:
            existing = db.query(models.ETLParameter).filter_by(parm_nm=nm).first()
            if not existing:
                db.add(models.ETLParameter(parm_nm=nm, parm_value=val))
                print(f"✅ Seeded Parameter: {nm}")

        db.commit()
        print("\n🚀 v1.0.0 Base State Seeded Successfully!")

if __name__ == "__main__":
    seed()
