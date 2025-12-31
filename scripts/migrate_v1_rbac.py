import os
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from dotenv import load_dotenv
from pathlib import Path

# Add framework path
sys.path.append(str(Path(__file__).parent.parent / "src"))
from metadata_framework import models

# Load env
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(env_path)

def get_url(db_name):
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    return f"postgresql://{user}:{password}@{host}:{port}/{db_name}"

def migrate():
    source_db_name = "dagster_etl_framework"
    target_db_name = os.getenv("POSTGRES_DB", "dpe_framework")
    
    print(f"🔗 Source DB: {source_db_name}")
    print(f"🎯 Target DB: {target_db_name}")

    source_engine = create_engine(get_url(source_db_name))
    target_engine = create_engine(get_url(target_db_name))
    
    with Session(source_engine) as source_db, Session(target_engine) as target_db:
        print("🚀 Starting RBAC Migration...")

        # 1. Migrate Orgs
        print("--- Migrating Organizations ---")
        orgs = source_db.execute(text("SELECT id, org_nm, org_code, description, actv_ind FROM etl_org")).fetchall()
        for org in orgs:
            existing = target_db.query(models.ETLOrg).filter_by(id=org.id).first()
            if not existing:
                new_org = models.ETLOrg(
                    id=org.id,
                    org_nm=org.org_nm,
                    org_code=org.org_code,
                    description=org.description,
                    actv_ind=org.actv_ind
                )
                target_db.add(new_org)
        target_db.flush()

        # 2. Migrate Teams
        print("--- Migrating Teams ---")
        teams = source_db.execute(text("SELECT id, org_id, team_nm, description, actv_ind FROM etl_team")).fetchall()
        for team in teams:
            existing = target_db.query(models.ETLTeam).filter_by(id=team.id).first()
            if not existing:
                new_team = models.ETLTeam(
                    id=team.id,
                    org_id=team.org_id,
                    team_nm=team.team_nm,
                    description=team.description,
                    actv_ind=team.actv_ind
                )
                target_db.add(new_team)
        target_db.flush()

        # 3. Migrate Roles (Both Global and Team-Scoped)
        print("--- Migrating Roles ---")
        roles = source_db.execute(text("SELECT id, team_id, role_nm, description, actv_ind FROM etl_role")).fetchall()
        for role in roles:
            existing = target_db.query(models.ETLRole).filter_by(id=role.id).first()
            if not existing:
                new_role = models.ETLRole(
                    id=role.id,
                    team_id=role.team_id,
                    role_nm=role.role_nm,
                    description=role.description,
                    actv_ind=role.actv_ind
                )
                target_db.add(new_role)
        target_db.flush()

        # 4. Migrate Users
        print("--- Migrating Users ---")
        users = source_db.execute(text("SELECT id, username, hashed_password, full_nm, email, role_id, org_id, default_team_id, actv_ind FROM etl_user")).fetchall()
        for user in users:
            existing = target_db.query(models.ETLUser).filter_by(id=user.id).first()
            if not existing:
                new_user = models.ETLUser(
                    id=user.id,
                    username=user.username,
                    hashed_password=user.hashed_password,
                    full_nm=user.full_nm,
                    email=user.email,
                    role_id=user.role_id,
                    org_id=user.org_id,
                    default_team_id=user.default_team_id,
                    actv_ind=user.actv_ind
                )
                target_db.add(new_user)
        target_db.flush()

        # 5. Migrate Team Memberships
        print("--- Migrating Team Memberships ---")
        members = source_db.execute(text("SELECT id, user_id, team_id, role_id, actv_ind FROM etl_team_member")).fetchall()
        for member in members:
            existing = target_db.query(models.ETLTeamMember).filter_by(id=member.id).first()
            if not existing:
                new_member = models.ETLTeamMember(
                    id=member.id,
                    user_id=member.user_id,
                    team_id=member.team_id,
                    role_id=member.role_id,
                    actv_ind=member.actv_ind
                )
                target_db.add(new_member)
        target_db.flush()

        # 6. Migrate Code Locations
        print("--- Migrating Code Locations ---")
        locs = source_db.execute(text("SELECT id, team_id, location_nm, repo_url FROM etl_code_location")).fetchall()
        for loc in locs:
            existing = target_db.query(models.ETLCodeLocation).filter_by(id=loc.id).first()
            if not existing:
                new_loc = models.ETLCodeLocation(
                    id=loc.id,
                    team_id=loc.team_id,
                    location_nm=loc.location_nm,
                    repo_url=loc.repo_url
                )
                target_db.add(new_loc)
        target_db.flush()

        target_db.commit()
        print("\n✅ Data Migration (RBAC & Workspace) Completed Successfully!")

if __name__ == "__main__":
    migrate()
