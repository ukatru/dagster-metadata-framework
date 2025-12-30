import sys
import os
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy import update
from dotenv import load_dotenv

# Add framework path
sys.path.append(str(Path(__file__).parent.parent / "src"))
from metadata_framework import models

# Load .env
load_dotenv(str(Path(__file__).parent.parent / ".env"))

# Database URL helper
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
        print("🌱 Seeding SaaS Foundation...")

        # 1. Create EDS Org
        eds_org = db.query(models.ETLOrg).filter(models.ETLOrg.org_code == "EDS").first()
        if not eds_org:
            eds_org = models.ETLOrg(
                org_nm="Enterprise Data Services",
                org_code="EDS",
                description="Root organization for Enterprise Data Services.",
                creat_by_nm="SEED_SCRIPT"
            )
            db.add(eds_org)
            db.flush()
            print(f"✅ Created Org: {eds_org.org_nm} (Code: {eds_org.org_code})")
        else:
            print(f"ℹ️ Org '{eds_org.org_nm}' already exists.")

        # 2. Create Marketplace Team
        default_team = db.query(models.ETLTeam).filter(
            models.ETLTeam.org_id == eds_org.id, 
            models.ETLTeam.team_nm == "Marketplace"
        ).first()
        if not default_team:
            default_team = models.ETLTeam(
                org_id=eds_org.id,
                team_nm="Marketplace",
                description="Default team for legacy resources.",
                creat_by_nm="SEED_SCRIPT"
            )
            db.add(default_team)
            db.flush()
            print(f"✅ Created Team: {default_team.team_nm}")
        else:
            print("ℹ️ Team 'Marketplace' already exists.")

        # 2.1 Create Default Code Location
        default_loc = db.query(models.ETLCodeLocation).filter(
            models.ETLCodeLocation.team_id == default_team.id,
            models.ETLCodeLocation.location_nm == "Core Pipelines"
        ).first()
        if not default_loc:
            default_loc = models.ETLCodeLocation(
                team_id=default_team.id,
                location_nm="Core Pipelines",
                repo_url="https://github.com/enterprise/core-pipelines",
                creat_by_nm="SEED_SCRIPT"
            )
            db.add(default_loc)
            db.flush()
            print(f"✅ Created Code Location: {default_loc.location_nm}")
        else:
            print(f"ℹ️ Code Location '{default_loc.location_nm}' already exists.")

        # 3. Associate all existing users with EDS Org
        users_updated = db.query(models.ETLUser).filter(models.ETLUser.org_id == None).update({"org_id": eds_org.id})
        print(f"✅ Associated {users_updated} legacy users with 'EDS' Org.")

        # 4. Associate all existing jobs with EDS Org and Marketplace Team
        jobs_updated = db.query(models.ETLJob).filter(models.ETLJob.org_id == None).update({
            "org_id": eds_org.id,
            "team_id": default_team.id,
            "code_location_id": default_loc.id
        })
        print(f"✅ Associated {jobs_updated} legacy jobs with 'EDS' Org, 'Marketplace' Team, and 'Core' Repo.")

        # 5. Associate all existing connections with EDS Org and Marketplace Team
        conns_updated = db.query(models.ETLConnection).filter(models.ETLConnection.org_id == None).update({
            "org_id": eds_org.id,
            "team_id": default_team.id,
            "owner_type": "TEAM",
            "owner_id": default_team.id
        })
        print(f"✅ Associated {conns_updated} legacy connections with 'EDS' Org and 'Marketplace' Team.")

        # 6. Associate all existing statuses with EDS Org and Marketplace Team
        status_updated = db.query(models.ETLJobStatus).filter(models.ETLJobStatus.org_id == None).update({
            "org_id": eds_org.id,
            "team_id": default_team.id
        })
        print(f"✅ Associated {status_updated} legacy job statuses with 'EDS' Org and 'Marketplace' Team.")

        # 7. Associate all existing schemas with Default Code Location
        schemas_updated = db.query(models.ETLParamsSchema).filter(models.ETLParamsSchema.code_location_id == None).update({
            "code_location_id": default_loc.id
        })
        print(f"✅ Associated {schemas_updated} legacy parameter schemas with 'Core Pipelines' repo.")

        db.commit()
        print("🚀 SaaS Foundation Seeded Successfully!")

if __name__ == "__main__":
    seed()
