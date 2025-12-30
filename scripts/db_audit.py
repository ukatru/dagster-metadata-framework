import os
import sqlalchemy
from sqlalchemy import text

def audit_roles():
    url = os.getenv('DATABASE_URL')
    if not url:
        print("DATABASE_URL not set")
        return
    
    engine = sqlalchemy.create_engine(url)
    with engine.connect() as conn:
        print("--- ROLES ---")
        result = conn.execute(text("SELECT id, role_nm, team_id FROM etl_role ORDER BY team_id NULLS FIRST;"))
        for row in result:
            print(f"ID: {row[0]}, Name: {row[1]}, TeamID: {row[2]}")
            
        print("\n--- PIPELINES ---")
        result = conn.execute(text("SELECT id, job_nm, team_id FROM etl_job;"))
        for row in result:
            print(f"ID: {row[0]}, Name: {row[1]}, TeamID: {row[2]}")
            
        print("\n--- RAW MEMBERSHIPS FOR UKATRU ---")
        result = conn.execute(text("""
            SELECT tm.*, t.team_nm 
            FROM etl_team_member tm
            JOIN etl_user u ON tm.user_id = u.id
            LEFT JOIN etl_team t ON tm.team_id = t.id
            WHERE u.username = 'ukatru'
        """))
        for row in result:
            print(row)
            
        print("\n--- USER GLOBAL ROLES ---")
        result = conn.execute(text("""
            SELECT u.username, r.role_nm 
            FROM etl_user u
            LEFT JOIN etl_role r ON u.role_id = r.id
        """))
        for row in result:
            print(f"User: {row[0]}, Global Role: {row[1]}")

if __name__ == "__main__":
    audit_roles()
