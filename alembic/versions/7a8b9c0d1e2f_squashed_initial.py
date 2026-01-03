"""Squashed Initial Schema

Revision ID: 7a8b9c0d1e2f
Revises: None
Create Date: 2026-01-02 22:00:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine.reflection import Inspector

# revision identifiers, used by Alembic.
revision: str = '7a8b9c0d1e2f'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    conn = op.get_bind()
    inspector = Inspector.from_engine(conn)
    existing_tables = inspector.get_table_names()

    # 1. Create Sequences
    # sequence check is a bit different, we can use raw SQL for safety in PG
    op.execute("CREATE SEQUENCE IF NOT EXISTS etl_pipeline_id_seq")

    # 2. ETL Org
    if 'etl_org' not in existing_tables:
        op.create_table('etl_org',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('org_nm', sa.String(length=100), nullable=False),
            sa.Column('org_code', sa.String(length=20), nullable=False),
            sa.Column('description', sa.String(length=255), nullable=True),
            sa.Column('actv_ind', sa.Boolean(), nullable=True),
            sa.Column('creat_by_nm', sa.String(length=100), nullable=False),
            sa.Column('creat_dttm', sa.DateTime(), nullable=False),
            sa.Column('updt_by_nm', sa.String(length=100), nullable=True),
            sa.Column('updt_dttm', sa.DateTime(), nullable=True),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('org_nm'),
            sa.UniqueConstraint('org_code')
        )

    # 3. ETL Team
    if 'etl_team' not in existing_tables:
        op.create_table('etl_team',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('org_id', sa.Integer(), nullable=False),
            sa.Column('team_nm', sa.String(length=100), nullable=False),
            sa.Column('description', sa.String(length=255), nullable=True),
            sa.Column('actv_ind', sa.Boolean(), nullable=True),
            sa.Column('creat_by_nm', sa.String(length=100), nullable=False),
            sa.Column('creat_dttm', sa.DateTime(), nullable=False),
            sa.Column('updt_by_nm', sa.String(length=100), nullable=True),
            sa.Column('updt_dttm', sa.DateTime(), nullable=True),
            sa.ForeignKeyConstraint(['org_id'], ['etl_org.id'], ),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('org_id', 'team_nm', name='uq_org_team')
        )

    # 4. ETL Code Location
    if 'etl_code_location' not in existing_tables:
        op.create_table('etl_code_location',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('team_id', sa.Integer(), nullable=False),
            sa.Column('location_nm', sa.String(length=100), nullable=False),
            sa.Column('repo_url', sa.String(length=255), nullable=True),
            sa.Column('creat_by_nm', sa.String(length=100), nullable=False),
            sa.Column('creat_dttm', sa.DateTime(), nullable=False),
            sa.Column('updt_by_nm', sa.String(length=100), nullable=True),
            sa.Column('updt_dttm', sa.DateTime(), nullable=True),
            sa.ForeignKeyConstraint(['team_id'], ['etl_team.id'], ),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('team_id', 'location_nm', name='uq_team_location')
        )

    # 5. ETL Connection
    if 'etl_connection' not in existing_tables:
        op.create_table('etl_connection',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('conn_nm', sa.String(length=255), nullable=False),
            sa.Column('conn_type', sa.String(length=50), nullable=False),
            sa.Column('config_json', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
            sa.Column('org_id', sa.Integer(), nullable=True),
            sa.Column('team_id', sa.Integer(), nullable=True),
            sa.Column('owner_type', sa.String(length=20), nullable=True),
            sa.Column('owner_id', sa.Integer(), nullable=True),
            sa.Column('creat_by_nm', sa.String(length=100), nullable=False),
            sa.Column('creat_dttm', sa.DateTime(), nullable=False),
            sa.Column('updt_by_nm', sa.String(length=100), nullable=True),
            sa.Column('updt_dttm', sa.DateTime(), nullable=True),
            sa.ForeignKeyConstraint(['org_id'], ['etl_org.id'], ),
            sa.ForeignKeyConstraint(['team_id'], ['etl_team.id'], ),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('conn_nm')
        )

    # 6. ETL Schedule
    if 'etl_schedule' not in existing_tables:
        op.create_table('etl_schedule',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('org_id', sa.Integer(), nullable=False),
            sa.Column('team_id', sa.Integer(), nullable=True),
            sa.Column('slug', sa.String(length=255), nullable=False),
            sa.Column('cron', sa.String(length=100), nullable=False),
            sa.Column('timezone', sa.String(length=100), nullable=True),
            sa.Column('actv_ind', sa.Boolean(), nullable=True),
            sa.Column('creat_by_nm', sa.String(length=100), nullable=False),
            sa.Column('creat_dttm', sa.DateTime(), nullable=False),
            sa.Column('updt_by_nm', sa.String(length=100), nullable=True),
            sa.Column('updt_dttm', sa.DateTime(), nullable=True),
            sa.ForeignKeyConstraint(['org_id'], ['etl_org.id'], ),
            sa.ForeignKeyConstraint(['team_id'], ['etl_team.id'], ),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('slug')
        )

    # 7. ETL Job Definition
    if 'etl_job_definition' not in existing_tables:
        op.create_table('etl_job_definition',
            sa.Column('id', sa.BigInteger(), server_default=sa.text("nextval('etl_pipeline_id_seq')"), nullable=False),
            sa.Column('job_nm', sa.String(length=255), nullable=False),
            sa.Column('description', sa.String(length=255), nullable=True),
            sa.Column('blueprint_ind', sa.Boolean(), nullable=False),
            sa.Column('file_loc', sa.String(length=512), nullable=True),
            sa.Column('file_hash', sa.String(length=64), nullable=True),
            sa.Column('yaml_content', sa.Text(), nullable=True),
            sa.Column('yaml_def', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
            sa.Column('params_schema', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
            sa.Column('asset_selection', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
            sa.Column('org_id', sa.Integer(), nullable=True),
            sa.Column('team_id', sa.Integer(), nullable=True),
            sa.Column('code_location_id', sa.Integer(), nullable=True),
            sa.Column('actv_ind', sa.Boolean(), nullable=True),
            sa.Column('creat_by_nm', sa.String(length=100), nullable=False),
            sa.Column('creat_dttm', sa.DateTime(), nullable=False),
            sa.Column('updt_by_nm', sa.String(length=100), nullable=True),
            sa.Column('updt_dttm', sa.DateTime(), nullable=True),
            sa.ForeignKeyConstraint(['code_location_id'], ['etl_code_location.id'], ),
            sa.ForeignKeyConstraint(['org_id'], ['etl_org.id'], ),
            sa.ForeignKeyConstraint(['team_id'], ['etl_team.id'], ),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('job_nm', 'team_id', 'code_location_id', name='uq_job_team_loc_def')
        )

    # 8. ETL Job Instance
    if 'etl_job_instance' not in existing_tables:
        op.create_table('etl_job_instance',
            sa.Column('id', sa.BigInteger(), server_default=sa.text("nextval('etl_pipeline_id_seq')"), nullable=False),
            sa.Column('instance_id', sa.String(length=255), nullable=False),
            sa.Column('description', sa.String(length=255), nullable=True),
            sa.Column('job_definition_id', sa.BigInteger(), nullable=False),
            sa.Column('schedule_id', sa.Integer(), nullable=True),
            sa.Column('cron_schedule', sa.String(length=100), nullable=True),
            sa.Column('partition_start_dt', sa.Date(), nullable=True),
            sa.Column('actv_ind', sa.Boolean(), nullable=True),
            sa.Column('org_id', sa.Integer(), nullable=True),
            sa.Column('team_id', sa.Integer(), nullable=True),
            sa.Column('code_location_id', sa.Integer(), nullable=True),
            sa.Column('creat_by_nm', sa.String(length=100), nullable=False),
            sa.Column('creat_dttm', sa.DateTime(), nullable=False),
            sa.Column('updt_by_nm', sa.String(length=100), nullable=True),
            sa.Column('updt_dttm', sa.DateTime(), nullable=True),
            sa.ForeignKeyConstraint(['code_location_id'], ['etl_code_location.id'], ),
            sa.ForeignKeyConstraint(['job_definition_id'], ['etl_job_definition.id'], ),
            sa.ForeignKeyConstraint(['org_id'], ['etl_org.id'], ),
            sa.ForeignKeyConstraint(['schedule_id'], ['etl_schedule.id'], ),
            sa.ForeignKeyConstraint(['team_id'], ['etl_team.id'], ),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('instance_id', 'job_definition_id', name='uq_inst_job_def')
        )

    # 9. ETL Job Parameter (Static)
    if 'etl_job_parameter' not in existing_tables:
        op.create_table('etl_job_parameter',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('job_definition_id', sa.BigInteger(), nullable=True),
            sa.Column('org_id', sa.Integer(), nullable=True),
            sa.Column('team_id', sa.Integer(), nullable=True),
            sa.Column('config_json', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
            sa.Column('cron_schedule', sa.String(length=100), nullable=True),
            sa.Column('partition_start_dt', sa.Date(), nullable=True),
            sa.Column('creat_by_nm', sa.String(length=100), nullable=False),
            sa.Column('creat_dttm', sa.DateTime(), nullable=False),
            sa.Column('updt_by_nm', sa.String(length=100), nullable=True),
            sa.Column('updt_dttm', sa.DateTime(), nullable=True),
            sa.ForeignKeyConstraint(['job_definition_id'], ['etl_job_definition.id'], ),
            sa.ForeignKeyConstraint(['org_id'], ['etl_org.id'], ),
            sa.ForeignKeyConstraint(['team_id'], ['etl_team.id'], ),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('job_definition_id')
        )

    # 10. ETL Instance Parameter (Blueprint)
    if 'etl_instance_parameter' not in existing_tables:
        op.create_table('etl_instance_parameter',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('instance_pk', sa.BigInteger(), nullable=True),
            sa.Column('org_id', sa.Integer(), nullable=True),
            sa.Column('team_id', sa.Integer(), nullable=True),
            sa.Column('config_json', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
            sa.Column('creat_by_nm', sa.String(length=100), nullable=False),
            sa.Column('creat_dttm', sa.DateTime(), nullable=False),
            sa.Column('updt_by_nm', sa.String(length=100), nullable=True),
            sa.Column('updt_dttm', sa.DateTime(), nullable=True),
            sa.ForeignKeyConstraint(['instance_pk'], ['etl_job_instance.id'], ondelete='CASCADE'),
            sa.ForeignKeyConstraint(['org_id'], ['etl_org.id'], ),
            sa.ForeignKeyConstraint(['team_id'], ['etl_team.id'], ),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('instance_pk')
        )

    # 11. ETL Parameter (Generic)
    if 'etl_parameter' not in existing_tables:
        op.create_table('etl_parameter',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('parm_nm', sa.String(length=255), nullable=False),
            sa.Column('parm_value', sa.Text(), nullable=True),
            sa.Column('creat_by_nm', sa.String(length=100), nullable=False),
            sa.Column('creat_dttm', sa.DateTime(), nullable=False),
            sa.Column('updt_by_nm', sa.String(length=100), nullable=True),
            sa.Column('updt_dttm', sa.DateTime(), nullable=True),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('parm_nm')
        )

    # 12. ETL Global Variables
    if 'etl_global_variables' not in existing_tables:
        op.create_table('etl_global_variables',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('org_id', sa.Integer(), nullable=True),
            sa.Column('var_nm', sa.String(length=255), nullable=False),
            sa.Column('var_value', sa.Text(), nullable=True),
            sa.Column('description', sa.String(length=255), nullable=True),
            sa.Column('creat_by_nm', sa.String(length=100), nullable=False),
            sa.Column('creat_dttm', sa.DateTime(), nullable=False),
            sa.Column('updt_by_nm', sa.String(length=100), nullable=True),
            sa.Column('updt_dttm', sa.DateTime(), nullable=True),
            sa.ForeignKeyConstraint(['org_id'], ['etl_org.id'], ),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('org_id', 'var_nm', name='uq_org_variable')
        )

    # 13. ETL Team Variables
    if 'etl_team_variables' not in existing_tables:
        op.create_table('etl_team_variables',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('org_id', sa.Integer(), nullable=True),
            sa.Column('team_id', sa.Integer(), nullable=False),
            sa.Column('var_nm', sa.String(length=255), nullable=False),
            sa.Column('var_value', sa.Text(), nullable=True),
            sa.Column('description', sa.String(length=255), nullable=True),
            sa.Column('creat_by_nm', sa.String(length=100), nullable=False),
            sa.Column('creat_dttm', sa.DateTime(), nullable=False),
            sa.Column('updt_by_nm', sa.String(length=100), nullable=True),
            sa.Column('updt_dttm', sa.DateTime(), nullable=True),
            sa.ForeignKeyConstraint(['org_id'], ['etl_org.id'], ),
            sa.ForeignKeyConstraint(['team_id'], ['etl_team.id'], ),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('team_id', 'var_nm', name='uq_team_variable')
        )

    # 14. ETL Params Schema
    if 'etl_params_schema' not in existing_tables:
        op.create_table('etl_params_schema',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('job_definition_id', sa.BigInteger(), nullable=True),
            sa.Column('org_id', sa.Integer(), nullable=True),
            sa.Column('team_id', sa.Integer(), nullable=True),
            sa.Column('code_location_id', sa.Integer(), nullable=True),
            sa.Column('json_schema', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
            sa.Column('description', sa.Text(), nullable=True),
            sa.Column('is_strict', sa.Boolean(), nullable=True),
            sa.Column('creat_by_nm', sa.String(length=100), nullable=False),
            sa.Column('creat_dttm', sa.DateTime(), nullable=False),
            sa.Column('updt_by_nm', sa.String(length=100), nullable=True),
            sa.Column('updt_dttm', sa.DateTime(), nullable=True),
            sa.ForeignKeyConstraint(['code_location_id'], ['etl_code_location.id'], ),
            sa.ForeignKeyConstraint(['job_definition_id'], ['etl_job_definition.id'], ),
            sa.ForeignKeyConstraint(['org_id'], ['etl_org.id'], ),
            sa.ForeignKeyConstraint(['team_id'], ['etl_team.id'], ),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('job_definition_id')
        )

    # 15. ETL Conn Type Schema
    if 'etl_conn_type_schema' not in existing_tables:
        op.create_table('etl_conn_type_schema',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('conn_type', sa.String(length=50), nullable=False),
            sa.Column('json_schema', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
            sa.Column('description', sa.Text(), nullable=True),
            sa.Column('creat_by_nm', sa.String(length=100), nullable=False),
            sa.Column('creat_dttm', sa.DateTime(), nullable=False),
            sa.Column('updt_by_nm', sa.String(length=100), nullable=True),
            sa.Column('updt_dttm', sa.DateTime(), nullable=True),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('conn_type')
        )

    # 16. ETL Job Status
    if 'etl_job_status' not in existing_tables:
        op.create_table('etl_job_status',
            sa.Column('btch_nbr', sa.BigInteger(), autoincrement=True, nullable=False),
            sa.Column('run_id', sa.String(length=64), nullable=False),
            sa.Column('org_id', sa.Integer(), nullable=True),
            sa.Column('team_id', sa.Integer(), nullable=True),
            sa.Column('job_nm', sa.String(length=256), nullable=False),
            sa.Column('instance_id', sa.String(length=255), nullable=True),
            sa.Column('strt_dttm', sa.DateTime(), nullable=True),
            sa.Column('end_dttm', sa.DateTime(), nullable=True),
            sa.Column('btch_sts_cd', sa.CHAR(length=1), nullable=True),
            sa.Column('run_mde_txt', sa.String(length=50), nullable=False),
            sa.Column('log_url', sa.Text(), nullable=True),
            sa.Column('creat_by_nm', sa.String(length=100), nullable=False),
            sa.Column('creat_dttm', sa.DateTime(), nullable=False),
            sa.Column('updt_by_nm', sa.String(length=100), nullable=True),
            sa.Column('updt_dttm', sa.DateTime(), nullable=True),
            sa.ForeignKeyConstraint(['org_id'], ['etl_org.id'], ),
            sa.ForeignKeyConstraint(['team_id'], ['etl_team.id'], ),
            sa.PrimaryKeyConstraint('btch_nbr'),
            sa.UniqueConstraint('run_id')
        )

    # 17. ETL Asset Status
    if 'etl_asset_status' not in existing_tables:
        op.create_table('etl_asset_status',
            sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
            sa.Column('btch_nbr', sa.BigInteger(), nullable=False),
            sa.Column('asset_nm', sa.String(length=256), nullable=False),
            sa.Column('parent_assets', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
            sa.Column('config_json', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
            sa.Column('partition_key', sa.String(length=255), nullable=True),
            sa.Column('dagster_event_type', sa.String(length=50), nullable=True),
            sa.Column('strt_dttm', sa.DateTime(), nullable=True),
            sa.Column('end_dttm', sa.DateTime(), nullable=True),
            sa.Column('asset_sts_cd', sa.CHAR(length=1), nullable=True),
            sa.Column('err_msg_txt', sa.Text(), nullable=True),
            sa.Column('creat_by_nm', sa.String(length=100), nullable=False),
            sa.Column('creat_dttm', sa.DateTime(), nullable=False),
            sa.Column('updt_by_nm', sa.String(length=100), nullable=True),
            sa.Column('updt_dttm', sa.DateTime(), nullable=True),
            sa.ForeignKeyConstraint(['btch_nbr'], ['etl_job_status.btch_nbr'], ondelete='CASCADE'),
            sa.PrimaryKeyConstraint('id')
        )

    # 18. ETL Role
    if 'etl_role' not in existing_tables:
        op.create_table('etl_role',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('team_id', sa.Integer(), nullable=True),
            sa.Column('role_nm', sa.String(length=100), nullable=False),
            sa.Column('description', sa.String(length=255), nullable=True),
            sa.Column('actv_ind', sa.Boolean(), nullable=True),
            sa.Column('creat_by_nm', sa.String(length=100), nullable=False),
            sa.Column('creat_dttm', sa.DateTime(), nullable=False),
            sa.Column('updt_by_nm', sa.String(length=100), nullable=True),
            sa.Column('updt_dttm', sa.DateTime(), nullable=True),
            sa.ForeignKeyConstraint(['team_id'], ['etl_team.id'], ),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('team_id', 'role_nm', name='uq_team_role')
        )

    # 19. ETL User
    if 'etl_user' not in existing_tables:
        op.create_table('etl_user',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('username', sa.String(length=100), nullable=False),
            sa.Column('hashed_password', sa.String(length=255), nullable=False),
            sa.Column('full_nm', sa.String(length=255), nullable=False),
            sa.Column('email', sa.String(length=255), nullable=True),
            sa.Column('role_id', sa.Integer(), nullable=False),
            sa.Column('org_id', sa.Integer(), nullable=True),
            sa.Column('default_team_id', sa.Integer(), nullable=True),
            sa.Column('actv_ind', sa.Boolean(), nullable=True),
            sa.Column('creat_by_nm', sa.String(length=100), nullable=False),
            sa.Column('creat_dttm', sa.DateTime(), nullable=False),
            sa.Column('updt_by_nm', sa.String(length=100), nullable=True),
            sa.Column('updt_dttm', sa.DateTime(), nullable=True),
            sa.ForeignKeyConstraint(['default_team_id'], ['etl_team.id'], ),
            sa.ForeignKeyConstraint(['org_id'], ['etl_org.id'], ),
            sa.ForeignKeyConstraint(['role_id'], ['etl_role.id'], ),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('username')
        )
        op.create_index(op.f('ix_etl_user_username'), 'etl_user', ['username'], unique=True)

    # 20. ETL Team Member
    if 'etl_team_member' not in existing_tables:
        op.create_table('etl_team_member',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('user_id', sa.Integer(), nullable=False),
            sa.Column('team_id', sa.Integer(), nullable=False),
            sa.Column('role_id', sa.Integer(), nullable=False),
            sa.Column('actv_ind', sa.Boolean(), nullable=True),
            sa.Column('creat_by_nm', sa.String(length=100), nullable=False),
            sa.Column('creat_dttm', sa.DateTime(), nullable=False),
            sa.Column('updt_by_nm', sa.String(length=100), nullable=True),
            sa.Column('updt_dttm', sa.DateTime(), nullable=True),
            sa.ForeignKeyConstraint(['role_id'], ['etl_role.id'], ),
            sa.ForeignKeyConstraint(['team_id'], ['etl_team.id'], ondelete='CASCADE'),
            sa.ForeignKeyConstraint(['user_id'], ['etl_user.id'], ondelete='CASCADE'),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('user_id', 'team_id', name='uq_user_team')
        )

def downgrade() -> None:
    # Downgrade is not supported for squashed initial as it represents the base state.
    pass
