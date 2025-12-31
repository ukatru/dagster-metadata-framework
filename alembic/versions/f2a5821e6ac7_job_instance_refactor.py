"""job_instance_refactor

Revision ID: f2a5821e6ac7
Revises: 5ff389487b32
Create Date: 2025-12-30 18:27:41.886581

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f2a5821e6ac7'
down_revision: Union[str, Sequence[str], None] = '5ff389487b32'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # 1. Create etl_job_definition
    op.create_table(
        'etl_job_definition',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('job_nm', sa.String(length=255), nullable=False),
        sa.Column('description', sa.String(length=1000), nullable=True),
        sa.Column('yaml_def', sa.dialects.postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('params_schema', sa.dialects.postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('asset_selection', sa.dialects.postgresql.JSONB(astext_type=sa.Text()), nullable=True),
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
        sa.UniqueConstraint('job_nm', 'team_id', 'code_location_id', name='uq_job_def_team_location')
    )

    # 2. Create etl_job_instance
    op.create_table(
        'etl_job_instance',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('job_definition_id', sa.Integer(), nullable=False),
        sa.Column('instance_id', sa.String(length=255), nullable=False),
        sa.Column('description', sa.String(length=1000), nullable=True),
        sa.Column('schedule_id', sa.Integer(), nullable=True),
        sa.Column('cron_schedule', sa.String(length=100), nullable=True),
        sa.Column('partition_start_dt', sa.Date(), nullable=True),
        sa.Column('actv_ind', sa.Boolean(), nullable=True),
        sa.Column('creat_by_nm', sa.String(length=100), nullable=False),
        sa.Column('creat_dttm', sa.DateTime(), nullable=False),
        sa.Column('updt_by_nm', sa.String(length=100), nullable=True),
        sa.Column('updt_dttm', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['job_definition_id'], ['etl_job_definition.id'], ),
        sa.ForeignKeyConstraint(['schedule_id'], ['etl_schedule.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('job_definition_id', 'instance_id', name='uq_job_instance_def_id')
    )

    # 3. Add job_instance_id to etl_job_parameter
    op.add_column('etl_job_parameter', sa.Column('job_instance_id', sa.Integer(), nullable=True))
    op.create_unique_constraint('uq_job_instance_param', 'etl_job_parameter', ['job_instance_id'])
    op.create_foreign_key('fk_job_instance_param', 'etl_job_parameter', 'etl_job_instance', ['job_instance_id'], ['id'], ondelete='CASCADE')

    # 4. Add job_instance_id to etl_job_status
    op.add_column('etl_job_status', sa.Column('job_instance_id', sa.Integer(), nullable=True))
    op.create_foreign_key('fk_job_instance_status', 'etl_job_status', 'etl_job_instance', ['job_instance_id'], ['id'])

    # 5. DATA MIGRATION
    # 5.1 Migrate Definitions
    # We take unique (job_nm, team_id, code_location_id) from etl_job + merge with etl_params_schema
    op.execute("""
        INSERT INTO etl_job_definition (
            job_nm, description, params_schema, code_location_id, team_id, org_id, 
            actv_ind, creat_by_nm, creat_dttm
        )
        SELECT DISTINCT ON (j.job_nm, j.team_id, j.code_location_id)
            j.job_nm, s.description, s.json_schema, j.code_location_id, j.team_id, j.org_id,
            j.actv_ind, j.creat_by_nm, j.creat_dttm
        FROM etl_job j
        LEFT JOIN etl_params_schema s ON j.job_nm = s.job_nm AND j.team_id = s.team_id AND j.code_location_id = s.code_location_id
        ORDER BY j.job_nm, j.team_id, j.code_location_id, j.creat_dttm DESC
    """)

    # 5.2 Migrate Instances
    op.execute("""
        INSERT INTO etl_job_instance (
            job_definition_id, instance_id, description, schedule_id, cron_schedule, 
            partition_start_dt, actv_ind, creat_by_nm, creat_dttm
        )
        SELECT 
            jd.id, j.invok_id, NULL, j.schedule_id, j.cron_schedule,
            j.partition_start_dt, j.actv_ind, j.creat_by_nm, j.creat_dttm
        FROM etl_job j
        JOIN etl_job_definition jd ON j.job_nm = jd.job_nm AND j.team_id = jd.team_id AND j.code_location_id = jd.code_location_id
    """)

    # 5.3 Point etl_job_parameter to etl_job_instance
    op.execute("""
        UPDATE etl_job_parameter p
        SET job_instance_id = (
            SELECT ji.id 
            FROM etl_job_instance ji
            JOIN etl_job_definition jd ON ji.job_definition_id = jd.id
            JOIN etl_job j ON j.job_nm = jd.job_nm AND j.team_id = jd.team_id AND j.code_location_id = jd.code_location_id AND j.invok_id = ji.instance_id
            WHERE p.etl_job_id = j.id
        )
    """)

    # 5.4 Point etl_job_status to etl_job_instance
    op.execute("""
        UPDATE etl_job_status s
        SET job_instance_id = (
            SELECT ji.id 
            FROM etl_job_instance ji
            JOIN etl_job_definition jd ON ji.job_definition_id = jd.id
            JOIN etl_job j ON j.job_nm = jd.job_nm AND j.team_id = jd.team_id AND j.code_location_id = jd.code_location_id AND j.invok_id = ji.instance_id
            WHERE s.job_nm = j.job_nm AND s.invok_id = j.invok_id AND s.team_id = j.team_id
        )
    """)


def downgrade() -> None:
    """Downgrade schema."""
    # Reversing changes
    op.drop_constraint('fk_job_instance_status', 'etl_job_status', type_='foreignkey')
    op.drop_column('etl_job_status', 'job_instance_id')
    op.drop_constraint('fk_job_instance_param', 'etl_job_parameter', type_='foreignkey')
    op.drop_constraint('uq_job_instance_param', 'etl_job_parameter', type_='unique')
    op.drop_column('etl_job_parameter', 'job_instance_id')
    op.drop_table('etl_job_instance')
    op.drop_table('etl_job_definition')
