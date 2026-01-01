"""consolidate_job_models

Revision ID: 1282532b263a
Revises: 9eea77a96cc5
Create Date: 2025-12-31 15:46:10.013469

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1282532b263a'
down_revision: Union[str, Sequence[str], None] = '9eea77a96cc5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Drop the legacy etl_job table
    op.drop_table('etl_job')
    
    # 2. Refine etl_job_instance
    op.add_column('etl_job_instance', sa.Column('code_location_id', sa.Integer(), sa.ForeignKey('etl_code_location.id'), nullable=True))
    
    # 3. Ensure actv_ind has proper defaults and values
    # First, update existing NULLs to TRUE
    op.execute("UPDATE etl_job_instance SET actv_ind = TRUE WHERE actv_ind IS NULL")
    
    # Set default and non-nullable
    op.alter_column('etl_job_instance', 'actv_ind',
               existing_type=sa.BOOLEAN(),
               nullable=False,
               server_default=sa.text('true'))

def downgrade() -> None:
    # Revert actv_ind changes
    op.alter_column('etl_job_instance', 'actv_ind',
               existing_type=sa.BOOLEAN(),
               nullable=True,
               server_default=None)
    
    # Recreate etl_job
    op.create_table('etl_job',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('job_nm', sa.String(255), nullable=False),
        sa.Column('instance_id', sa.String(255), nullable=False),
        sa.Column('org_id', sa.Integer(), sa.ForeignKey('etl_org.id')),
        sa.Column('team_id', sa.Integer(), sa.ForeignKey('etl_team.id')),
        sa.Column('code_location_id', sa.Integer(), sa.ForeignKey('etl_code_location.id')),
        sa.Column('schedule_id', sa.Integer(), sa.ForeignKey('etl_schedule.id')),
        sa.Column('cron_schedule', sa.String(100)),
        sa.Column('partition_start_dt', sa.Date()),
        sa.Column('actv_ind', sa.Boolean(), default=True),
        sa.Column('creat_by_nm', sa.String(100), nullable=False),
        sa.Column('creat_dttm', sa.DateTime(), nullable=False),
        sa.Column('updt_by_nm', sa.String(100)),
        sa.Column('updt_dttm', sa.DateTime())
    )
    
    # Remove column from etl_job_instance
    op.drop_column('etl_job_instance', 'code_location_id')
