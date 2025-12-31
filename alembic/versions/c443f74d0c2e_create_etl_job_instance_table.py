"""create_etl_job_instance_table

Revision ID: c443f74d0c2e
Revises: b244dc2ea856
Create Date: 2025-12-30 22:22:XX

Phase 18: Collapsed Architecture - Separate YAML jobs from user-created instances
- etl_job_definition: YAML-defined singleton jobs (read-only, synced from YAML)
- etl_job_instance: User-created jobs from templates (editable via UI)
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'c443f74d0c2e'
down_revision: Union[str, Sequence[str], None] = 'b244dc2ea856'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create etl_job_instance table for user-created pipeline instances."""
    op.create_table(
        'etl_job_instance',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('instance_nm', sa.String(255), nullable=False, comment='User-defined instance name'),
        sa.Column('template_id', sa.Integer(), nullable=False, comment='Reference to etl_job_template'),
        sa.Column('params_json', postgresql.JSONB, nullable=True, comment='Runtime parameters (overrides template defaults)'),
        sa.Column('cron_schedule', sa.String(100), nullable=True, comment='Cron expression for scheduling'),
        sa.Column('partition_start_dt', sa.DateTime(timezone=True), nullable=True),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('actv_ind', sa.Boolean(), nullable=False, server_default='true'),
        
        # Scoping
        sa.Column('team_id', sa.Integer(), nullable=False),
        sa.Column('org_id', sa.Integer(), nullable=False),
        sa.Column('code_location_id', sa.Integer(), nullable=False),
        
        # Audit
        sa.Column('creat_by_nm', sa.String(100), nullable=False),
        sa.Column('creat_dttm', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('updt_by_nm', sa.String(100), nullable=False),
        sa.Column('updt_dttm', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
        
        # Foreign Keys
        sa.ForeignKeyConstraint(['template_id'], ['etl_job_template.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['team_id'], ['etl_team.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['org_id'], ['etl_org.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['code_location_id'], ['etl_code_location.id'], ondelete='CASCADE'),
        
        # Unique constraint: instance name must be unique within team/location
        sa.UniqueConstraint('instance_nm', 'team_id', 'code_location_id', name='uq_job_instance_name_team_location')
    )
    
    # Indexes for performance
    op.create_index('ix_job_instance_template_id', 'etl_job_instance', ['template_id'])
    op.create_index('ix_job_instance_team_id', 'etl_job_instance', ['team_id'])
    op.create_index('ix_job_instance_actv_ind', 'etl_job_instance', ['actv_ind'])


def downgrade() -> None:
    """Drop etl_job_instance table."""
    op.drop_index('ix_job_instance_actv_ind', table_name='etl_job_instance')
    op.drop_index('ix_job_instance_team_id', table_name='etl_job_instance')
    op.drop_index('ix_job_instance_template_id', table_name='etl_job_instance')
    op.drop_table('etl_job_instance')
