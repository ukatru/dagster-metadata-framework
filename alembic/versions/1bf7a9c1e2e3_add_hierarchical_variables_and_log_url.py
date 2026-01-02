"""add_hierarchical_variables_and_log_url

Revision ID: 1bf7a9c1e2e3
Revises: 0cb07e96dc1e
Create Date: 2026-01-02 11:46:59.851701

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1bf7a9c1e2e3'
down_revision: Union[str, Sequence[str], None] = '0cb07e96dc1e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # 1. Create etl_global_variables table
    op.create_table(
        'etl_global_variables',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('org_id', sa.Integer(), sa.ForeignKey('etl_org.id'), nullable=True),
        sa.Column('var_nm', sa.String(length=255), nullable=False),
        sa.Column('var_value', sa.Text(), nullable=True),
        # Audit columns
        sa.Column('creat_by_nm', sa.String(length=100), nullable=False, server_default='DAGSTER'),
        sa.Column('creat_dttm', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('updt_by_nm', sa.String(length=100), nullable=True),
        sa.Column('updt_dttm', sa.DateTime(), nullable=True, onupdate=sa.func.now()),
        sa.UniqueConstraint('org_id', 'var_nm', name='uq_org_variable')
    )

    # 2. Create etl_team_variables table
    op.create_table(
        'etl_team_variables',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('org_id', sa.Integer(), sa.ForeignKey('etl_org.id'), nullable=True),
        sa.Column('team_id', sa.Integer(), sa.ForeignKey('etl_team.id'), nullable=False),
        sa.Column('var_nm', sa.String(length=255), nullable=False),
        sa.Column('var_value', sa.Text(), nullable=True),
        # Audit columns
        sa.Column('creat_by_nm', sa.String(length=100), nullable=False, server_default='DAGSTER'),
        sa.Column('creat_dttm', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('updt_by_nm', sa.String(length=100), nullable=True),
        sa.Column('updt_dttm', sa.DateTime(), nullable=True, onupdate=sa.func.now()),
        sa.UniqueConstraint('team_id', 'var_nm', name='uq_team_variable')
    )

    # 3. Add log_url to etl_job_status
    op.add_column('etl_job_status', sa.Column('log_url', sa.Text(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('etl_job_status', 'log_url')
    op.drop_table('etl_team_variables')
    op.drop_table('etl_global_variables')
