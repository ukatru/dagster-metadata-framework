"""scoping_etl_params_schema

Revision ID: 5ff389487b32
Revises: 7637ef72d2dc
Create Date: 2025-12-30 13:07:58.207603

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5ff389487b32'
down_revision: Union[str, Sequence[str], None] = '7637ef72d2dc'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('etl_params_schema', sa.Column('org_id', sa.Integer(), sa.ForeignKey('etl_org.id'), nullable=True))
    op.add_column('etl_params_schema', sa.Column('team_id', sa.Integer(), sa.ForeignKey('etl_team.id'), nullable=True))
    
    # Drop old constraint and add new team-aware one
    op.drop_constraint('uq_job_location_schema', 'etl_params_schema', type_='unique')
    op.create_unique_constraint('uq_job_team_location_schema', 'etl_params_schema', ['job_nm', 'team_id', 'code_location_id'])


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint('uq_job_team_location_schema', 'etl_params_schema', type_='unique')
    op.create_unique_constraint('uq_job_location_schema', 'etl_params_schema', ['job_nm', 'code_location_id'])
    op.drop_column('etl_params_schema', 'team_id')
    op.drop_column('etl_params_schema', 'org_id')
