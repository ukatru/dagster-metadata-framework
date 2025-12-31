"""add_unique_constraint_job_definition

Revision ID: c86fc6fe70be
Revises: c443f74d0c2e
Create Date: 2025-12-30 22:28:XX

Prevent duplicate job definitions by adding unique constraint on job_nm + team_id + code_location_id
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'c86fc6fe70be'
down_revision: Union[str, Sequence[str], None] = 'c443f74d0c2e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add unique constraint to prevent duplicate job definitions."""
    op.create_unique_constraint(
        'uq_job_definition_name_team_location',
        'etl_job_definition',
        ['job_nm', 'team_id', 'code_location_id']
    )


def downgrade() -> None:
    """Remove unique constraint."""
    op.drop_constraint('uq_job_definition_name_team_location', 'etl_job_definition', type_='unique')
