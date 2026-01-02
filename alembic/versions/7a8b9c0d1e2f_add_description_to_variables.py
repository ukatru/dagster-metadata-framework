"""add_description_to_variables

Revision ID: 7a8b9c0d1e2f
Revises: 1bf7a9c1e2e3
Create Date: 2026-01-02 12:25:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7a8b9c0d1e2f'
down_revision: Union[str, Sequence[str], None] = '1bf7a9c1e2e3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('etl_global_variables', sa.Column('description', sa.String(length=255), nullable=True))
    op.add_column('etl_team_variables', sa.Column('description', sa.String(length=255), nullable=True))


def downgrade() -> None:
    op.drop_column('etl_team_variables', 'description')
    op.drop_column('etl_global_variables', 'description')
