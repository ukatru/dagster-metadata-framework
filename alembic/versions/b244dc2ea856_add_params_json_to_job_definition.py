"""add_params_json_to_job_definition

Revision ID: b244dc2ea856
Revises: fb5aff861891
Create Date: 2025-12-30 22:15:59.611453

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b244dc2ea856'
down_revision: Union[str, Sequence[str], None] = 'fb5aff861891'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add params_json column to etl_job_definition for runtime parameters."""
    op.add_column('etl_job_definition', 
        sa.Column('params_json', sa.dialects.postgresql.JSONB, nullable=True)
    )


def downgrade() -> None:
    """Remove params_json column from etl_job_definition."""
    op.drop_column('etl_job_definition', 'params_json')
