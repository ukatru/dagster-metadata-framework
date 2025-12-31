"""enforce_scoping_non_nullable

Revision ID: 46f5d6eb309a
Revises: c86fc6fe70be
Create Date: 2025-12-31 00:03:50.047849

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '46f5d6eb309a'
down_revision: Union[str, Sequence[str], None] = 'c86fc6fe70be'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Enforce non-nullable scoping columns."""
    # etl_job_template
    op.alter_column('etl_job_template', 'org_id', nullable=False)
    op.alter_column('etl_job_template', 'team_id', nullable=False)
    op.alter_column('etl_job_template', 'code_location_id', nullable=False)
    
    # etl_job_definition
    op.alter_column('etl_job_definition', 'org_id', nullable=False)
    op.alter_column('etl_job_definition', 'team_id', nullable=False)
    op.alter_column('etl_job_definition', 'code_location_id', nullable=False)


def downgrade() -> None:
    """Allow nullable scoping columns."""
    # etl_job_definition
    op.alter_column('etl_job_definition', 'code_location_id', nullable=True)
    op.alter_column('etl_job_definition', 'team_id', nullable=True)
    op.alter_column('etl_job_definition', 'org_id', nullable=True)

    # etl_job_template
    op.alter_column('etl_job_template', 'code_location_id', nullable=True)
    op.alter_column('etl_job_template', 'team_id', nullable=True)
    op.alter_column('etl_job_template', 'org_id', nullable=True)
