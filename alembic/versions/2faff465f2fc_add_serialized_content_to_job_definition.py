"""add_serialized_content_to_job_definition

Revision ID: 2faff465f2fc
Revises: 5ff389487b32
Create Date: 2025-12-31 12:23:34.233166

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2faff465f2fc'
down_revision: Union[str, Sequence[str], None] = '5ff389487b32'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
