"""create_etl_schedule_table

Revision ID: 46919db7c573
Revises: 3af592cd1669
Create Date: 2025-12-29 21:04:08.848552

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '46919db7c573'
down_revision: Union[str, Sequence[str], None] = '8406e311f476'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table('etl_schedule',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('slug', sa.String(length=255), nullable=False),
    sa.Column('cron', sa.String(length=100), nullable=False),
    sa.Column('timezone', sa.String(length=100), nullable=True),
    sa.Column('actv_ind', sa.Boolean(), nullable=True),
    sa.Column('created_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('slug')
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table('etl_schedule')
