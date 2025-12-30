"""rename_schema_json_to_json_schema

Revision ID: e0e84631b88c
Revises: b3f0294a0ee1
Create Date: 2025-12-29 20:51:03.440964

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'e0e84631b88c'
down_revision: Union[str, Sequence[str], None] = 'b3f0294a0ee1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.alter_column('etl_conn_type_schema', 'schema_json', new_column_name='json_schema')
    op.alter_column('etl_params_schema', 'schema_json', new_column_name='json_schema')


def downgrade() -> None:
    """Downgrade schema."""
    op.alter_column('etl_conn_type_schema', 'json_schema', new_column_name='schema_json')
    op.alter_column('etl_params_schema', 'json_schema', new_column_name='schema_json')
