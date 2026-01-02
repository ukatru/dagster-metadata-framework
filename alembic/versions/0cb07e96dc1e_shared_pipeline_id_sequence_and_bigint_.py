"""shared_pipeline_id_sequence_and_bigint_upgrade

Revision ID: 0cb07e96dc1e
Revises: 4f77c3a2b13d
Create Date: 2026-01-02 00:12:43.825716

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0cb07e96dc1e'
down_revision: Union[str, Sequence[str], None] = '4f77c3a2b13d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Create the sequence starting at 1010
    op.execute("CREATE SEQUENCE IF NOT EXISTS etl_pipeline_id_seq START WITH 1010")

    # 2. Upgrade definitions IDs to BIGINT
    # Primary Key
    op.alter_column('etl_job_definition', 'id', type_=sa.BigInteger(), existing_type=sa.Integer())

    # 3. Upgrade instances IDs and FKs to BIGINT
    op.alter_column('etl_job_instance', 'id', type_=sa.BigInteger(), existing_type=sa.Integer())
    op.alter_column('etl_job_instance', 'job_definition_id', type_=sa.BigInteger(), existing_type=sa.Integer())

    # 4. Upgrade other FKs
    op.alter_column('etl_job_parameter', 'job_definition_id', type_=sa.BigInteger(), existing_type=sa.Integer())
    op.alter_column('etl_params_schema', 'job_definition_id', type_=sa.BigInteger(), existing_type=sa.Integer())
    op.alter_column('etl_instance_parameter', 'instance_pk', type_=sa.BigInteger(), existing_type=sa.Integer())

    # 5. Set default values for IDs to use the sequence
    op.execute("ALTER TABLE etl_job_definition ALTER COLUMN id SET DEFAULT nextval('etl_pipeline_id_seq')")
    op.execute("ALTER TABLE etl_job_instance ALTER COLUMN id SET DEFAULT nextval('etl_pipeline_id_seq')")


def downgrade() -> None:
    # 1. Remove default values
    op.execute("ALTER TABLE etl_job_definition ALTER COLUMN id DROP DEFAULT")
    op.execute("ALTER TABLE etl_job_instance ALTER COLUMN id DROP DEFAULT")

    # 2. Downgrade other FKs back to Integer (assuming no data exceeds 2B)
    op.alter_column('etl_instance_parameter', 'instance_pk', type_=sa.Integer(), existing_type=sa.BigInteger())
    op.alter_column('etl_params_schema', 'job_definition_id', type_=sa.Integer(), existing_type=sa.BigInteger())
    op.alter_column('etl_job_parameter', 'job_definition_id', type_=sa.Integer(), existing_type=sa.BigInteger())

    # 3. Downgrade instances IDs and FKs
    op.alter_column('etl_job_instance', 'job_definition_id', type_=sa.Integer(), existing_type=sa.BigInteger())
    op.alter_column('etl_job_instance', 'id', type_=sa.Integer(), existing_type=sa.BigInteger())

    # 4. Downgrade definitions IDs
    op.alter_column('etl_job_definition', 'id', type_=sa.Integer(), existing_type=sa.BigInteger())

    # 5. Drop the sequence
    op.execute("DROP SEQUENCE IF EXISTS etl_pipeline_id_seq")
