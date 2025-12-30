from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, Text, ForeignKey, BigInteger, CHAR, UniqueConstraint, Date
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class AuditMixin:
    """
    Mixin to add mandatory audit columns to every table.
    Ensures traceability of who/what created and updated records.
    """
    creat_by_nm = Column(String(100), nullable=False, default='DAGSTER')
    creat_dttm = Column(DateTime, nullable=False, default=datetime.utcnow)
    updt_by_nm = Column(String(100))
    updt_dttm = Column(DateTime, onupdate=datetime.utcnow)

class ETLConnection(Base, AuditMixin):
    __tablename__ = "etl_connection"
    
    id = Column(Integer, primary_key=True)
    conn_nm = Column(String(255), unique=True, nullable=False)
    conn_type = Column(String(50), nullable=False)
    config_json = Column(JSONB, nullable=False, default={})
    # Removing legacy created_at in favor of AuditMixin

class ETLSchedule(Base, AuditMixin):
    __tablename__ = "etl_schedule"
    
    id = Column(Integer, primary_key=True)
    slug = Column(String(255), unique=True, nullable=False)
    cron = Column(String(100), nullable=False)
    timezone = Column(String(100))
    actv_ind = Column(Boolean, default=True)
    # Removing legacy created_at in favor of AuditMixin

class ETLJob(Base, AuditMixin):
    __tablename__ = "etl_job"
    
    id = Column(Integer, primary_key=True)
    job_nm = Column(String(255), nullable=False)
    invok_id = Column(String(255), nullable=False)
    source_conn_nm = Column(String(255), ForeignKey("etl_connection.conn_nm"))
    target_conn_nm = Column(String(255), ForeignKey("etl_connection.conn_nm"))
    schedule_id = Column(Integer, ForeignKey("etl_schedule.id")) # Linked to centralized schedule
    cron_schedule = Column(String(100)) # Legacy, will be deprecated
    partition_start_dt = Column(Date)
    actv_ind = Column(Boolean, default=True)
    # Removing legacy created_at in favor of AuditMixin
    
    __table_args__ = (
        UniqueConstraint("job_nm", "invok_id", name="uq_job_invok"),
        {"sqlite_autoincrement": True}, # For testing if needed
    )

class ETLJobParameter(Base, AuditMixin):
    __tablename__ = "etl_job_parameter"
    
    id = Column(Integer, primary_key=True)
    etl_job_id = Column(Integer, ForeignKey("etl_job.id", ondelete="CASCADE"), unique=True)
    config_json = Column(JSONB, nullable=False, default={})
    # Removing legacy updated_at in favor of AuditMixin

class ETLParameter(Base, AuditMixin):
    __tablename__ = "etl_parameter"
    
    id = Column(Integer, primary_key=True)
    parm_nm = Column(String(255), unique=True, nullable=False)
    parm_value = Column(Text)
    # Removing legacy created_at in favor of AuditMixin

class ETLParamsSchema(Base, AuditMixin):
    """
    Developer Contract Table.
    Stores the expected parameter schema for a job name.
    """
    __tablename__ = "etl_params_schema"
    
    id = Column(Integer, primary_key=True)
    job_nm = Column(String(255), unique=True, nullable=False)
    json_schema = Column(JSONB, nullable=False)
    description = Column(Text)
    is_strict = Column(Boolean, default=False)

class ETLConnTypeSchema(Base, AuditMixin):
    """
    Connection Type Schema Registry.
    Stores the expected configuration schema for each connection type (S3, SFTP, etc).
    """
    __tablename__ = "etl_conn_type_schema"
    
    id = Column(Integer, primary_key=True)
    conn_type = Column(String(50), unique=True, nullable=False)
    json_schema = Column(JSONB, nullable=False)
    description = Column(Text)

class ETLJobStatus(Base, AuditMixin):
    __tablename__ = "etl_job_status"
    
    btch_nbr = Column(BigInteger, primary_key=True, autoincrement=True)
    run_id = Column(String(64), unique=True, nullable=False)
    job_nm = Column(String(256), nullable=False)
    invok_id = Column(String(255))
    strt_dttm = Column(DateTime, default=datetime.utcnow)
    end_dttm = Column(DateTime)
    btch_sts_cd = Column(CHAR(1), default='R') # R, C, A
    run_mde_txt = Column(String(50), nullable=False) # SCHEDULED, MANUAL, BACKFILL
    # Audit columns are now provided by Mixin

class ETLAssetStatus(Base, AuditMixin):
    __tablename__ = "etl_asset_status"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    btch_nbr = Column(BigInteger, ForeignKey("etl_job_status.btch_nbr", ondelete="CASCADE"), nullable=False)
    asset_nm = Column(String(256), nullable=False)
    parent_assets = Column(JSONB) # List of upstream asset names
    config_json = Column(JSONB)
    partition_key = Column(String(255))
    dagster_event_type = Column(String(50)) # Materialization, Check, etc.
    strt_dttm = Column(DateTime, default=datetime.utcnow)
    end_dttm = Column(DateTime)
    asset_sts_cd = Column(CHAR(1), default='R') # R, C, A
    err_msg_txt = Column(Text)
    # Audit columns are now provided by Mixin

class ETLRole(Base, AuditMixin):
    """
    Role Registry.
    Defines the permission tiers: DPE_PLATFORM_ADMIN, DPE_DEVELOPER, DPE_DATA_ANALYST.
    """
    __tablename__ = "etl_role"
    
    id = Column(Integer, primary_key=True)
    role_nm = Column(String(100), unique=True, nullable=False)
    description = Column(String(255))
    actv_ind = Column(Boolean, default=True)

    # Relationships
    users = relationship("ETLUser", back_populates="role")

class ETLUser(Base, AuditMixin):
    """
    User Registry.
    Stores identities and secure credential hashes.
    """
    __tablename__ = "etl_user"
    
    id = Column(Integer, primary_key=True)
    username = Column(String(100), unique=True, nullable=False, index=True)
    hashed_password = Column(String(255), nullable=False)  # Salted bcrypt hash
    full_nm = Column(String(255), nullable=False)
    email = Column(String(255))
    
    # FK to role
    role_id = Column(Integer, ForeignKey("etl_role.id"), nullable=False)
    
    actv_ind = Column(Boolean, default=True)

    # Relationships
    role = relationship("ETLRole", back_populates="users")
