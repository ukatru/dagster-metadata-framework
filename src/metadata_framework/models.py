from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, Text, ForeignKey, BigInteger, CHAR, UniqueConstraint
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class ETLConnection(Base):
    __tablename__ = "etl_connection"
    
    id = Column(Integer, primary_key=True)
    conn_nm = Column(String(255), unique=True, nullable=False)
    conn_type = Column(String(50), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

class ETLJob(Base):
    __tablename__ = "etl_job"
    
    id = Column(Integer, primary_key=True)
    job_nm = Column(String(255), nullable=False)
    invok_id = Column(String(255), nullable=False)
    source_conn_nm = Column(String(255), ForeignKey("etl_connection.conn_nm"))
    target_conn_nm = Column(String(255), ForeignKey("etl_connection.conn_nm"))
    cron_schedule = Column(String(100))
    actv_ind = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        UniqueConstraint("job_nm", "invok_id", name="uq_job_invok"),
        {"sqlite_autoincrement": True}, # For testing if needed
    )

class ETLJobParameter(Base):
    __tablename__ = "etl_job_parameter"
    
    id = Column(Integer, primary_key=True)
    etl_job_id = Column(Integer, ForeignKey("etl_job.id", ondelete="CASCADE"), unique=True)
    config_json = Column(JSONB, nullable=False, default={})
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class ETLParameter(Base):
    __tablename__ = "etl_parameter"
    
    id = Column(Integer, primary_key=True)
    parm_nm = Column(String(255), unique=True, nullable=False)
    parm_value = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)

class ETLJobStatus(Base):
    __tablename__ = "etl_job_status"
    
    btch_nbr = Column(BigInteger, primary_key=True, autoincrement=True)
    run_id = Column(String(64), unique=True, nullable=False)
    job_nm = Column(String(256), nullable=False)
    invok_id = Column(String(255))
    strt_dttm = Column(DateTime, default=datetime.utcnow)
    end_dttm = Column(DateTime)
    btch_sts_cd = Column(CHAR(1), default='R') # R, C, A
    run_mde_txt = Column(String(50), nullable=False) # SCHEDULED, MANUAL, BACKFILL
    updt_by_nm = Column(String(30))
    updt_dttm = Column(DateTime, onupdate=datetime.utcnow)
    creat_by_nm = Column(String(30), default='DAGSTER')
    creat_dttm = Column(DateTime, default=datetime.utcnow)

class ETLAssetStatus(Base):
    __tablename__ = "etl_asset_status"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    btch_nbr = Column(BigInteger, ForeignKey("etl_job_status.btch_nbr", ondelete="CASCADE"), nullable=False)
    asset_nm = Column(String(256), nullable=False)
    parent_asset_nm = Column(String(256))
    config_json = Column(JSONB)
    partition_key = Column(String(255))
    dagster_event_type = Column(String(50)) # Materialization, Check, etc.
    strt_dttm = Column(DateTime, default=datetime.utcnow)
    end_dttm = Column(DateTime)
    asset_sts_cd = Column(CHAR(1), default='R') # R, C, A
    err_msg_txt = Column(Text)
    creat_dttm = Column(DateTime, default=datetime.utcnow)
