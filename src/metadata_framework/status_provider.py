import os
from pathlib import Path
import time
import logging
from datetime import datetime
from typing import Optional, Any, Dict
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager

from metadata_framework.models import ETLJobStatus, ETLAssetStatus

from dotenv import load_dotenv

# Logger for observability errors (should not fail the main job)
logger = logging.getLogger("nexus.observability")

class NexusStatusProvider:
    def __init__(self, db_url: Optional[str] = None):
        # 🟢 Use absolute path to ensure we always find the correct .env
        env_path = Path("/home/ukatru/github/dagster-metadata-framework/.env")
        if env_path.exists():
            load_dotenv(dotenv_path=env_path)
            env_status = "Loaded"
        else:
            env_status = f"NOT FOUND at {env_path}"
        
        # Check for Debug mode (if set to TRUE, we will hard-fail on DB errors)
        self.debug_mode = os.getenv("NEXUS_DEBUG", "FALSE").upper() == "TRUE"
        
        if not db_url:
            user = os.getenv("POSTGRES_USER", "postgres")
            password = os.getenv("POSTGRES_PASSWORD", "")
            host = os.getenv("POSTGRES_HOST", "localhost")
            port = os.getenv("POSTGRES_PORT", "5432")
            db = os.getenv("POSTGRES_DB", "postgres")
            db_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
        
        # Diagnostics
        display_url = db_url.replace(password, "****") if (password and password != "") else db_url
        logger.info(f"🚀 Nexus StatusProvider Init: url={display_url}, env={env_status}, debug={self.debug_mode}")
        
        self.engine = create_engine(db_url, pool_pre_ping=True)
        self.Session = sessionmaker(bind=self.engine)

    @contextmanager
    def _session_scope(self):
        """Transactional scope for SQLAlchemy sessions with soft-failure."""
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            msg = f"❌ Nexus Observability Error: {str(e)}"
            logger.error(msg)
            
            # If explicit debug is enabled, we hard-fail
            if self.debug_mode:
                raise RuntimeError(msg) from e
            
            # 🟢 SOFT-FAIL: Default behavior is to keep the pipeline alive
        finally:
            session.close()

    def _retry_with_backoff(self, func, *args, max_retries=3, initial_delay=1, **kwargs):
        """Executes a function with exponential backoff on failure."""
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.warning(f"Nexus Retry Failed after {max_retries} attempts: {str(e)}")
                    if self.debug_mode:
                        raise e
                    return None
                delay = initial_delay * (2 ** attempt)
                logger.info(f"Nexus DB Busy. Retrying in {delay}s... (Attempt {attempt + 1})")
                time.sleep(delay)

    def log_job_start(self, run_id: str, job_nm: str, invok_id: str, run_mode: str, strt_dttm: Optional[datetime] = None) -> Optional[int]:
        """Atomic get-or-create for etl_job_status to handle race conditions."""
        def _execute():
            with self._session_scope() as session:
                actual_start = strt_dttm or datetime.utcnow()
                
                # 1. Atomic Check & Create
                status = session.query(ETLJobStatus).filter_by(run_id=run_id).first()
                if not status:
                    # 🌱 Create fresh record
                    status = ETLJobStatus(
                        run_id=run_id,
                        job_nm=job_nm,
                        invok_id=invok_id,
                        run_mde_txt=run_mode,
                        btch_sts_cd='R',
                        strt_dttm=actual_start
                    )
                    session.add(status)
                    try:
                        session.flush()
                    except Exception:
                        session.rollback()
                        # Someone else won the race, fetch theirs
                        status = session.query(ETLJobStatus).filter_by(run_id=run_id).first()
                else:
                    # 🧩 Update stub record created by Asset if it exists
                    if status.job_nm == "UNKNOWN" or status.btch_sts_cd == 'R':
                        status.job_nm = job_nm
                        status.invok_id = invok_id
                        status.run_mde_txt = run_mode
                
                return status.btch_nbr if status else None
                    
        return self._retry_with_backoff(_execute)

    def log_job_end(self, run_id: str, status_cd: str, end_dttm: Optional[datetime] = None, error_msg: Optional[str] = None):
        """Updates the parent record in etl_job_status, ensures it exists."""
        def _execute():
            with self._session_scope() as session:
                status = session.query(ETLJobStatus).filter_by(run_id=run_id).first()
                if status:
                    status.btch_sts_cd = status_cd
                    status.end_dttm = end_dttm or datetime.utcnow()
                    status.updt_dttm = datetime.utcnow()
                else:
                    logger.warning(f"⚠️ Nexus: log_job_end called for non-existent Run ID: {run_id}")
        return self._retry_with_backoff(_execute)

    def log_asset_start(
        self, 
        run_id: str, 
        asset_nm: str, 
        config_json: Optional[Dict[str, Any]] = None,
        parent_asset_nm: Optional[str] = None,
        partition_key: Optional[str] = None,
        event_type: str = "Materialization",
        strt_dttm: Optional[datetime] = None
    ) -> Optional[int]:
        """Creates a child record in etl_asset_status."""
        def _execute():
            with self._session_scope() as session:
                # 1. Lookup batch number by run_id (with small retry for race condition)
                job_status = None
                for _ in range(3):
                    job_status = session.query(ETLJobStatus).filter_by(run_id=run_id).first()
                    if job_status:
                        break
                    session.rollback() # Ensure we see fresh data
                    time.sleep(0.5)
                
                if not job_status:
                    # 🟢 Self-Healing: Create a stub job record if the sensor is slow.
                    # This prevents the asset from orphaning. The sensor will later "merge" into this.
                    logger.info(f"🌱 Nexus Observability: Creating stub job record for Run ID: {run_id}")
                    
                    job_nm = "UNKNOWN"
                    invok_id = "UNKNOWN"
                    if config_json and "template_vars" in config_json:
                        tags = config_json["template_vars"].get("run_tags", {})
                        invok_id = tags.get("invok_id", "UNKNOWN")
                        metadata = config_json["template_vars"].get("metadata", {})
                        job_nm = metadata.get("_job_nm", "UNKNOWN")
                    
                    job_status = ETLJobStatus(
                        run_id=run_id,
                        job_nm=job_nm,
                        invok_id=invok_id,
                        run_mde_txt='MANUAL', # Default to manual, sensor will update
                        btch_sts_cd='R',
                        strt_dttm=datetime.utcnow()
                    )
                    session.add(job_status)
                    session.flush() # Get the btch_nbr (id)
                
                asset_status = ETLAssetStatus(
                    btch_nbr=job_status.btch_nbr,
                    asset_nm=asset_nm,
                    parent_asset_nm=parent_asset_nm,
                    config_json=config_json,
                    partition_key=partition_key,
                    dagster_event_type=event_type,
                    asset_sts_cd='R',
                    strt_dttm=strt_dttm or datetime.utcnow()
                )
                session.add(asset_status)
                session.flush()
                return asset_status.id
        return self._retry_with_backoff(_execute)

    def log_asset_end(
        self, 
        run_id: str, 
        asset_nm: str, 
        status_cd: str, 
        end_dttm: Optional[datetime] = None,
        error_msg: Optional[str] = None
    ):
        """Updates child record in etl_asset_status."""
        def _execute():
            with self._session_scope() as session:
                # We use the most recent record for this run/asset combo
                job_status = session.query(ETLJobStatus).filter_by(run_id=run_id).first()
                if not job_status:
                    return
                
                asset_status = session.query(ETLAssetStatus).filter_by(
                    btch_nbr=job_status.btch_nbr, 
                    asset_nm=asset_nm
                ).order_by(ETLAssetStatus.strt_dttm.desc()).first()
                
                if asset_status:
                    asset_status.asset_sts_cd = status_cd
                    asset_status.end_dttm = end_dttm or datetime.utcnow()
                    if error_msg:
                        asset_status.err_msg_txt = error_msg
        return self._retry_with_backoff(_execute)

    def sync_asset_timings(self, run_id: str, asset_nm: str, strt_dttm: datetime, end_dttm: datetime):
        """Standardizes asset timings to match official Dagster event logs."""
        def _execute():
            with self._session_scope() as session:
                job_status = session.query(ETLJobStatus).filter_by(run_id=run_id).first()
                if not job_status:
                    return
                
                asset_status = session.query(ETLAssetStatus).filter_by(
                    btch_nbr=job_status.btch_nbr, 
                    asset_nm=asset_nm
                ).first()
                
                if asset_status:
                    asset_status.strt_dttm = strt_dttm
                    asset_status.end_dttm = end_dttm
        return self._retry_with_backoff(_execute)
