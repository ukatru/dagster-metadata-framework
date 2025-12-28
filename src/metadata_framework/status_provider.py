import os
import time
import logging
from datetime import datetime
from typing import Optional, Any, Dict
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager

from metadata_framework.models import ETLJobStatus, ETLAssetStatus

# Logger for observability errors (should not fail the main job)
logger = logging.getLogger("nexus.observability")

class NexusStatusProvider:
    def __init__(self, db_url: Optional[str] = None):
        if not db_url:
            user = os.getenv("POSTGRES_USER", "postgres")
            password = os.getenv("POSTGRES_PASSWORD", "")
            host = os.getenv("POSTGRES_HOST", "localhost")
            port = os.getenv("POSTGRES_PORT", "5432")
            db = os.getenv("POSTGRES_DB", "postgres")
            db_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
        
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
            logger.warning(f"Nexus Observability Error: {str(e)}")
            # 🟢 SOFT-FAIL: We do NOT re-raise. The main data job must continue.
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
                    return None
                delay = initial_delay * (2 ** attempt)
                logger.info(f"Nexus DB Busy. Retrying in {delay}s... (Attempt {attempt + 1})")
                time.sleep(delay)

    def log_job_start(self, run_id: str, job_nm: str, invok_id: str, run_mode: str) -> Optional[int]:
        """Creates a parent record in etl_job_status."""
        def _execute():
            with self._session_scope() as session:
                status = ETLJobStatus(
                    run_id=run_id,
                    job_nm=job_nm,
                    invok_id=invok_id,
                    run_mde_txt=run_mode,
                    btch_sts_cd='R',
                    strt_dttm=datetime.utcnow()
                )
                session.add(status)
                session.flush() # Populate pk
                return status.btch_nbr
        return self._retry_with_backoff(_execute)

    def log_job_end(self, run_id: str, status_cd: str, error_msg: Optional[str] = None):
        """Updates the parent record in etl_job_status."""
        def _execute():
            with self._session_scope() as session:
                status = session.query(ETLJobStatus).filter_by(run_id=run_id).first()
                if status:
                    status.btch_sts_cd = status_cd
                    status.end_dttm = datetime.utcnow()
                    status.updt_dttm = datetime.utcnow()
                    if error_msg:
                        # Job status doesn't have err_msg, but could be added if needed
                        pass
        return self._retry_with_backoff(_execute)

    def log_asset_start(
        self, 
        run_id: str, 
        asset_nm: str, 
        config_json: Optional[Dict[str, Any]] = None,
        parent_asset_nm: Optional[str] = None,
        partition_key: Optional[str] = None,
        event_type: str = "Materialization"
    ) -> Optional[int]:
        """Creates a child record in etl_asset_status."""
        def _execute():
            with self._session_scope() as session:
                # 1. Lookup batch number by run_id
                job_status = session.query(ETLJobStatus).filter_by(run_id=run_id).first()
                if not job_status:
                    logger.warning(f"Could not find parent job status for Run ID: {run_id}")
                    return None
                
                asset_status = ETLAssetStatus(
                    btch_nbr=job_status.btch_nbr,
                    asset_nm=asset_nm,
                    parent_asset_nm=parent_asset_nm,
                    config_json=config_json,
                    partition_key=partition_key,
                    dagster_event_type=event_type,
                    asset_sts_cd='R',
                    strt_dttm=datetime.utcnow()
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
                    asset_status.end_dttm = datetime.utcnow()
                    if error_msg:
                        asset_status.err_msg_txt = error_msg
        return self._retry_with_backoff(_execute)
