import functools
import logging
from typing import Any, Dict, List, Optional
from dagster import (
    DagsterRunStatus,
    RunStatusSensorContext,
    run_status_sensor
)

from metadata_framework.status_provider import NexusStatusProvider

from datetime import datetime, timezone

logger = logging.getLogger("nexus.observability")

@run_status_sensor(run_status=DagsterRunStatus.STARTED)
def nexus_job_started_sensor(context: RunStatusSensorContext):
    provider = NexusStatusProvider()
    run = context.dagster_run
    tags = run.tags if hasattr(run, "tags") else {}
    invok_id = tags.get("invok_id", "MANUAL")
    job_nm = tags.get("job_nm") or run.job_name or "UNKNOWN_JOB"
    is_scheduled = any(k.startswith("dagster/schedule") for k in tags.keys())
    run_mode = "SCHEDULED" if is_scheduled else "MANUAL"
    
    # Accurate start time from Dagster
    stats = context.instance.get_run_stats(run.run_id)
    strt_dttm = datetime.fromtimestamp(stats.start_time, tz=timezone.utc).replace(tzinfo=None) if stats.start_time else None
    
    context.log.info(f"Nexus Observability: Logging START for job={job_nm}, run_id={run.run_id}, mode={run_mode}, start_time={strt_dttm}")
    try:
        provider.log_job_start(run.run_id, job_nm, invok_id, run_mode, strt_dttm=strt_dttm)
    except Exception as e:
        context.log.error(f"❌ Nexus Observability: Failed to log job start: {e}")

@run_status_sensor(run_status=DagsterRunStatus.SUCCESS)
def nexus_job_success_sensor(context: RunStatusSensorContext):
    provider = NexusStatusProvider()
    run = context.dagster_run
    
    stats = context.instance.get_run_stats(run.run_id)
    end_dttm = datetime.fromtimestamp(stats.end_time, tz=timezone.utc).replace(tzinfo=None) if stats.end_time else None
    
    context.log.info(f"Nexus Observability: Logging SUCCESS for run_id={run.run_id}, end_time={end_dttm}")
    try:
        provider.log_job_end(run.run_id, status_cd='C', end_dttm=end_dttm)
    except Exception as e:
        context.log.error(f"❌ Nexus Observability: Failed to log job success: {e}")
    
    # 🟢 Step 3.3.5: Sync official step timestamps
    try:
        step_stats = context.instance.get_run_step_stats(run.run_id)
        for step in step_stats:
            # Strip partition suffix (e.g., 'asset_nm[partition]') to match etl_asset_status.asset_nm
            base_asset_nm = step.step_key.split("[")[0]
            s_start = datetime.fromtimestamp(step.start_time, tz=timezone.utc).replace(tzinfo=None) if step.start_time else None
            s_end = datetime.fromtimestamp(step.end_time, tz=timezone.utc).replace(tzinfo=None) if step.end_time else None
            
            if s_start and s_end:
                context.log.info(f"Nexus Observability: Syncing timings for {base_asset_nm} ({step.step_key})")
                provider.sync_asset_timings(run.run_id, base_asset_nm, s_start, s_end)
    except Exception as e:
        context.log.warning(f"Failed to sync asset timings: {e}")

@run_status_sensor(run_status=DagsterRunStatus.FAILURE)
def nexus_job_failure_sensor(context: RunStatusSensorContext):
    provider = NexusStatusProvider()
    run = context.dagster_run
    
    stats = context.instance.get_run_stats(run.run_id)
    end_dttm = datetime.fromtimestamp(stats.end_time, tz=timezone.utc).replace(tzinfo=None) if stats.end_time else None
    
    context.log.info(f"Nexus Observability: Logging FAILURE for run_id={run.run_id}, end_time={end_dttm}")
    try:
        provider.log_job_end(run.run_id, status_cd='A', end_dttm=end_dttm)
    except Exception as e:
        context.log.error(f"❌ Nexus Observability: Failed to log job failure: {e}")
    
    # Sync timings even on failure (important for seeing where it stopped)
    try:
        step_stats = context.instance.get_run_step_stats(run.run_id)
        for step in step_stats:
            base_asset_nm = step.step_key.split("[")[0]
            s_start = datetime.fromtimestamp(step.start_time, tz=timezone.utc).replace(tzinfo=None) if step.start_time else None
            s_end = datetime.fromtimestamp(step.end_time, tz=timezone.utc).replace(tzinfo=None) if step.end_time else None
            
            if s_start and s_end:
                context.log.info(f"Nexus Observability: Syncing failure timings for {base_asset_nm}")
                provider.sync_asset_timings(run.run_id, base_asset_nm, s_start, s_end)
    except Exception as e:
        context.log.warning(f"Failed to sync failure asset timings: {e}")

@run_status_sensor(run_status=DagsterRunStatus.CANCELED)
def nexus_job_canceled_sensor(context: RunStatusSensorContext):
    provider = NexusStatusProvider()
    run = context.dagster_run
    
    stats = context.instance.get_run_stats(run.run_id)
    end_dttm = datetime.fromtimestamp(stats.end_time, tz=timezone.utc).replace(tzinfo=None) if stats.end_time else None
    
    context.log.info(f"Nexus Observability: Logging CANCELLATION for run_id={run.run_id}, end_time={end_dttm}")
    try:
        provider.log_job_end(run.run_id, status_cd='A', end_dttm=end_dttm) # Mark as Aborted
    except Exception as e:
        context.log.error(f"❌ Nexus Observability: Failed to log job cancellation: {e}")

# Sensor list for registration
nexus_listeners = [
    nexus_job_started_sensor,
    nexus_job_success_sensor,
    nexus_job_failure_sensor,
    nexus_job_canceled_sensor
]

def to_json_serializable(obj):
    """Helper to ensure complex objects can be stored in JSONB."""
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if hasattr(obj, "__dict__"):
        return {k: to_json_serializable(v) for k, v in obj.__dict__.items() if not k.startswith("_")}
    if isinstance(obj, dict):
        return {k: to_json_serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [to_json_serializable(v) for v in obj]
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    return str(obj)


class NexusObservability:
    """Extension for asset-level tracking (Phase 3.3)"""
    def __init__(self, db_url: Optional[str] = None):
        self.provider = NexusStatusProvider(db_url)

    def log_job_start_once(self, context):
        """Deprecated in favor of the flight recorder sensor."""
        pass

    def wrap_operator_execute(self, name: str, original_execute: Any):
        """
        Wraps the operator's execute method to capture asset-level start/end/config.
        """
        @functools.wraps(original_execute)
        def tracked_execute(context, source_config, target_config, template_vars, **kwargs):
            
            run_id = context.run_id
            asset_nm = name
            
            # 🟢 Step 3.3.3: Capture high-fidelity config snapshots
            # We prioritize specific keys to avoid bloat
            pruned_tpl_vars = {}
            if isinstance(template_vars, dict):
                for k in ["vars", "run_tags", "metadata", "partition_key", "trigger"]:
                    if k in template_vars:
                        pruned_tpl_vars[k] = to_json_serializable(template_vars[k])

            full_config = {
                "source": to_json_serializable(source_config),
                "target": to_json_serializable(target_config),
                "template_vars": pruned_tpl_vars
            }
            
            # Start timing
            start_time = datetime.utcnow()
            
            self.provider.log_asset_start(
                run_id=run_id,
                asset_nm=asset_nm,
                config_json=full_config,
                partition_key=context.partition_key if hasattr(context, "has_partition_key") and context.has_partition_key else None,
                strt_dttm=start_time
            )
            
            try:
                result = original_execute(
                    context=context,
                    source_config=source_config,
                    target_config=target_config,
                    template_vars=template_vars,
                    **kwargs
                )
                self.provider.log_asset_end(run_id, asset_nm, status_cd='C', end_dttm=datetime.utcnow())
                return result
            except Exception as e:
                self.provider.log_asset_end(run_id, asset_nm, status_cd='A', end_dttm=datetime.utcnow(), error_msg=str(e))
                raise e

        return tracked_execute
