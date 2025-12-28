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
    provider.log_job_start(run.run_id, job_nm, invok_id, run_mode, strt_dttm=strt_dttm)

@run_status_sensor(run_status=DagsterRunStatus.SUCCESS)
def nexus_job_success_sensor(context: RunStatusSensorContext):
    provider = NexusStatusProvider()
    run = context.dagster_run
    
    stats = context.instance.get_run_stats(run.run_id)
    end_dttm = datetime.fromtimestamp(stats.end_time, tz=timezone.utc).replace(tzinfo=None) if stats.end_time else None
    
    context.log.info(f"Nexus Observability: Logging SUCCESS for run_id={run.run_id}, end_time={end_dttm}")
    provider.log_job_end(run.run_id, status_cd='C', end_dttm=end_dttm)

@run_status_sensor(run_status=DagsterRunStatus.FAILURE)
def nexus_job_failure_sensor(context: RunStatusSensorContext):
    provider = NexusStatusProvider()
    run = context.dagster_run
    
    stats = context.instance.get_run_stats(run.run_id)
    end_dttm = datetime.fromtimestamp(stats.end_time, tz=timezone.utc).replace(tzinfo=None) if stats.end_time else None
    
    context.log.info(f"Nexus Observability: Logging FAILURE for run_id={run.run_id}, end_time={end_dttm}")
    provider.log_job_end(run.run_id, status_cd='A', end_dttm=end_dttm)

@run_status_sensor(run_status=DagsterRunStatus.CANCELED)
def nexus_job_canceled_sensor(context: RunStatusSensorContext):
    provider = NexusStatusProvider()
    run = context.dagster_run
    
    stats = context.instance.get_run_stats(run.run_id)
    end_dttm = datetime.fromtimestamp(stats.end_time, tz=timezone.utc).replace(tzinfo=None) if stats.end_time else None
    
    context.log.info(f"Nexus Observability: Logging CANCELLATION for run_id={run.run_id}, end_time={end_dttm}")
    provider.log_job_end(run.run_id, status_cd='A', end_dttm=end_dttm) # Mark as Aborted

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
            
            # Prune and serialize template_vars
            pruned_tpl_vars = {}
            if isinstance(template_vars, dict):
                # We prioritize logging specific keys to avoid bloat
                for k in ["vars", "run_tags", "metadata", "partition_key", "trigger"]:
                    if k in template_vars:
                        pruned_tpl_vars[k] = to_json_serializable(template_vars[k])

            full_config = {
                "source": to_json_serializable(source_config),
                "target": to_json_serializable(target_config),
                "template_vars": pruned_tpl_vars
            }
            
            self.provider.log_asset_start(
                run_id=run_id,
                asset_nm=asset_nm,
                config_json=full_config,
                partition_key=context.partition_key if hasattr(context, "has_partition_key") and context.has_partition_key else None
            )
            
            try:
                result = original_execute(
                    context=context,
                    source_config=source_config,
                    target_config=target_config,
                    template_vars=template_vars,
                    **kwargs
                )
                self.provider.log_asset_end(run_id, asset_nm, status_cd='C')
                return result
            except Exception as e:
                self.provider.log_asset_end(run_id, asset_nm, status_cd='A', error_msg=str(e))
                raise e

        return tracked_execute
