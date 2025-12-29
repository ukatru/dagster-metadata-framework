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

def make_nexus_listeners(location_name: Optional[str] = None, global_monitor: bool = False) -> List[Any]:
    """
    Dynamic factory for Nexus observability sensors.
    
    Args:
        location_name: Optional prefix for sensor names to avoid collisions in Cloud.
        global_monitor: If True, uses 'monitor_all_code_locations' to track all runs in the workspace.
    """
    prefix = f"{location_name}_" if location_name else ""
    
    # 🟢 1. Job Started Sensor
    @run_status_sensor(
        name=f"{prefix}nexus_job_started_sensor",
        run_status=DagsterRunStatus.STARTED,
        monitor_all_code_locations=global_monitor,
        request_job=None # Only if needed for requests
    )
    def started_sensor(context: RunStatusSensorContext):
        provider = NexusStatusProvider()
        run = context.dagster_run
        tags = run.tags if hasattr(run, "tags") else {}
        invok_id = tags.get("invok_id", "MANUAL")
        job_nm = tags.get("job_nm") or run.job_name or "UNKNOWN_JOB"
        
        is_scheduled = any(k.startswith("dagster/schedule") for k in tags.keys())
        run_mode = "SCHEDULED" if is_scheduled else "MANUAL"
        
        stats = context.instance.get_run_stats(run.run_id)
        strt_dttm = datetime.fromtimestamp(stats.start_time, tz=timezone.utc).replace(tzinfo=None) if stats.start_time else None
        
        context.log.info(f"Nexus Observability: Logging START for job={job_nm}, run_id={run.run_id}")
        try:
            provider.log_job_start(run.run_id, job_nm, invok_id, run_mode, strt_dttm=strt_dttm)
        except Exception as e:
            context.log.error(f"❌ Nexus Observability: Failed to log job start: {e}")

    # 🟢 2. Job Success Sensor
    @run_status_sensor(
        name=f"{prefix}nexus_job_success_sensor",
        run_status=DagsterRunStatus.SUCCESS,
        monitor_all_code_locations=global_monitor
    )
    def success_sensor(context: RunStatusSensorContext):
        provider = NexusStatusProvider()
        run = context.dagster_run
        stats = context.instance.get_run_stats(run.run_id)
        end_dttm = datetime.fromtimestamp(stats.end_time, tz=timezone.utc).replace(tzinfo=None) if stats.end_time else None
        
        try:
            provider.log_job_end(run.run_id, status_cd='C', end_dttm=end_dttm)
            # Sync step-level timings (Assets)
            step_stats = context.instance.get_run_step_stats(run.run_id)
            for step in step_stats:
                base_asset_nm = step.step_key.split("[")[0]
                s_start = datetime.fromtimestamp(step.start_time, tz=timezone.utc).replace(tzinfo=None) if step.start_time else None
                s_end = datetime.fromtimestamp(step.end_time, tz=timezone.utc).replace(tzinfo=None) if step.end_time else None
                if s_start and s_end:
                    provider.sync_asset_timings(run.run_id, base_asset_nm, s_start, s_end)
        except Exception as e:
            context.log.error(f"❌ Nexus Observability: Failed to finalize job success: {e}")

    # 🟢 3. Job Failure Sensor
    @run_status_sensor(
        name=f"{prefix}nexus_job_failure_sensor",
        run_status=DagsterRunStatus.FAILURE,
        monitor_all_code_locations=global_monitor
    )
    def failure_sensor(context: RunStatusSensorContext):
        provider = NexusStatusProvider()
        run = context.dagster_run
        stats = context.instance.get_run_stats(run.run_id)
        end_dttm = datetime.fromtimestamp(stats.end_time, tz=timezone.utc).replace(tzinfo=None) if stats.end_time else None
        
        try:
            provider.log_job_end(run.run_id, status_cd='A', end_dttm=end_dttm)
            # Sync timings even on failure
            step_stats = context.instance.get_run_step_stats(run.run_id)
            for step in step_stats:
                base_asset_nm = step.step_key.split("[")[0]
                s_start = datetime.fromtimestamp(step.start_time, tz=timezone.utc).replace(tzinfo=None) if step.start_time else None
                s_end = datetime.fromtimestamp(step.end_time, tz=timezone.utc).replace(tzinfo=None) if step.end_time else None
                if s_start and s_end:
                    provider.sync_asset_timings(run.run_id, base_asset_nm, s_start, s_end)
        except Exception as e:
            context.log.error(f"❌ Nexus Observability: Failed to log job failure: {e}")

    # 🟢 4. Job Canceled Sensor
    @run_status_sensor(
        name=f"{prefix}nexus_job_canceled_sensor",
        run_status=DagsterRunStatus.CANCELED,
        monitor_all_code_locations=global_monitor
    )
    def canceled_sensor(context: RunStatusSensorContext):
        provider = NexusStatusProvider()
        run = context.dagster_run
        stats = context.instance.get_run_stats(run.run_id)
        end_dttm = datetime.fromtimestamp(stats.end_time, tz=timezone.utc).replace(tzinfo=None) if stats.end_time else None
        try:
            provider.log_job_end(run.run_id, status_cd='A', end_dttm=end_dttm)
        except Exception as e:
            context.log.error(f"❌ Nexus Observability: Failed to log job cancellation: {e}")

    return [started_sensor, success_sensor, failure_sensor, canceled_sensor]

# Legacy default list for single-node deployments
nexus_listeners = make_nexus_listeners()

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
            
            tags = context.run.tags if hasattr(context, "run") else {}
            logical_job_nm = tags.get("job_nm") or context.job_name or "UNKNOWN_JOB"
            
            # 🟢 Step 3.3.3: Capture high-fidelity config snapshots
            # We prioritize specific keys to avoid bloat
            pruned_tpl_vars = {}
            if isinstance(template_vars, dict):
                for k in ["vars", "run_tags", "params", "partition_key", "trigger"]:
                    if k in template_vars:
                        pruned_tpl_vars[k] = to_json_serializable(template_vars[k])

            full_config = {
                "source": to_json_serializable(source_config),
                "target": to_json_serializable(target_config),
                "template_vars": pruned_tpl_vars
            }
            
            # 🟢 Step 4.2: Extract Lineage (Upstream Parents)
            # We extract from context.assets_def which contains the full graph info
            upstreams = []
            if hasattr(context, "assets_def") and context.assets_def:
                # keys_by_input_name contains all official AssetIn dependencies
                for asset_key in context.assets_def.keys_by_input_name.values():
                    upstreams.append(asset_key.to_user_string())
            
            # Start timing
            start_time = datetime.now(timezone.utc).replace(tzinfo=None)
            
            self.provider.log_asset_start(
                run_id=run_id,
                asset_nm=asset_nm,
                config_json=full_config,
                parent_assets=upstreams,
                partition_key=context.partition_key if hasattr(context, "has_partition_key") and context.has_partition_key else None,
                strt_dttm=start_time,
                job_nm=logical_job_nm
            )
            
            try:
                result = original_execute(
                    context=context,
                    source_config=source_config,
                    target_config=target_config,
                    template_vars=template_vars,
                    **kwargs
                )
                self.provider.log_asset_end(run_id, asset_nm, status_cd='C', end_dttm=datetime.now(timezone.utc).replace(tzinfo=None))
                return result
            except Exception as e:
                self.provider.log_asset_end(run_id, asset_nm, status_cd='A', end_dttm=datetime.now(timezone.utc).replace(tzinfo=None), error_msg=str(e))
                raise e

        return tracked_execute
