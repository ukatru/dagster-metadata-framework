import functools
import logging
from typing import Any, Dict, List, Optional
from dagster import (
    DagsterRunStatus, 
    RunStatusSensorContext, 
    success_hook, 
    failure_hook,
    AssetExecutionContext
)

from metadata_framework.status_provider import NexusStatusProvider

logger = logging.getLogger("nexus.observability")

class NexusObservability:
    """
    The Pluggable Bridge for Nexus Metadata Observability.
    
    This extension automatically attaches job-level hooks and wraps asset
    execution logic to provide deep traceability in Postgres.
    """
    def __init__(self, db_url: Optional[str] = None):
        self.provider = NexusStatusProvider(db_url)

    def get_job_hooks(self) -> List[Any]:
        """Returns success and failure hooks for job-level tracking."""
        
        @success_hook
        def on_success(context):
            run_id = context.run_id
            self.provider.log_job_end(run_id, status_cd='C')

        @failure_hook
        def on_failure(context):
            run_id = context.run_id
            # TODO: Extract error from context if possible
            self.provider.log_job_end(run_id, status_cd='A')

        return [on_success, on_failure]

    def wrap_operator_execute(self, name: str, original_execute: Any):
        """
        Wraps an operator's execute method with Nexus tracking.
        
        Captures:
        - Start/End timestamps
        - Final rendered configuration (Snapshot)
        - Success/Failure status
        """
        @functools.wraps(original_execute)
        def tracked_execute(context, source_config, target_config, template_vars, **kwargs):
            run_id = context.run_id
            asset_nm = name
            
            # 1. Capture Configuration Snapshot
            # This is the 'Flight Recorder' data.
            full_config = {
                "source": source_config.model_dump() if hasattr(source_config, "model_dump") else str(source_config),
                "target": target_config.model_dump() if hasattr(target_config, "model_dump") else str(target_config),
                "template_vars": template_vars
            }
            
            # 2. Log Start
            self.provider.log_asset_start(
                run_id=run_id,
                asset_nm=asset_nm,
                config_json=full_config,
                partition_key=context.partition_key if hasattr(context, "has_partition_key") and context.has_partition_key else None
            )
            
            # 3. Execute Original Operator Logic
            try:
                result = original_execute(
                    context=context,
                    source_config=source_config,
                    target_config=target_config,
                    template_vars=template_vars,
                    **kwargs
                )
                # 4. Log Success
                self.provider.log_asset_end(run_id, asset_nm, status_cd='C')
                return result
            except Exception as e:
                # 4. Log Failure
                self.provider.log_asset_end(run_id, asset_nm, status_cd='A', error_msg=str(e))
                raise e

        return tracked_execute
