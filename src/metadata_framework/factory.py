from typing import Dict, Any
from pathlib import Path
from dagster_dag_factory.factory.asset_factory import AssetFactory
from dagster_dag_factory.factory.dagster_factory import DagsterFactory
from .metadata_provider import JobParamsProvider

class ParamsAssetFactory(AssetFactory):
    def _get_template_vars(self, context) -> Dict[str, Any]:
        # 🟢 Get base vars (partitions, env, etc) from core library
        template_vars = super()._get_template_vars(context)
        
        # Extract context info
        run_tags = context.run.tags if hasattr(context, "run") else {}
        invok_id = run_tags.get("invok_id")
        
        # 🟢 Smart Job Name Resolution
        # Use 'job_nm' from tags first (most reliable for manual/scheduled runs)
        # Fall back to context.job_name
        job_nm = run_tags.get("job_nm") or (context.job_name if hasattr(context, "job_name") else None)
        
        # When materializing from the Asset UI, Dagster uses '__ASSET_JOB'.
        # If we didn't find a 'job_nm' tag, we will have to use the invok_id lookup.
        
        if invok_id:
            context.log.info(f"🔍 Loading parameters for Invok: {invok_id} (Job: {job_nm})")
            provider = JobParamsProvider(self.base_dir)
            params = provider.get_job_params(job_nm, invok_id)
            
            if not params:
                context.log.error(f"❌ No parameters found in DB for job={job_nm}, invok={invok_id}.")
            else:
                context.log.info(f"✅ Successfully loaded {len(params)} parameters into run context.")
            
            template_vars["params"] = params
        
        return template_vars

    def _wrap_operator(self, operator: Any, asset_conf: Dict[str, Any]) -> Any:
        """
        Injects Nexus Observability into the operator execution.
        Captures asset-level snapshots and configs.
        """
        from .extensions.observability import NexusObservability
        obs = NexusObservability()
        
        asset_nm = asset_conf.get("name")
        operator.execute = obs.wrap_operator_execute(asset_nm, operator.execute)
        return operator

from dagster import (
    AssetExecutionContext, 
    Definitions, 
    ScheduleDefinition, 
    RunRequest,
    build_schedule_from_partitioned_job
)

class ParamsDagsterFactory(DagsterFactory):
    def __init__(self, base_dir: Path, **kwargs):
        super().__init__(base_dir, **kwargs)
        # Swap the standard AssetFactory with our Params-aware version
        self.asset_factory = ParamsAssetFactory(self.base_dir)

    def build_definitions(self) -> Definitions:
        # 1. Build standard definitions from YAML
        defs = super().build_definitions()
        
        # 2. Fetch Dynamic Schedules
        provider = JobParamsProvider(self.base_dir)
        try:
            params_schedules = provider.get_active_schedules()
        except Exception as e:
            print(f"⚠️ Warning: Failed to fetch dynamic schedules: {e}")
            params_schedules = []
        
        jobs_map = {job.name: job for job in defs.jobs}
        
        dynamic_schedules = []
        for sched in params_schedules:
            job_nm = sched['job_nm']
            invok_id = sched['invok_id']
            cron = sched['cron_schedule']
            
            if job_nm in jobs_map:
                job = jobs_map[job_nm]
                sched_name = f"{job_nm}_{invok_id}_schedule"
                tags = {"invok_id": invok_id, "job_nm": job_nm}
                
                # 🟢 Check if this job is partitioned (e.g., Daily/Weekly)
                # If so, we MUST use build_schedule_from_partitioned_job to avoid the partition_key error
                is_partitioned = hasattr(job, "partitions_def") and job.partitions_def is not None
                
                if is_partitioned:
                    # Dagster's partitioned schedule builder
                    # For time-window partitions, it handles the Cron/RunRequest/Tags internally
                    dynamic_sched = build_schedule_from_partitioned_job(
                        name=sched_name,
                        job=job,
                        tags=tags
                    )
                else:
                    # Standard non-partitioned schedule
                    dynamic_sched = ScheduleDefinition(
                        name=sched_name,
                        job=job,
                        cron_schedule=cron,
                        tags=tags
                    )
                dynamic_schedules.append(dynamic_sched)
            else:
                print(f"⚠️ Warning: Dynamic schedule found for unknown job '{job_nm}'. Skipping.")

        # 3. Inject Global Listeners
        from .extensions.observability import nexus_listeners
        
        # 4. Merge results
        return Definitions.merge(
            defs, 
            Definitions(
                schedules=dynamic_schedules,
                sensors=nexus_listeners
            )
        )
