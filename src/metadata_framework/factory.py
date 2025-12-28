from typing import Dict, Any
from pathlib import Path
from dagster_dag_factory.factory.asset_factory import AssetFactory
from dagster_dag_factory.factory.dagster_factory import DagsterFactory
from .metadata_provider import MetadataProvider

class MetadataAssetFactory(AssetFactory):
    def _get_template_vars(self, context) -> Dict[str, Any]:
        # 1. Get base vars (partitions, env, etc) from core library
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
        is_generic_job = job_nm == "__ASSET_JOB" or not job_nm

        if invok_id:
            source = "tags" if "job_nm" in run_tags else "context"
            context.log.info(f"🔍 Hydrating metadata for Invok: {invok_id} (Job: {job_nm} from {source})")
            provider = MetadataProvider(self.base_dir)
            
            # If job_nm is still generic, the provider can try a fuzzy lookup or invok_id only
            metadata = provider.get_job_metadata(job_nm, invok_id)
            
            if not metadata:
                context.log.error(f"❌ No metadata found in DB for job={job_nm}, invok={invok_id}. Check your DB seeding!")
            else:
                context.log.info(f"✅ Hydrated {len(metadata)} metadata parameters: {list(metadata.keys())}")
            
            # 3. Inject into Jinja scope as 'metadata'
            template_vars["metadata"] = metadata
        else:
            if not invok_id:
                context.log.warning("⚠️ No 'invok_id' found in run tags. Skipping metadata hydration.")
            
        return template_vars

from dagster import (
    AssetExecutionContext, 
    Definitions, 
    ScheduleDefinition, 
    RunRequest,
    build_schedule_from_partitioned_job
)

class MetadataDagsterFactory(DagsterFactory):
    def __init__(self, base_dir: Path, **kwargs):
        super().__init__(base_dir, **kwargs)
        # Swap the standard AssetFactory with our Metadata-aware version
        # This is where the 'magic' happens without touching core lib
        self.asset_factory = MetadataAssetFactory(self.base_dir)

    def build_definitions(self) -> Definitions:
        # 1. Build standard definitions from YAML (Assets, Jobs, static Schedules)
        defs = super().build_definitions()
        
        # 2. Fetch Dynamic Schedules from Postgres
        provider = MetadataProvider(self.base_dir)
        try:
            metadata_schedules = provider.get_active_schedules()
        except Exception as e:
            print(f"⚠️ Warning: Failed to fetch dynamic schedules from metadata DB: {e}")
            return defs

        if not metadata_schedules:
            return defs

        # 3. Create Jobs map for schedule association
        jobs_map = {job.name: job for job in defs.jobs}
        
        dynamic_schedules = []
        for sched in metadata_schedules:
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

        # 4. Merge results
        if dynamic_schedules:
            return Definitions.merge(defs, Definitions(schedules=dynamic_schedules))
        
        return defs
