from typing import Dict, Any, List, Optional
from pathlib import Path
from dagster_dag_factory.factory.asset_factory import AssetFactory
from dagster_dag_factory.factory.dagster_factory import DagsterFactory
from .params_provider import JobParamsProvider
from dagster import (
    Definitions, 
    ScheduleDefinition, 
    AssetsDefinition,
    AssetChecksDefinition,
    JobDefinition,
    SensorDefinition,
    RunRequest
)

class ParamsAssetFactory(AssetFactory):
    def _create_asset(self, config: Dict[str, Any]) -> List[Any]:
        # 🟢 Inject logical job name into asset metadata for runtime discovery
        # This avoids hardcoded string checks in sensors/providers.
        if "metadata" not in config:
             config["metadata"] = {}
        if "tags" not in config:
             config["tags"] = {}
        
        # We assume the default job name for a standalone asset is {name}_job
        job_nm = f"{config['name']}_job"
        if "job_nm" not in config["metadata"]:
             config["metadata"]["job_nm"] = job_nm
        if "job_nm" not in config["tags"]:
             config["tags"]["job_nm"] = job_nm
             
        return super()._create_asset(config)

    def _get_template_vars(self, context) -> Dict[str, Any]:
        template_vars = super()._get_template_vars(context)
        try:
            run_tags = context.run.tags if hasattr(context, "run") else {}
        except Exception:
            run_tags = getattr(context, "run_tags", {})
            
        invok_id = run_tags.get("invok_id")
        job_nm = run_tags.get("job_nm") or (context.job_name if hasattr(context, "job_name") else None)
        
        if invok_id:
            context.log.info(f"🔍 Loading parameters for Invok: {invok_id} (Job: {job_nm})")
            provider = JobParamsProvider(self.base_dir)
            params = provider.get_job_params(job_nm, invok_id)
            if params:
                context.log.info(f"✅ Successfully loaded {len(params)} parameters into run context.")
            template_vars["params"] = params
        
        return template_vars

    def _wrap_operator(self, operator: Any, asset_conf: Dict[str, Any]) -> Any:
        from .extensions.observability import NexusObservability
        obs = NexusObservability()
        asset_nm = asset_conf.get("name")
        operator.execute = obs.wrap_operator_execute(asset_nm, operator.execute)
        return operator

class ParamsDagsterFactory(DagsterFactory):
    def __init__(
        self, 
        base_dir: Path, 
        location_name: Optional[str] = None, 
        observability_enabled: Optional[bool] = None, 
        global_observability: Optional[bool] = None, 
        **kwargs
    ):
        super().__init__(base_dir, **kwargs)
        import os
        
        self.asset_factory = ParamsAssetFactory(self.base_dir)
        
        # 🟢 Priority 1: Constructor Argument | Priority 2: ENV Variable | Priority 3: Default
        self.location_name = location_name or os.getenv("DAGSTER_LOCATION_NAME")
        
        if observability_enabled is not None:
            self.observability_enabled = observability_enabled
        else:
            self.observability_enabled = os.getenv("NEXUS_OBSERVABILITY_ENABLED", "FALSE").upper() == "TRUE"
            
        if global_observability is not None:
            self.global_observability = global_observability
        else:
            self.global_observability = os.getenv("NEXUS_GLOBAL_OBSERVABILITY", "FALSE").upper() == "TRUE"

    def build_definitions(self) -> Definitions:
        """Injects global listeners and observability tags after core construction."""
        defs = super().build_definitions()
        
        # 🟢 Observability Tag Management (Framework Layer)
        processed_jobs = []
        for job in defs.jobs:
            if isinstance(job, JobDefinition):
                tags = job.tags or {}
                if "job_nm" not in tags:
                    tags = {**tags, "job_nm": job.name}
                processed_jobs.append(job.with_tags(tags))
            else:
                processed_jobs.append(job)

        # 🟢 Observability Hub Management (Namespacing & Scalability)
        listeners = []
        if self.observability_enabled:
            from .extensions.observability import make_nexus_listeners
            listeners = make_nexus_listeners(
                location_name=self.location_name,
                global_monitor=self.global_observability
            )

        return Definitions(
            assets=defs.assets,
            jobs=processed_jobs,
            schedules=defs.schedules,
            sensors=defs.sensors + listeners,
            resources=defs.resources,
            asset_checks=defs.asset_checks
        )

    def _apply_overrides(self, all_configs: list) -> list:
        """
        Phase 2: Robust Transformation Phase.
        Applies DB-driven overrides using whole-repo topological awareness.
        """
        provider = JobParamsProvider(self.base_dir)
        try:
            active_schedules = provider.get_active_schedules()
            batch_schedules = provider.get_batch_schedules()
        except Exception as e:
            print(f"⚠️ Warning: Failed to fetch dynamic schedules: {e}")
            active_schedules = []
            batch_schedules = []

        if not active_schedules and not batch_schedules:
            return all_configs

        overridden_jobs = {s['job_nm']: s for s in active_schedules}
        
        # Consolidate ALL jobs that are managed by the DB (either 1:1 or Batch)
        managed_job_names = set(overridden_jobs.keys())
        for batch in batch_schedules:
            for job in batch["jobs"]:
                managed_job_names.add(job["name"])

        # 1. Build Whole-Repo Topology (Asset -> Jobs)
        asset_to_jobs = {}
        jobs_targeting_all = set()
        
        for item in all_configs:
            config = item["config"]
            if "assets" in config:
                for a_conf in config["assets"]:
                    a_nm = a_conf.get("name")
                    if a_nm:
                        asset_to_jobs.setdefault(a_nm, set()).update({a_nm, f"{a_nm}_job"})
            
            if "jobs" in config:
                for j_conf in config["jobs"]:
                    j_nm = j_conf.get("name")
                    if not j_nm: continue
                    selection = j_conf.get("selection", [])
                    if selection == "*":
                        jobs_targeting_all.add(j_nm)
                    elif isinstance(selection, list):
                        for sel in selection:
                            if isinstance(sel, str): asset_to_jobs.setdefault(sel, set()).add(j_nm)
                    elif isinstance(selection, str):
                        asset_to_jobs.setdefault(selection, set()).add(j_nm)

        # 2. Apply Transformations
        for item in all_configs:
            config = item["config"]
            
            if "assets" in config:
                for a_conf in config["assets"]:
                    a_nm = a_conf.get("name")
                    matched = (asset_to_jobs.get(a_nm, set()) | jobs_targeting_all) & managed_job_names
                    if matched:
                        override = overridden_jobs[sorted(list(matched))[0]]
                        a_conf.pop("cron", None)
                        if "partitions_def" in a_conf:
                            self._patch_partitions(a_conf["partitions_def"], override)

            if "jobs" in config:
                for j_conf in config["jobs"]:
                    if j_conf.get("name") in managed_job_names:
                        # Priority 1: 1:1 Override (if it exists)
                        override = overridden_jobs.get(j_conf["name"])
                        j_conf.pop("cron", None)
                        if override and "partitions_def" in j_conf:
                            self._patch_partitions(j_conf["partitions_def"], override)

            # Suppress Legacy Triggers
            if "schedules" in config:
                config["schedules"] = [s for s in config["schedules"] if s.get("job") not in managed_job_names]
            if "sensors" in config:
                config["sensors"] = [s for s in config["sensors"] if s.get("job") not in managed_job_names]

        # 3. Inject Dynamic Schedules
        dynamic_schedules = []
        for sched in active_schedules:
            dynamic_schedules.append({
                "name": f"{sched['job_nm']}_{sched['invok_id']}_schedule",
                "job": sched['job_nm'],
                "cron": sched['cron_schedule'],
                "tags": {"invok_id": sched['invok_id'], "job_nm": sched['job_nm']}
            })

        # Priority 2: Batch Schedules (Fan-Out)
        for batch in batch_schedules:
            dynamic_schedules.append({
                "name": batch["name"],
                "cron": batch["cron"],
                "timezone": batch["timezone"],
                "jobs": batch["jobs"] # 🚀 Using the new Core Lib Fan-Out Engine!
            })

        if dynamic_schedules:
            all_configs.append({
                "config": {"schedules": dynamic_schedules},
                "file": Path("METADATA_DYNAMIC_OVERRIDES.yaml")
            })

        return all_configs

    def _patch_partitions(self, p_conf: dict, override: dict):
        cron_str = override.get("cron_schedule")
        start_date = override.get("partition_start_dt")
        
        if start_date:
            p_conf["start_date"] = start_date.isoformat()
            
        if cron_str:
            parts = cron_str.split()
            if len(parts) >= 2:
                try:
                    p_conf["minute_offset"] = int(parts[0])
                    p_conf["hour_offset"] = int(parts[1])
                except (ValueError, IndexError): pass
            if p_conf.get("type") == "cron":
                p_conf["cron_schedule"] = cron_str
