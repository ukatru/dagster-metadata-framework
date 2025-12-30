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
    RunRequest,
    Config,
    RunConfig,
    define_asset_job,
    AssetSelection
)
from dagster_dag_factory.factory.job_factory import JobFactory
from pydantic import create_model

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
        if "params" not in template_vars:
            template_vars["params"] = {}
            
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
            template_vars["params"].update(params)
        
        # 🟢 Priority 2: UI Overrides (extracted from Run Config)
        # We read directly from the run_config dictionary to avoid needing 
        # mandatory resource requirements on every asset.
        try:
            run_config = context.run.run_config if hasattr(context, "run") else {}
            # Check for resource-based config (most common for discovery)
            ui_params = run_config.get("resources", {}).get("params", {}).get("config", {})
            if ui_params and isinstance(ui_params, dict):
                 template_vars["params"].update(ui_params)
            
            # Also check for direct 'params' key (if user used pythonic config)
            direct_params = run_config.get("params", {})
            if direct_params and isinstance(direct_params, dict):
                 template_vars["params"].update(direct_params)
        except Exception:
            pass
            
        return template_vars

    def _wrap_operator(self, operator: Any, asset_conf: Dict[str, Any]) -> Any:
        from .extensions.observability import NexusObservability
        obs = NexusObservability()
        asset_nm = asset_conf.get("name")
        operator.execute = obs.wrap_operator_execute(asset_nm, operator.execute)
        return operator

class ParamsJobFactory(JobFactory):
    def __init__(self, asset_factory: ParamsAssetFactory):
        self.asset_factory = asset_factory

    def create_jobs(self, jobs_config: List[Dict[str, Any]], asset_partitions: Dict[str, Any]):
        jobs = []
        for job_conf in jobs_config:
            name = job_conf["name"]
            selection = job_conf.get("selection", "*")
            
            # 1. Standard Selection & Partition Logic (copied from base JobFactory for now, or could call super)
            # Actually, let's just use the base logic but inject config.
            
            # Determine asset selection
            if isinstance(selection, list):
                asset_sel = None
                for item in selection:
                    new_sel = AssetSelection.from_string(item)
                    asset_sel = (asset_sel | new_sel) if asset_sel else new_sel
            elif selection == "*":
                asset_sel = AssetSelection.all()
            elif isinstance(selection, str):
                asset_sel = AssetSelection.from_string(selection)
            else:
                asset_sel = AssetSelection.all()

            if asset_sel is None:
                asset_sel = AssetSelection.all()

            # Determine partition
            partitions_def = None
            check_names = selection if isinstance(selection, list) else []
            if isinstance(selection, str) and "*" not in selection and ":" not in selection:
                check_names = [selection]
            for asset_name in check_names:
                if asset_name in asset_partitions:
                    partitions_def = asset_partitions[asset_name]
                    break

            tags = job_conf.get("tags") or {}

            # 2. 🟢 Dynamic RunConfig Generation (Developer Contract in UI)
            shorthand = job_conf.get("params_schema")
            run_config = None
            if shorthand:
                model_fields = {}
                for key, val in shorthand.items():
                    if isinstance(val, list):
                        # Enum
                        model_fields[key] = (str, val[0] if val else None)
                    elif isinstance(val, str):
                        is_required = "!" in val
                        clean_val = val.replace("!", "")
                        parts = clean_val.split("|")
                        type_str = parts[0]
                        default_val = parts[1] if len(parts) > 1 else None
                        
                        if type_str == "int": field_type = int
                        elif type_str == "float": field_type = float
                        elif type_str == "bool": field_type = bool
                        else: field_type = str
                        
                        if default_val is not None:
                            try:
                                if field_type == int: default_val = int(default_val)
                                elif field_type == float: default_val = float(default_val)
                                elif field_type == bool: default_val = default_val.lower() == "true"
                            except: pass
                            model_fields[key] = (field_type, default_val)
                        elif is_required:
                             model_fields[key] = (field_type, ...)
                        else:
                             model_fields[key] = (Optional[field_type], None)
                    else:
                        model_fields[key] = (type(val), val)
                
                if model_fields:
                    # 🟢 Create the model for UI discovery
                    # We don't actually instantiate it here to avoid build errors.
                    # We just need the defaults to pre-populate the UI.
                    defaults_dict = {}
                    for k, v in model_fields.items():
                        if isinstance(v, tuple) and v[1] is not Ellipsis:
                            defaults_dict[k] = v[1]
                    
                    if defaults_dict:
                        # This structure gives the user a pretty form in the Launchpad
                        run_config = {
                            "resources": {
                                "params": {
                                    "config": defaults_dict
                                }
                            }
                        }

            jobs.append(
                define_asset_job(
                    name=name,
                    selection=asset_sel,
                    partitions_def=partitions_def,
                    tags=tags,
                    config=run_config
                )
            )
        return jobs

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
        self.job_factory = ParamsJobFactory(self.asset_factory)
        
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

        # 🟢 Re-integrate 'params' resource for UI Discovery
        # It exists in the repository but isn't required by assets at compile time.
        from dagster import resource
        
        @resource
        def params_resource(context):
            """Framework resource for dynamic parameter overrides."""
            return context.resource_config or {}

        resources = defs.resources or {}
        resources["params"] = params_resource

        return Definitions(
            assets=defs.assets,
            jobs=processed_jobs,
            schedules=defs.schedules,
            sensors=defs.sensors + listeners,
            resources=resources,
            asset_checks=defs.asset_checks
        )

    def _apply_overrides(self, all_configs: list) -> list:
        """
        Phase 2: Robust Transformation Phase.
        Applies DB-driven overrides using whole-repo topological awareness.
        """
        # 🟢 Phase 0: Sync Params Schemas (Developer Contract)
        self._sync_params_schemas(all_configs)

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

    def _sync_params_schemas(self, all_configs: list):
        """
        Iterates through all configs and syncs job-level params_schema to the DB.
        """
        provider = JobParamsProvider(self.base_dir)
        for item in all_configs:
            config = item.get("config", {})
            if "jobs" in config:
                for j_conf in config["jobs"]:
                    job_nm = j_conf.get("name")
                    shorthand = j_conf.get("params_schema")
                    if job_nm and shorthand:
                        try:
                            schema_json = self._parse_shorthand(shorthand)
                            provider.upsert_params_schema(
                                job_nm=job_nm,
                                schema_json=schema_json,
                                description=j_conf.get("description"),
                                is_strict=j_conf.get("is_strict", False),
                                by_nm="ParamsDagsterFactory.Sync"
                            )
                        except Exception as e:
                            print(f"⚠️ Warning: Failed to sync schema for job {job_nm}: {e}")

    def _parse_shorthand(self, shorthand: Dict[str, Any]) -> Dict[str, Any]:
        """
        Converts developer-friendly shorthand to formal JSON Schema.
        e.g. "string!" -> {type: "string"}, required: ["key"]
        """
        properties = {}
        required = []
        
        for key, val in shorthand.items():
            prop = {}
            if isinstance(val, list):
                # Enum shorthand: ["A", "B"]
                prop["type"] = "string"
                prop["enum"] = val
            elif isinstance(val, str):
                # Parse "type!|default"
                is_required = "!" in val
                clean_val = val.replace("!", "")
                
                parts = clean_val.split("|")
                type_str = parts[0]
                default_val = parts[1] if len(parts) > 1 else None
                
                # Type mapping
                if type_str == "int":
                    prop["type"] = "integer"
                elif type_str == "float":
                    prop["type"] = "number"
                elif type_str == "bool":
                    prop["type"] = "boolean"
                else:
                    prop["type"] = "string"
                
                if default_val is not None:
                    # Cast default value
                    try:
                        if prop["type"] == "integer": prop["default"] = int(default_val)
                        elif prop["type"] == "number": prop["default"] = float(default_val)
                        elif prop["type"] == "boolean": prop["default"] = default_val.lower() == "true"
                        else: prop["default"] = default_val
                    except (ValueError, TypeError):
                        prop["default"] = default_val
                
                if is_required:
                    required.append(key)
            else:
                # Fallback for complex nesting if any (though not explicitly in shorthand spec)
                prop = {"type": "string", "default": str(val)}
            
            properties[key] = prop
            
        return {
            "type": "object",
            "properties": properties,
            "required": required
        }
