from typing import Dict, Any, List, Optional
from pathlib import Path
import yaml
import os
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
        return super()._create_asset(config)

    def _get_template_vars(self, context) -> Dict[str, Any]:
        template_vars = super()._get_template_vars(context)
        if "params" not in template_vars:
            template_vars["params"] = {}
            
        try:
            run_tags = context.run.tags if hasattr(context, "run") else {}
        except Exception:
            run_tags = getattr(context, "run_tags", {})
            
        instance_id = run_tags.get("instance_id")
        job_nm = run_tags.get("job_nm") or (context.job_name if hasattr(context, "job_name") else None)
        
        # 🟢 Priority 1: UI Overrides (extracted from Run Config)
        # These are initially pre-populated with YAML defaults for UI discovery.
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

        # 🟢 Priority 2: Database Overrides (Active Audit Contract)
        # If an instance_id is present, the database is the source of truth.
        # We also support team-level overrides for Static jobs (no instance_id but has platform/type: static)
        platform_type = run_tags.get("platform/type")
        
        if instance_id or platform_type == "static":
            team_nm = run_tags.get("team")
            org_code = run_tags.get("org")
            
            provider = JobParamsProvider(self.base_dir)
            
            # 🟢 Tier 1: Hierarchical Variables (Global -> Team)
            # These populate the 'vars' object and os.environ
            db_vars = provider.get_hierarchical_vars(team_nm=team_nm, org_code=org_code)
            if db_vars:
                # Update Jinja 'vars' (Takes priority over local YAML vars)
                if "vars" not in template_vars:
                    from dagster_dag_factory.factory.helpers.dynamic import Dynamic
                    template_vars["vars"] = Dynamic({})
                
                # Merge DB vars into the Dynamic vars object
                for k, v in db_vars.items():
                    template_vars["vars"][k] = v
                    # Also inject into os.environ for connection/resource usage
                    if v is not None:
                        os.environ[k] = str(v)
                
                context.log.info(f"✅ Successfully hydrated {len(db_vars)} hierarchical variables from DB (Overriding local YAML)")

            # 🟢 Tier 2: Job Parameters (1:1 or 1:N Overrides)
            # These populate the 'params' object
            db_params = provider.get_job_params(
                job_nm=job_nm, 
                instance_id=instance_id, 
                team_nm=team_nm,
                is_static=(platform_type == "static")
            )
            
            if db_params:
                # Mask sensitive fields for logging
                masked_params = self._mask_sensitive_params(db_params)
                import json
                params_str = json.dumps(masked_params, indent=2, default=str)
                context.log.info(f"✅ Successfully loaded {len(db_params)} parameters from DB (overwriting UI defaults):\n{params_str}")
                template_vars["params"].update(db_params)
            else:
                context.log.info(f"ℹ️ No database overrides found for job='{job_nm}', instance='{instance_id}', team='{team_nm}', is_static={(platform_type == 'static')}")
        else:
            context.log.info(f"ℹ️ Skipping DB hydration: instance_id={instance_id}, platform_type={platform_type}")
            
        return template_vars
    
    def _mask_sensitive_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Mask sensitive parameter values for logging.
        Sensitive fields include: password, token, secret, key, credential, api_key, etc.
        """
        sensitive_keywords = ['password', 'passwd', 'pwd', 'token', 'secret', 'key', 'credential', 'api_key', 'auth']
        masked = {}
        
        for key, value in params.items():
            key_lower = key.lower()
            # Check if key contains any sensitive keyword
            is_sensitive = any(keyword in key_lower for keyword in sensitive_keywords)
            
            if is_sensitive and value:
                # Mask the value
                if isinstance(value, str) and len(value) > 4:
                    masked[key] = f"{value[:2]}***{value[-2:]}"
                else:
                    masked[key] = "***"
            else:
                masked[key] = value
        
        return masked

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
        
        # 🟢 Hierarchical Metadata Loading (Phase 15)
        self.repo_metadata = {}
        metadata_path = self.base_dir / "metadata.yaml"
        if metadata_path.exists():
            try:
                with open(metadata_path, 'r') as f:
                    self.repo_metadata = yaml.safe_load(f) or {}
            except Exception as e:
                print(f"⚠️ Warning: Failed to load metadata.yaml: {e}")
        
        self.team_nm = self.repo_metadata.get("team")

        self.asset_factory = ParamsAssetFactory(self.base_dir)
        self.job_factory = ParamsJobFactory(self.asset_factory)
        
        # 🟢 Code Location Resolution (Phase 15)
        # Priority: 1. metadata.yaml | 2. Constructor Argument | 3. ENV Variable
        self.location_name = (
            self.repo_metadata.get("code_location") or 
            location_name or 
            os.getenv("DAGSTER_LOCATION_NAME")
        )
        
        if observability_enabled is not None:
            self.observability_enabled = observability_enabled
        else:
            self.observability_enabled = os.getenv("NEXUS_OBSERVABILITY_ENABLED", "FALSE").upper() == "TRUE"
            
        if global_observability is not None:
            self.global_observability = global_observability
        else:
            self.global_observability = os.getenv("NEXUS_GLOBAL_OBSERVABILITY", "FALSE").upper() == "TRUE"

    def build_definitions(self) -> Definitions:
        """Injects global listeners and listeners after core construction."""
        defs = super().build_definitions()
        
        # 🟢 Observability Hub Management (Namespacing & Scalability)

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
            jobs=defs.jobs,
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
        # 🟢 Phase -1: Unified Enrichment
        self._enrich_configs(all_configs)
        
        # 🟢 Phase 0: Metadata/Registry Sync
        sync_enabled = os.getenv("NEXUS_SYNC_ENABLED", "TRUE").upper() == "TRUE"
        if sync_enabled:
            # Sync Whole YAML Definitions (Modern Registry) - Handles Schmeas automatically
            self._sync_job_definitions(all_configs)

        provider = JobParamsProvider(self.base_dir)
        try:
            active_schedules = provider.get_active_schedules(team_nm=self.team_nm)
            batch_schedules = provider.get_batch_schedules(team_nm=self.team_nm)
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
                        # No longer popping 'cron' or patching partitions.
                        # The ScheduleFactory now respects cron_schedule overrides directly.

            if "jobs" in config:
                for j_conf in config["jobs"]:
                    if j_conf.get("name") in managed_job_names:
                        # Priority 1: 1:1 Override (if it exists)
                        override = overridden_jobs.get(j_conf["name"])
                        # No longer popping 'cron' or patching partitions.

            # Suppress Legacy Triggers
            if "schedules" in config:
                config["schedules"] = [s for s in config["schedules"] if s.get("job") not in managed_job_names]
            if "sensors" in config:
                config["sensors"] = [s for s in config["sensors"] if s.get("job") not in managed_job_names]

        # 3. Inject Dynamic Schedules
        dynamic_schedules = []
        for sched in active_schedules:
            dynamic_schedules.append({
                "name": f"{sched['job_nm']}_{sched['instance_id']}_schedule",
                "cron": sched['cron_schedule'],
                "job": sched['job_nm'],
                "tags": {
                    "instance_id": sched['instance_id'], 
                    "job_nm": sched['job_nm'],
                    **self.repo_metadata
                }
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


    def _enrich_configs(self, all_configs: List[Dict[str, Any]]):
        """
        Injects platform metadata (team, org, static tags) into raw config dictionaries.
        This ensures both Dagster instantiation and DB sync are consistent.
        """
        for item in all_configs:
            config = item.get("config", {})
            if not config: continue
            
            # 1. Assets
            if "assets" in config:
                for a_conf in config["assets"]:
                    if "tags" not in a_conf: a_conf["tags"] = {}
                    
                    # Job Name for discovery
                    if "job_nm" not in a_conf["tags"]:
                        job_nm = f"{a_conf['name']}_job"
                        a_conf["tags"]["job_nm"] = job_nm
                        a_conf.setdefault("metadata", {})["job_nm"] = job_nm

                    # Platform Type
                    if "platform/type" not in a_conf["tags"]:
                        a_conf["tags"]["platform/type"] = "static"
                    
                    # Repo Metadata (Team, Org) - Skip 'description' as it's not a valid tag value
                    for k, v in self.repo_metadata.items():
                        if k != "description" and k not in a_conf["tags"]:
                            a_conf["tags"][k] = str(v)

            # 2. Explicit Jobs
            if "jobs" in config:
                for j_conf in config["jobs"]:
                    if "tags" not in j_conf: j_conf["tags"] = {}
                    
                    if "job_nm" not in j_conf["tags"]:
                        j_conf["tags"]["job_nm"] = j_conf["name"]

                    if "platform/type" not in j_conf["tags"] and "instance_id" not in j_conf["tags"]:
                        j_conf["tags"]["platform/type"] = "static"

                    # Repo Metadata (Team, Org) - Skip 'description' as it's not a valid tag value
                    for k, v in self.repo_metadata.items():
                        if k != "description" and k not in j_conf["tags"]:
                            j_conf["tags"][k] = str(v)

    def _sync_job_definitions(self, all_configs: list):
        """
        Syncs all YAML definitions to the DB for UI discovery.
        Everything is now routed to ETLJobDefinition with a blueprint_ind.
        """
        import hashlib
        provider = JobParamsProvider(self.base_dir)
        
        for item in all_configs:
            config = item["config"]
            yaml_file = item["file"]
            # Read raw content for hashing and storage
            try:
                with open(yaml_file, 'r') as f:
                    raw_content = f.read()
            except Exception as e: 
                continue
            
            content_hash = hashlib.md5(raw_content.encode()).hexdigest()
            is_blueprint = config.get("blueprint", False)
            
            # 🟢 Unified Definition Discovery
            if is_blueprint:
                # Route 1: Blueprint Discovery (Logic Templates)
                # We prioritize actual job names from the 'jobs' block to ensure Dagster alignment.
                job_names = [j.get("name") for j in config.get("jobs", []) if j.get("name")]
                if not job_names:
                    job_names = [config.get("name") or yaml_file.stem]
            else:
                # Route 2: Static Pipeline Discovery (Concrete Jobs)
                job_names = [j.get("name") for j in config.get("jobs", []) if j.get("name")]
                if not job_names and "assets" in config:
                    job_names = [f"{a['name']}_job" for a in config["assets"]]

            for job_nm in job_names:
                try:
                    # Resolve Schema Shorthand
                    params_shorthand = config.get("params_schema")
                    
                    # Check nested jobs if not found at top level (common in multi-job or blueprint files)
                    if not params_shorthand and config.get("jobs"):
                        # For blueprints or static multi-jobs, check if any job defines the schema
                        for j in config["jobs"]:
                            if j.get("name") == job_nm or is_blueprint:
                                params_shorthand = j.get("params_schema")
                                if params_shorthand: break
                    
                    params_schema = self._parse_shorthand(params_shorthand) if params_shorthand else None
                    asset_selection = [a['name'] for a in config.get("assets", [])]
                    
                    # 1. Upsert Unified Definition
                    job_def_id = provider.upsert_job_definition(
                        job_nm=job_nm,
                        yaml_def=config,
                        file_loc=str(yaml_file.relative_to(self.base_dir)),
                        file_hash=content_hash,
                        yaml_content=raw_content,
                        description=config.get("description"),
                        params_schema=params_schema,
                        asset_selection=asset_selection,
                        team_nm=self.team_nm,
                        location_nm=self.location_name,
                        blueprint_ind=is_blueprint,
                        by_nm="ParamsDagsterFactory.Sync"
                    )

                    # 2. 🟢 Explicit Schema Linking (The Holistic Fix)
                    if params_schema:
                        provider.upsert_params_schema_by_id(
                            job_definition_id=job_def_id,
                            json_schema=params_schema,
                            description=config.get("description"),
                            is_strict=config.get("is_strict", False),
                            team_nm=self.team_nm,
                            location_nm=self.location_name,
                            by_nm="ParamsDagsterFactory.Sync"
                        )

                except Exception as e:
                    print(f"⚠️ Warning: Failed to sync {job_nm}: {e}")


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
