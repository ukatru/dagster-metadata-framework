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
    def __init__(self, base_dir: Path, location_name: Optional[str] = None):
        super().__init__(base_dir)
        self.location_name = location_name
    
    def _create_asset(self, config: Dict[str, Any]) -> List[Any]:
        # 🟢 Asset Namespacing: Prefix with location to prevent collisions
        original_name = config['name']
        
        # Build namespaced key segments
        if self.location_name:
            # Sanitize location name (Dagster only allows alphanumeric + underscore)
            safe_location = self.location_name.replace('-', '_')
            config['_original_name'] = original_name
            config['name'] = f"{safe_location}__{original_name}"
            
            # Also namespace dependencies (ins and deps)
            if 'ins' in config:
                namespaced_ins = {}
                for alias, dep_config in config['ins'].items():
                    # The alias itself might be the asset name that needs namespacing
                    namespaced_alias = f"{safe_location}__{alias}"
                    
                    if isinstance(dep_config, dict):
                        if 'key' in dep_config:
                            # Explicit key specified - namespace it
                            original_key = dep_config['key']
                            dep_config['key'] = f"{safe_location}__{original_key}"
                        # If no key, the alias is used as the key, which we've already namespaced
                        namespaced_ins[namespaced_alias] = dep_config
                    elif isinstance(dep_config, str):
                        # Simple string reference - namespace it
                        namespaced_ins[namespaced_alias] = f"{safe_location}__{dep_config}"
                    else:
                        # Empty dict {} or other - use namespaced alias
                        namespaced_ins[namespaced_alias] = dep_config
                        
                config['ins'] = namespaced_ins
            
            if 'deps' in config:
                config['deps'] = [f"{safe_location}__{dep}" for dep in config['deps']]
        
        # 🟢 Inject logical job name into asset metadata for runtime discovery
        # This avoids hardcoded string checks in sensors/providers.
        if "metadata" not in config:
             config["metadata"] = {}
        if "tags" not in config:
             config["tags"] = {}
        
        # We assume the default job name for a standalone asset is {name}_job
        job_nm = f"{original_name}_job"
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
            
        # 🟢 Phase 18: Collapsed Architecture - job_nm is the primary identifier
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

        # 🟢 Priority 2: Database Overrides (Collapsed Architecture)
        # If job_nm is present, fetch params from etl_job_definition
        if job_nm:
            context.log.info(f"🔍 Loading parameters for Job: {job_nm}")
            team_nm = run_tags.get("team")
            
            provider = JobParamsProvider(self.base_dir)
            db_params = provider.get_job_params_by_name(job_nm, team_nm=team_nm)
            if db_params:
                # Mask sensitive fields for logging
                masked_params = self._mask_sensitive_params(db_params)
                import json
                params_str = json.dumps(masked_params, indent=2, default=str)
                context.log.info(f"✅ Successfully loaded {len(db_params)} parameters from DB (overwriting UI defaults):\\n{params_str}")
                template_vars["params"].update(db_params)
            
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
            
            # 🟢 Namespace job name with location for global uniqueness
            if hasattr(self.asset_factory, 'location_name') and self.asset_factory.location_name:
                # Sanitize location name (Dagster only allows alphanumeric + underscore)
                safe_location = self.asset_factory.location_name.replace('-', '_')
                namespaced_job_name = f"{safe_location}__{name}"
            else:
                namespaced_job_name = name
            
            # 🟢 Namespace asset selections if location_name is set
            if hasattr(self.asset_factory, 'location_name') and self.asset_factory.location_name:
                safe_location = self.asset_factory.location_name.replace('-', '_')
                if isinstance(selection, list):
                    # Namespace each asset in the list (skip special selectors)
                    namespaced_selection = []
                    for item in selection:
                        # Skip special selectors: group:, tag:, key:, *, +, etc.
                        if any(prefix in item for prefix in ['group:', 'tag:', 'key:', '*', '+']):
                            namespaced_selection.append(item)
                        elif not item.startswith(safe_location):
                            namespaced_selection.append(f"{safe_location}__{item}")
                        else:
                            namespaced_selection.append(item)
                    selection = namespaced_selection
                elif isinstance(selection, str) and selection != "*" and not any(prefix in selection for prefix in ['group:', 'tag:', 'key:', '+', '*']):
                    # Simple asset name - namespace it
                    selection = f"{safe_location}__{selection}"
            
            # 1. Standard Selection & Partition Logic
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
            
            # 🟢 Inject Team/Org Context into Job Tags
            if hasattr(self.asset_factory, "dagster_factory"): # Assume we might attach it later or refactor
                 pass # Placeholder for more decoupling
            
            # For now, let's assume we can pass the repo_metadata down
            if hasattr(self, "repo_metadata"):
                for k, v in self.repo_metadata.items():
                    if k not in tags:
                        tags[k] = v

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
                    name=namespaced_job_name,
                    description=job_conf.get("description"),
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

        # 🟢 Code Location Resolution (Phase 15)
        # Priority: 1. metadata.yaml | 2. Constructor Argument | 3. ENV Variable
        self.location_name = (
            self.repo_metadata.get("code_location") or 
            location_name or 
            os.getenv("DAGSTER_LOCATION_NAME")
        )
        
        # Sanitize for Dagster compatibility (only alphanumeric + underscore allowed)
        if self.location_name:
            self.sanitized_location_name = self.location_name.replace('-', '_')
        else:
            self.sanitized_location_name = None

        # Pass sanitized name to asset factory for namespacing
        self.asset_factory = ParamsAssetFactory(self.base_dir, location_name=self.sanitized_location_name)
        self.job_factory = ParamsJobFactory(self.asset_factory)
        
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
                
                # Injection from repo metadata (moved from JobFactory for global catch-all)
                for k, v in self.repo_metadata.items():
                    if k not in tags:
                        tags[k] = str(v)

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
        print(f"🔍 _apply_overrides called with location_name={self.location_name}")
        
        # 🟢 Phase 0: Sync Job Definitions (Unified Registry)
        self._sync_job_definitions(all_configs)

        provider = JobParamsProvider(self.base_dir)
        try:
            # Fetch ALL job definitions from DB (not just scheduled ones)
            job_definitions = provider.get_job_definitions(location_nm=self.location_name, team_nm=self.team_nm)
            print(f"🔍 Fetched {len(job_definitions)} job definitions from DB")
            
            # Also fetch scheduled jobs for dynamic schedule creation
            active_schedules = provider.get_active_schedules(location_nm=self.location_name, team_nm=self.team_nm)
            print(f"🔍 Fetched {len(active_schedules)} active_schedules")
        except Exception as e:
            print(f"⚠️ Warning: Failed to fetch job definitions: {e}")
            job_definitions = []
            active_schedules = []

        # 🟢 Namespace sensor and schedule job references (MUST run before early return!)
        if self.sanitized_location_name:
            safe_location = self.sanitized_location_name
            sensor_count = sum(1 for item in all_configs if "sensors" in item.get("config", {}))
            print(f"🔍 Found {sensor_count} configs with sensors, location_name={self.location_name}")
            
            for item in all_configs:
                config = item.get("config", {})
                
                # Update sensors
                if "sensors" in config:
                    print(f"🔍 Processing {len(config['sensors'])} sensors from {item.get('file', 'unknown')}")
                    for sensor in config["sensors"]:
                        if "job" in sensor:
                            original_job = sensor["job"]
                            namespaced_job = f"{safe_location}__{original_job}"
                            sensor["job"] = namespaced_job
                            print(f"🔧 Namespaced sensor '{sensor.get('name')}' job: {original_job} → {namespaced_job}")
                
                # Update schedules (non-dynamic ones from YAML)
                if "schedules" in config:
                    for schedule in config["schedules"]:
                        if "job" in schedule and not schedule.get("jobs"):  # Skip batch schedules
                            original_job = schedule["job"]
                            # Don't re-namespace if already namespaced
                            if "__" not in original_job or not original_job.startswith(safe_location):
                                namespaced_job = f"{safe_location}__{original_job}"
                                schedule["job"] = namespaced_job
                                print(f"🔧 Namespaced schedule '{schedule.get('name')}' job: {original_job} → {namespaced_job}")

        if not active_schedules:
            print(f"🔍 _apply_overrides completed (no DB schedules), returning {len(all_configs)} configs")
            return all_configs

        # Build a mapping of jobs that are overridden by the DB (singletons or instances)
        overridden_job_names = set()
        for s in active_schedules:
            overridden_job_names.add(s['job_nm'])
            if s.get('template_job_nm'):
                overridden_job_names.add(s['template_job_nm'])

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
                    # Check if any job targeting this asset is overridden in DB
                    related_jobs = asset_to_jobs.get(a_nm, set()) | jobs_targeting_all
                    if related_jobs & overridden_job_names:
                        # If the asset has a local 'cron', remove it (DB handles it)
                        a_conf.pop("cron", None)
                        # Note: Partition patching for instances is handled via Jinja/Params at runtime,
                        # but we can still patch the definition if it's a singleton.
                        # (Omitted legacy partition patching here to keep logic clean)

            if "jobs" in config:
                for j_conf in config["jobs"]:
                    if j_conf.get("name") in overridden_job_names:
                        j_conf.pop("cron", None)

            # Suppress Legacy Triggers (Schedules/Sensors in YAML that point to overridden jobs)
            if "schedules" in config:
                config["schedules"] = [s for s in config["schedules"] if s.get("job") not in overridden_job_names]
            if "sensors" in config:
                config["sensors"] = [s for s in config["sensors"] if s.get("job") not in overridden_job_names]

        # 3. Inject Dynamic Schedules
        dynamic_schedules = []
        safe_location = self.sanitized_location_name or ""
        
        for sched in active_schedules:
            if sched.get("type") == "singleton":
                # Regular singleton - job_nm is already namespaced in DB sync
                target_job = sched['job_nm']
            else:
                # Blueprint instance - job_nm is user's instance name, needs to trigger template job
                # template_job_nm is the logical name from YAML, needs namespacing for Dagster
                target_job = f"{safe_location}__{sched['template_job_nm']}" if safe_location else sched['template_job_nm']
                
            dynamic_schedules.append({
                "name": f"{sched['job_nm']}_{sched['instance_id']}_schedule",
                "job": target_job,
                "cron": sched['cron_schedule'],
                "tags": {
                    "instance_id": sched['instance_id'], 
                    "job_nm": sched['job_nm'], # THIS is the Instance ID for param lookup
                    **self.repo_metadata
                }
            })

        if dynamic_schedules:
            all_configs.append({
                "config": {"schedules": dynamic_schedules},
                "file": Path("METADATA_DYNAMIC_OVERRIDES.yaml")
            })

        print(f"🔍 _apply_overrides completed, returning {len(all_configs)} configs")
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

    def _sync_job_definitions(self, all_configs: list):
        """
        Phase 18: Collapsed Registry Sync.
        Parses YAML and routes to either Template or Definition table.
        """
        provider = JobParamsProvider(self.base_dir)
        for item in all_configs:
            config = item.get("config", {})
            # Check for Template Flag at root
            is_template = config.get("template", False)
            
            # Build a map of job_name -> cron_schedule from the schedules section
            schedule_map = {}
            if "schedules" in config:
                for sched in config["schedules"]:
                    job_name = sched.get("job")
                    cron = sched.get("cron")
                    if job_name and cron:
                        schedule_map[job_name] = cron
            
            if "jobs" in config:
                for j_conf in config["jobs"]:
                    job_nm = j_conf.get("name")
                    if not job_nm: continue
                    
                    try:
                        # Extract schema if shorthand exists
                        shorthand = j_conf.get("params_schema")
                        json_schema = self._parse_shorthand(shorthand) if shorthand else {}
                        
                        # Extract asset selection
                        selection = j_conf.get("selection", "*")
                        asset_list = selection if isinstance(selection, list) else [selection]
                        
                        # Get cron from schedule map
                        cron_schedule = schedule_map.get(job_nm)
                        
                        # 🟢 Namespace job name to match runtime job names
                        if self.sanitized_location_name:
                            namespaced_job_nm = f"{self.sanitized_location_name}__{job_nm}"
                        else:
                            namespaced_job_nm = job_nm

                        if is_template:
                            # 🟢 Route to Template Table (Blueprints)
                            provider.upsert_job_template(
                                template_nm=job_nm,  # Templates use original name
                                yaml_def=config,
                                params_schema=json_schema,
                                asset_selection=asset_list,
                                description=j_conf.get("description"),
                                team_nm=self.team_nm,
                                location_nm=self.location_name,  # Use original for DB lookup
                                by_nm="ParamsDagsterFactory.Sync"
                            )
                        else:
                            # 🟢 Route to Definition Table (Executable Singletons)
                            provider.upsert_job_definition(
                                job_nm=namespaced_job_nm,  # Use namespaced name
                                yaml_def=config,
                                params_schema=json_schema,
                                asset_selection=asset_list,
                                description=j_conf.get("description"),
                                cron_schedule=cron_schedule,  # Pass the extracted cron
                                team_nm=self.team_nm,
                                location_nm=self.location_name,
                                is_singleton=True,
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
