from pathlib import Path
from dagster import build_asset_context, DagsterInstance, AssetKey
from metadata_framework import ParamsDagsterFactory
from metadata_framework.params_provider import JobParamsProvider
import pytest
import os

def test_params_hydration():
    """
    Tests that params are correctly loaded from the provider and available in template_vars.
    """
    # Setup
    base_dir = Path(__file__).parent.parent
    factory = ParamsDagsterFactory(base_dir)
    
    # Mock tags that would come from a dynamic schedule or manual launch
    tags = {
        "invok_id": "TEST_001",
        "job_nm": "cross_ref_test_job"
    }
    
    with build_asset_context(instance=DagsterInstance.ephemeral(), run_tags=tags) as context:
        template_vars = factory.asset_factory._get_template_vars(context)
        
        assert "params" in template_vars
        # We don't assert on specific DB values here to avoid dependency on DB state in unit test,
        # but we check the key exists. 
        # For a full integration test, we would check specific values.

def test_jinja_rendering_with_params():
    """
    Tests that Jinja2 templates can access the 'params' key.
    """
    from dagster_dag_factory.factory.helpers.rendering import render_config
    
    template_vars = {
        "params": {
            "source_path": "/tmp/data",
            "job_id": 123
        }
    }
    
    config = {
        "path": "{{ params.source_path }}",
        "target": "output/{{ params.job_id }}"
    }
    
    rendered = render_config(config, template_vars)
    
    assert rendered["path"] == "/tmp/data"
    assert rendered["target"] == "output/123"
