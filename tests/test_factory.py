from pathlib import Path
from metadata_framework import ParamsDagsterFactory
import pytest

def test_factory_initialization():
    base_dir = Path(__file__).parent.parent
    factory = ParamsDagsterFactory(base_dir)
    assert factory.base_dir == base_dir
    assert factory.asset_factory is not None

def test_factory_apply_overrides():
    """
    Tests that _apply_overrides can handle a list of configs and transform them.
    (Simplified check for structure).
    """
    base_dir = Path(__file__).parent.parent
    factory = ParamsDagsterFactory(base_dir)
    
    mock_configs = [
        {
            "config": {
                "assets": [{"name": "test_asset"}]
            },
            "file": Path("test.yaml")
        }
    ]
    
    # This calls the DB by default, so we might want to mock JobParamsProvider if we want it to be pure unit.
    # For now, we just ensure it doesn't crash if DB is unreacheable (it has a try-except).
    result = factory._apply_overrides(mock_configs)
    assert isinstance(result, list)
    assert len(result) >= 1
