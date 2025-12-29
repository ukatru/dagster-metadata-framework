from metadata_framework.extensions.observability import NexusObservability
from metadata_framework.factory import ParamsDagsterFactory
from dagster import build_asset_context, DagsterInstance, AssetKey
import pytest
from pathlib import Path

def test_observability_wrapping():
    """
    Tests that the factory correctly wraps operators with observability.
    """
    base_dir = Path(__file__).parent.parent
    factory = ParamsDagsterFactory(base_dir)
    
    asset_conf = {"name": "obs_test_asset"}
    
    class MockOperator:
        def execute(self, **kwargs):
            return {"status": "success"}
            
    op = MockOperator()
    wrapped_op = factory.asset_factory._wrap_operator(op, asset_conf)
    
    assert hasattr(wrapped_op.execute, "__wrapped__")
    assert wrapped_op.execute.__wrapped__.__name__ == "execute"

@pytest.mark.skip(reason="Requires active Postgres database")
def test_observability_logging():
    # This would be a full integration test
    pass
