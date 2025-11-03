import pytest
from unittest.mock import patch, MagicMock
from core.plugin_manager.manager import FrameworkManager, framework_manager
from core.data_container.container import DataContainer
from core.plugin_manager.base_plugin import BasePlugin

# -----------------------------
# Mock plugin
# -----------------------------
class MockPlugin(BasePlugin):
    def __init__(self, params=None):
        self.params = params or {}
        self.executed = False

    def get_plugin_name(self) -> str:
        return "mock_plugin"

    def get_parameters_schema(self):
        # Empty schema is fine
        return {"type": "object"}

    def run(self, input_data: DataContainer, container: DataContainer) -> DataContainer:
        # Set executed flag
        container.add_history("mock_run")
        self.executed = True
        return container

    def set_params(self, params):
        self.params = params

# -----------------------------
# Test class
# -----------------------------
class TestFrameworkManager:

    @pytest.fixture
    def manager(self):
        # Provide a mock instead of the real plugin package
        with patch("core.plugin_manager.manager.plugins") as mock_plugins:
            mock_plugins.__name__ = "mock_plugins"
            mock_plugins.__path__ = []
            fm = FrameworkManager()
            # Directly add the mock plugin to the cache
            fm._plugin_name_cache["mock_plugin"] = MockPlugin()
            yield fm

    # -----------------------------
    # call_plugin_execute normal case
    # -----------------------------
    def test_call_plugin_execute_success(self, manager):
        input_container = DataContainer()
        inputs = {"input": input_container}
        params = {"param1": 123}

        result = manager.call_plugin_execute("mock_plugin", params, inputs)

        assert isinstance(result, DataContainer)
        assert "mock_plugin" in manager._plugin_name_cache
        plugin_instance = manager._plugin_name_cache["mock_plugin"]
        assert plugin_instance.params == params
        assert plugin_instance.executed is True

    # -----------------------------
    # call_plugin_execute plugin not registered
    # -----------------------------
    def test_call_plugin_execute_not_found(self, manager):
        with pytest.raises(ValueError):
            manager.call_plugin_execute("non_existent_plugin", {}, {})

    # -----------------------------
    # call_plugin_execute empty cache
    # -----------------------------
    def test_call_plugin_execute_empty_cache(self):
        fm = FrameworkManager()
        fm._plugin_name_cache = {}  # Make cache empty
        with pytest.raises(RuntimeError):
            fm.call_plugin_execute("mock_plugin", {}, {})

    # -----------------------------
    # _discover_and_instantiate_plugins internal error handling
    # -----------------------------
    def test_discover_plugin_exception_handling(self):
        class BadPlugin(BasePlugin):
            def get_plugin_name(self):
                raise RuntimeError("fail")

        with patch("core.plugin_manager.manager.pkgutil.walk_packages", return_value=[(None, "modname", False)]), \
             patch("core.plugin_manager.manager.importlib.import_module", return_value=MagicMock(BadPlugin=BadPlugin)):
            # Exceptions are caught during instantiation even if errors occur
            fm = FrameworkManager()
            # Cache remains empty
            assert fm._plugin_name_cache == {}

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
