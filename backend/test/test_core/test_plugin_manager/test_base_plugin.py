import pytest
from unittest.mock import MagicMock
from core.data_container.container import DataContainer, DataContainerStatus
from scripts.core.plugin_manager.base_plugin import BasePlugin

class TestBasePlugin:

    # ----------------------------------------
    # Test subclass
    # ----------------------------------------
    @pytest.fixture
    def mock_plugin_class(self):
        class MockPlugin(BasePlugin):
            def run(self, input_data: DataContainer, container: DataContainer) -> DataContainer:
                container.add_history("mock_run")
                return container

            def get_plugin_name(self) -> str:
                return "MockPlugin"

            def get_parameters_schema(self):
                return {"type": "object"}
        return MockPlugin

    @pytest.fixture
    def exception_plugin_class(self):
        class ExceptionPlugin(BasePlugin):
            def run(self, input_data, container):
                raise RuntimeError("fail")

            def get_plugin_name(self):
                return "ExceptionPlugin"

            def get_parameters_schema(self):
                return {}
        return ExceptionPlugin

    @pytest.fixture
    def data_container(self):
        return DataContainer()

    # ----------------------------------------
    # execute success
    # ----------------------------------------
    def test_execute_success(self, mock_plugin_class, data_container):
        plugin = mock_plugin_class(params={})
        result = plugin.execute(data_container)
        result = plugin.finalize_container(result)  # Set status to SUCCESS

        assert isinstance(result, DataContainer)
        assert result.status == DataContainerStatus.SUCCESS
        assert "MockPlugin" in result.history

    # ----------------------------------------
    # execute exception
    # ----------------------------------------
    def test_execute_exception(self, exception_plugin_class, data_container):
        plugin = exception_plugin_class(params={})
        result = plugin.execute(data_container)

        assert isinstance(result, DataContainer)
        assert result.status == DataContainerStatus.ERROR
        assert any("fail" in e for e in result.errors)

    # ----------------------------------------
    # finalize_container test
    # ----------------------------------------
    def test_finalize_container(self, mock_plugin_class, data_container):
        plugin = mock_plugin_class(params={})
        result = plugin.finalize_container(
            data_container,
            output_path="/tmp/output.csv",
            metadata={"key": "value"}
        )

        assert result.status == DataContainerStatus.SUCCESS
        assert "/tmp/output.csv" in result.file_paths
        assert result.metadata.get("key") == "value"

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
