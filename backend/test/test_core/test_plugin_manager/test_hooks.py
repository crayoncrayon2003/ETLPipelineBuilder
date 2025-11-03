import pytest
import pluggy
from unittest.mock import Mock, MagicMock
from core.plugin_manager.hooks import EtlHookSpecs, hookspec

class TestEtlHookSpecs:
    """Test class for EtlHookSpecs"""

    @pytest.fixture
    def hook_specs(self):
        """Fixture providing an EtlHookSpecs instance"""
        return EtlHookSpecs()

    def test_hookspec_marker_is_correct(self):
        """Test that the hookspec marker is correctly set"""
        # Assert
        assert hookspec.project_name == "etl_framework"

    def test_get_plugin_name_is_hookspec(self, hook_specs):
        """Test that get_plugin_name is defined as a hookspec"""
        # Assert
        assert hasattr(hook_specs, 'get_plugin_name')
        assert callable(hook_specs.get_plugin_name)

        # Check that the hookspec marker is attached
        method = getattr(EtlHookSpecs, 'get_plugin_name')
        assert hasattr(method, 'etl_framework_spec')

    def test_get_parameters_schema_is_hookspec(self, hook_specs):
        """Test that get_parameters_schema is defined as a hookspec"""
        # Assert
        assert hasattr(hook_specs, 'get_parameters_schema')
        assert callable(hook_specs.get_parameters_schema)

        # Check that the hookspec marker is attached
        method = getattr(EtlHookSpecs, 'get_parameters_schema')
        assert hasattr(method, 'etl_framework_spec')

    def test_hook_specs_methods_return_none_by_default(self, hook_specs):
        """Test that hookspec methods return None by default"""
        # Act & Assert
        # Default implementation of hookspec returns None or nothing
        result1 = hook_specs.get_plugin_name()
        result2 = hook_specs.get_parameters_schema()

        assert result1 is None
        assert result2 is None

    def test_plugin_manager_can_register_hookspecs(self):
        """Test that PluginManager can register hookspecs"""
        # Arrange
        pm = pluggy.PluginManager("etl_framework")

        # Act
        pm.add_hookspecs(EtlHookSpecs)

        # Assert
        assert pm.hook.get_plugin_name is not None
        assert pm.hook.get_parameters_schema is not None

    def test_plugin_can_implement_get_plugin_name(self):
        """Test that a plugin can implement get_plugin_name"""
        # Arrange
        pm = pluggy.PluginManager("etl_framework")
        pm.add_hookspecs(EtlHookSpecs)

        class MockPlugin:
            @pluggy.HookimplMarker("etl_framework")
            def get_plugin_name(self):
                return "test_plugin"

        plugin = MockPlugin()

        # Act
        pm.register(plugin)
        results = pm.hook.get_plugin_name()

        # Assert
        assert "test_plugin" in results

    def test_plugin_can_implement_get_parameters_schema(self):
        """Test that a plugin can implement get_parameters_schema"""
        # Arrange
        pm = pluggy.PluginManager("etl_framework")
        pm.add_hookspecs(EtlHookSpecs)

        test_schema = {
            "type": "object",
            "properties": {
                "param1": {"type": "string"}
            }
        }

        class MockPlugin:
            @pluggy.HookimplMarker("etl_framework")
            def get_parameters_schema(self):
                return test_schema

        plugin = MockPlugin()

        # Act
        pm.register(plugin)
        results = pm.hook.get_parameters_schema()

        # Assert
        assert test_schema in results

    def test_multiple_plugins_can_implement_same_hook(self):
        """Test that multiple plugins can implement the same hook"""
        # Arrange
        pm = pluggy.PluginManager("etl_framework")
        pm.add_hookspecs(EtlHookSpecs)

        class Plugin1:
            @pluggy.HookimplMarker("etl_framework")
            def get_plugin_name(self):
                return "plugin1"

        class Plugin2:
            @pluggy.HookimplMarker("etl_framework")
            def get_plugin_name(self):
                return "plugin2"

        # Act
        pm.register(Plugin1())
        pm.register(Plugin2())
        results = pm.hook.get_plugin_name()

        # Assert
        assert len(results) == 2
        assert "plugin1" in results
        assert "plugin2" in results

    def test_get_plugin_name_return_type_annotation(self, hook_specs):
        """Test that get_plugin_name has the correct return type annotation"""
        # Arrange
        import inspect

        # Act
        signature = inspect.signature(hook_specs.get_plugin_name)
        return_annotation = signature.return_annotation

        # Assert
        assert return_annotation == str

    def test_get_parameters_schema_return_type_annotation(self, hook_specs):
        """Test that get_parameters_schema has the correct return type annotation"""
        # Arrange
        import inspect
        from typing import Dict, Any

        # Act
        signature = inspect.signature(hook_specs.get_parameters_schema)
        return_annotation = signature.return_annotation

        # Assert
        assert return_annotation == Dict[str, Any]

    def test_hook_specs_has_no_required_parameters(self, hook_specs):
        """Test that hookspec methods have no required parameters"""
        # Arrange
        import inspect

        # Act
        name_params = inspect.signature(hook_specs.get_plugin_name).parameters
        schema_params = inspect.signature(hook_specs.get_parameters_schema).parameters

        # Assert
        # Exclude self and check that there are no other parameters
        assert len([p for p in name_params.values() if p.name != 'self']) == 0
        assert len([p for p in schema_params.values() if p.name != 'self']) == 0

    def test_plugin_with_different_project_name_not_called(self):
        """Test that plugins with a different project name are not called"""
        # Arrange
        pm = pluggy.PluginManager("etl_framework")
        pm.add_hookspecs(EtlHookSpecs)

        class WrongPlugin:
            # Wrong project name
            @pluggy.HookimplMarker("wrong_framework")
            def get_plugin_name(self):
                return "wrong_plugin"

        # Act
        pm.register(WrongPlugin())
        results = pm.hook.get_plugin_name()

        # Assert
        assert len(results) == 0

    def test_plugin_without_hookimpl_marker_not_called(self):
        """Test that plugins without HookimplMarker are not called"""
        # Arrange
        pm = pluggy.PluginManager("etl_framework")
        pm.add_hookspecs(EtlHookSpecs)

        class NoMarkerPlugin:
            # No HookimplMarker
            def get_plugin_name(self):
                return "no_marker_plugin"

        # Act
        pm.register(NoMarkerPlugin())
        results = pm.hook.get_plugin_name()

        # Assert
        assert len(results) == 0

    def test_etl_hook_specs_class_structure(self):
        """Test the structure of the EtlHookSpecs class"""
        # Assert
        assert hasattr(EtlHookSpecs, 'get_plugin_name')
        assert hasattr(EtlHookSpecs, 'get_parameters_schema')

        # Ensure no other methods exist (except those starting with __)
        methods = [m for m in dir(EtlHookSpecs) if not m.startswith('_')]
        assert set(methods) == {'get_plugin_name', 'get_parameters_schema'}

    def test_complex_parameters_schema_can_be_returned(self):
        """Test that a complex parameter schema can be returned"""
        # Arrange
        pm = pluggy.PluginManager("etl_framework")
        pm.add_hookspecs(EtlHookSpecs)

        complex_schema = {
            "type": "object",
            "properties": {
                "input_path": {
                    "type": "string",
                    "description": "Input file path"
                },
                "options": {
                    "type": "object",
                    "properties": {
                        "encoding": {"type": "string", "default": "utf-8"},
                        "delimiter": {"type": "string", "default": ","}
                    }
                },
                "columns": {
                    "type": "array",
                    "items": {"type": "string"}
                }
            },
            "required": ["input_path"]
        }

        class ComplexPlugin:
            @pluggy.HookimplMarker("etl_framework")
            def get_parameters_schema(self):
                return complex_schema

        # Act
        pm.register(ComplexPlugin())
        results = pm.hook.get_parameters_schema()

        # Assert
        assert complex_schema in results
        assert results[0]["properties"]["input_path"]["type"] == "string"
        assert results[0]["properties"]["options"]["properties"]["encoding"]["default"] == "utf-8"

    def test_hook_call_with_no_registered_plugins(self):
        """Test hook call when no plugins are registered"""
        # Arrange
        pm = pluggy.PluginManager("etl_framework")
        pm.add_hookspecs(EtlHookSpecs)

        # Act
        results_name = pm.hook.get_plugin_name()
        results_schema = pm.hook.get_parameters_schema()

        # Assert
        assert results_name == []
        assert results_schema == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
