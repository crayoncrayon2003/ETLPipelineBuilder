import pytest
from unittest.mock import Mock, patch, MagicMock
from fastapi.testclient import TestClient
from fastapi import FastAPI
from api.routers.plugins import router, _get_plugin_type


class TestGetPluginType:
    """Test class for the _get_plugin_type function"""

    def test_get_plugin_type_extractor(self):
        """Test for identifying Extractor type"""
        # Arrange
        mock_plugin = Mock()
        mock_plugin.__class__.__module__ = "plugins.extractors.csv_reader"

        # Act
        result = _get_plugin_type(mock_plugin)

        # Assert
        assert result == "extractor"

    def test_get_plugin_type_cleanser(self):
        """Test for identifying Cleanser type"""
        # Arrange
        mock_plugin = Mock()
        mock_plugin.__class__.__module__ = "plugins.cleansing.duplicate_remover"

        # Act
        result = _get_plugin_type(mock_plugin)

        # Assert
        assert result == "cleanser"

    def test_get_plugin_type_transformer(self):
        """Test for identifying Transformer type"""
        # Arrange
        mock_plugin = Mock()
        mock_plugin.__class__.__module__ = "plugins.transformers.normalizer"

        # Act
        result = _get_plugin_type(mock_plugin)

        # Assert
        assert result == "transformer"

    def test_get_plugin_type_validator(self):
        """Test for identifying Validator type"""
        # Arrange
        mock_plugin = Mock()
        mock_plugin.__class__.__module__ = "plugins.validators.quality_checker"

        # Act
        result = _get_plugin_type(mock_plugin)

        # Assert
        assert result == "validator"

    def test_get_plugin_type_loader(self):
        """Test for identifying Loader type"""
        # Arrange
        mock_plugin = Mock()
        mock_plugin.__class__.__module__ = "plugins.loaders.database_writer"

        # Act
        result = _get_plugin_type(mock_plugin)

        # Assert
        assert result == "loader"

    def test_get_plugin_type_unknown(self):
        """Test for identifying Unknown type"""
        # Arrange
        mock_plugin = Mock()
        mock_plugin.__class__.__module__ = "plugins.custom.my_plugin"

        # Act
        result = _get_plugin_type(mock_plugin)

        # Assert
        assert result == "unknown"

    def test_get_plugin_type_with_nested_module(self):
        """Test for a nested module path"""
        # Arrange
        mock_plugin = Mock()
        mock_plugin.__class__.__module__ = "my_app.plugins.extractors.advanced.csv_reader"

        # Act
        result = _get_plugin_type(mock_plugin)

        # Assert
        assert result == "extractor"


class TestPluginsRouter:
    """Test class for the plugins router"""

    @pytest.fixture
    def client(self):
        """Fixture providing a FastAPI test client"""
        app = FastAPI()
        app.include_router(router)
        return TestClient(app)

    @pytest.fixture
    def mock_framework_manager(self):
        """Fixture mocking the framework_manager"""
        with patch('api.routers.plugins.framework_manager') as mock_manager:
            yield mock_manager

    def test_get_available_plugins_empty(self, client, mock_framework_manager):
        """Test when no plugins are available"""
        # Arrange
        mock_framework_manager._plugin_name_cache = {}

        # Act
        response = client.get("/plugins/")

        # Assert
        assert response.status_code == 200
        assert response.json() == []

    def test_get_available_plugins_single_plugin(self, client, mock_framework_manager):
        """Test when a single plugin is available"""
        # Arrange
        mock_plugin = Mock()
        mock_plugin.__class__.__module__ = "plugins.extractors.test"
        mock_plugin.__doc__ = "Test plugin description"
        mock_plugin.get_parameters_schema.return_value = {
            "type": "object",
            "properties": {"path": {"type": "string"}}
        }

        mock_framework_manager._plugin_name_cache = {
            "test_plugin": mock_plugin
        }

        # Act
        response = client.get("/plugins/")

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["name"] == "test_plugin"
        assert data[0]["type"] == "extractor"
        assert data[0]["description"] == "Test plugin description"
        assert data[0]["parameters_schema"]["type"] == "object"

    def test_get_available_plugins_multiple_plugins(self, client, mock_framework_manager):
        """Test when multiple plugins are available"""
        # Arrange
        mock_plugin1 = Mock()
        mock_plugin1.__class__.__module__ = "plugins.extractors.csv"
        mock_plugin1.__doc__ = "CSV extractor"
        mock_plugin1.get_parameters_schema.return_value = {}

        mock_plugin2 = Mock()
        mock_plugin2.__class__.__module__ = "plugins.loaders.db"
        mock_plugin2.__doc__ = "Database loader"
        mock_plugin2.get_parameters_schema.return_value = {}

        mock_plugin3 = Mock()
        mock_plugin3.__class__.__module__ = "plugins.transformers.normalize"
        mock_plugin3.__doc__ = "Data normalizer"
        mock_plugin3.get_parameters_schema.return_value = {}

        mock_framework_manager._plugin_name_cache = {
            "csv_extractor": mock_plugin1,
            "db_loader": mock_plugin2,
            "normalizer": mock_plugin3
        }

        # Act
        response = client.get("/plugins/")

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 3

        # Ensure the names are sorted
        names = [p["name"] for p in data]
        assert names == sorted(names)

    def test_get_available_plugins_sorted_by_name(self, client, mock_framework_manager):
        """Test that plugins are sorted by name"""
        # Arrange
        mock_plugin_z = Mock()
        mock_plugin_z.__class__.__module__ = "plugins.extractors.z"
        mock_plugin_z.__doc__ = "Z plugin"
        mock_plugin_z.get_parameters_schema.return_value = {}

        mock_plugin_a = Mock()
        mock_plugin_a.__class__.__module__ = "plugins.extractors.a"
        mock_plugin_a.__doc__ = "A plugin"
        mock_plugin_a.get_parameters_schema.return_value = {}

        mock_framework_manager._plugin_name_cache = {
            "z_plugin": mock_plugin_z,
            "a_plugin": mock_plugin_a
        }

        # Act
        response = client.get("/plugins/")

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data[0]["name"] == "a_plugin"
        assert data[1]["name"] == "z_plugin"

    def test_get_available_plugins_without_schema_method(self, client, mock_framework_manager):
        """Test plugins without get_parameters_schema method"""
        # Arrange
        mock_plugin = Mock(spec=[])  # No methods
        mock_plugin.__class__.__module__ = "plugins.extractors.test"
        mock_plugin.__doc__ = "Test plugin"

        mock_framework_manager._plugin_name_cache = {
            "test_plugin": mock_plugin
        }

        # Act
        response = client.get("/plugins/")

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["parameters_schema"] == {}

    def test_get_available_plugins_schema_method_raises_exception(self, client, mock_framework_manager):
        """Test when get_parameters_schema method raises an exception"""
        # Arrange
        mock_plugin = Mock()
        mock_plugin.__class__.__module__ = "plugins.extractors.test"
        mock_plugin.__doc__ = "Test plugin"
        mock_plugin.get_parameters_schema.side_effect = Exception("Schema error")

        mock_framework_manager._plugin_name_cache = {
            "test_plugin": mock_plugin
        }

        # Act
        response = client.get("/plugins/")

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert "error" in data[0]["parameters_schema"]["properties"]
        assert "Could not load schema" in data[0]["parameters_schema"]["properties"]["error"]["default"]

    def test_get_available_plugins_without_docstring(self, client, mock_framework_manager):
        """Test plugins without a docstring"""
        # Arrange
        mock_plugin = Mock()
        mock_plugin.__class__.__module__ = "plugins.extractors.test"
        mock_plugin.__doc__ = None
        mock_plugin.get_parameters_schema.return_value = {}

        mock_framework_manager._plugin_name_cache = {
            "test_plugin": mock_plugin
        }

        # Act
        response = client.get("/plugins/")

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data[0]["description"] == "No description provided."

    def test_get_available_plugins_with_whitespace_docstring(self, client, mock_framework_manager):
        """Test plugins with docstring containing whitespace"""
        # Arrange
        mock_plugin = Mock()
        mock_plugin.__class__.__module__ = "plugins.extractors.test"
        mock_plugin.__doc__ = "  Test plugin with spaces  \n"
        mock_plugin.get_parameters_schema.return_value = {}

        mock_framework_manager._plugin_name_cache = {
            "test_plugin": mock_plugin
        }

        # Act
        response = client.get("/plugins/")

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data[0]["description"] == "Test plugin with spaces"

    def test_get_available_plugins_complex_schema(self, client, mock_framework_manager):
        """Test plugins with a complex schema"""
        # Arrange
        mock_plugin = Mock()
        mock_plugin.__class__.__module__ = "plugins.extractors.test"
        mock_plugin.__doc__ = "Complex plugin"
        mock_plugin.get_parameters_schema.return_value = {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "File path"
                },
                "options": {
                    "type": "object",
                    "properties": {
                        "encoding": {"type": "string", "default": "utf-8"},
                        "delimiter": {"type": "string", "default": ","}
                    }
                }
            },
            "required": ["path"]
        }

        mock_framework_manager._plugin_name_cache = {
            "complex_plugin": mock_plugin
        }

        # Act
        response = client.get("/plugins/")

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data[0]["parameters_schema"]["properties"]["path"]["type"] == "string"
        assert data[0]["parameters_schema"]["required"] == ["path"]
        assert data[0]["parameters_schema"]["properties"]["options"]["properties"]["encoding"]["default"] == "utf-8"

    def test_get_available_plugins_all_types(self, client, mock_framework_manager):
        """Test all plugin types are correctly classified"""
        # Arrange
        plugins = {}
        types = [
            ("extractor", "plugins.extractors.test"),
            ("cleanser", "plugins.cleansing.test"),
            ("transformer", "plugins.transformers.test"),
            ("validator", "plugins.validators.test"),
            ("loader", "plugins.loaders.test"),
            ("unknown", "plugins.custom.test")
        ]

        for plugin_type, module_path in types:
            mock_plugin = Mock()
            mock_plugin.__class__.__module__ = module_path
            mock_plugin.__doc__ = f"{plugin_type} plugin"
            mock_plugin.get_parameters_schema.return_value = {}
            plugins[f"{plugin_type}_plugin"] = mock_plugin

        mock_framework_manager._plugin_name_cache = plugins

        # Act
        response = client.get("/plugins/")

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 6

        # Verify each type is correctly set
        plugin_types = {p["name"]: p["type"] for p in data}
        assert plugin_types["extractor_plugin"] == "extractor"
        assert plugin_types["cleanser_plugin"] == "cleanser"
        assert plugin_types["transformer_plugin"] == "transformer"
        assert plugin_types["validator_plugin"] == "validator"
        assert plugin_types["loader_plugin"] == "loader"
        assert plugin_types["unknown_plugin"] == "unknown"

    def test_get_available_plugins_response_model(self, client, mock_framework_manager):
        """Test the response model structure"""
        # Arrange
        mock_plugin = Mock()
        mock_plugin.__class__.__module__ = "plugins.extractors.test"
        mock_plugin.__doc__ = "Test plugin"
        mock_plugin.get_parameters_schema.return_value = {"type": "object"}

        mock_framework_manager._plugin_name_cache = {
            "test_plugin": mock_plugin
        }

        # Act
        response = client.get("/plugins/")

        # Assert
        assert response.status_code == 200
        data = response.json()

        # Verify the response structure
        plugin = data[0]
        assert "name" in plugin
        assert "type" in plugin
        assert "description" in plugin
        assert "parameters_schema" in plugin

        # Verify field types
        assert isinstance(plugin["name"], str)
        assert isinstance(plugin["type"], str)
        assert isinstance(plugin["description"], str)
        assert isinstance(plugin["parameters_schema"], dict)

    def test_router_prefix_and_tags(self):
        """Test router prefix and tags"""
        # Assert
        assert router.prefix == "/plugins"
        assert "Plugins" in router.tags

    def test_get_available_plugins_with_unicode_docstring(self, client, mock_framework_manager):
        """Test plugins with Unicode characters in docstring"""
        # Arrange
        mock_plugin = Mock()
        mock_plugin.__class__.__module__ = "plugins.extractors.test"
        mock_plugin.__doc__ = "日本語の説明文です"
        mock_plugin.get_parameters_schema.return_value = {}

        mock_framework_manager._plugin_name_cache = {
            "japanese_plugin": mock_plugin
        }

        # Act
        response = client.get("/plugins/")

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data[0]["description"] == "日本語の説明文です"

    def test_get_available_plugins_empty_string_docstring(self, client, mock_framework_manager):
        """Test plugins with an empty string docstring"""
        # Arrange
        mock_plugin = Mock()
        mock_plugin.__class__.__module__ = "plugins.extractors.test"
        mock_plugin.__doc__ = ""
        mock_plugin.get_parameters_schema.return_value = {}

        mock_framework_manager._plugin_name_cache = {
            "test_plugin": mock_plugin
        }

        # Act
        response = client.get("/plugins/")

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data[0]["description"] == "No description provided."


class TestPluginsRouterEdgeCases:
    """Test class for plugin router edge cases"""

    @pytest.fixture
    def client(self):
        """Fixture providing a FastAPI test client"""
        app = FastAPI()
        app.include_router(router)
        return TestClient(app)

    @pytest.fixture
    def mock_framework_manager(self):
        """Fixture mocking the framework_manager"""
        with patch('api.routers.plugins.framework_manager') as mock_manager:
            yield mock_manager

    def test_get_available_plugins_with_very_long_name(self, client, mock_framework_manager):
        """Test plugins with a very long name"""
        # Arrange
        long_name = "a" * 1000
        mock_plugin = Mock()
        mock_plugin.__class__.__module__ = "plugins.extractors.test"
        mock_plugin.__doc__ = "Test"
        mock_plugin.get_parameters_schema.return_value = {}

        mock_framework_manager._plugin_name_cache = {
            long_name: mock_plugin
        }

        # Act
        response = client.get("/plugins/")

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data[0]["name"] == long_name

    def test_get_available_plugins_mixed_sorting(self, client, mock_framework_manager):
        """Test sorting when names contain both numbers and letters"""
        # Arrange
        plugins = {}
        names = ["plugin10", "plugin2", "plugin1", "pluginA", "pluginB"]

        for name in names:
            mock_plugin = Mock()
            mock_plugin.__class__.__module__ = "plugins.extractors.test"
            mock_plugin.__doc__ = f"{name} description"
            mock_plugin.get_parameters_schema.return_value = {}
            plugins[name] = mock_plugin

        mock_framework_manager._plugin_name_cache = plugins

        # Act
        response = client.get("/plugins/")

        # Assert
        assert response.status_code == 200
        data = response.json()
        result_names = [p["name"] for p in data]
        assert result_names == sorted(names)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
