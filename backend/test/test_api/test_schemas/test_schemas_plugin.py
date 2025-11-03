import pytest
from pydantic import ValidationError
from api.schemas.plugin import PluginInfo, PluginType


class TestPluginType:
    """Test class for PluginType (Literal type)"""

    def test_plugin_type_valid_values(self):
        """Test valid PluginType values"""
        # Arrange
        valid_types = ["extractor", "cleanser", "transformer", "validator", "loader", "unknown"]

        # Act & Assert
        for plugin_type in valid_types:
            # Create PluginInfo and check if the type field is accepted
            plugin = PluginInfo(
                name="test_plugin",
                type=plugin_type,
                description="Test description",
                parameters_schema={}
            )
            assert plugin.type == plugin_type

    def test_plugin_type_invalid_value(self):
        """Test invalid PluginType value"""
        # Arrange & Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            PluginInfo(
                name="test_plugin",
                type="invalid_type",  # invalid value
                description="Test description",
                parameters_schema={}
            )

        # Check that 'type' is included in the error message
        assert "type" in str(exc_info.value)


class TestPluginInfo:
    """Test class for PluginInfo schema"""

    def test_plugin_info_valid_creation(self):
        """Test that a valid PluginInfo can be created"""
        # Arrange & Act
        plugin = PluginInfo(
            name="from_local_file",
            type="extractor",
            description="Reads a source file",
            parameters_schema={"type": "object", "properties": {}}
        )

        # Assert
        assert plugin.name == "from_local_file"
        assert plugin.type == "extractor"
        assert plugin.description == "Reads a source file"
        assert plugin.parameters_schema == {"type": "object", "properties": {}}

    def test_plugin_info_all_plugin_types(self):
        """Test creating PluginInfo with all PluginType values"""
        # Arrange
        plugin_types = ["extractor", "cleanser", "transformer", "validator", "loader", "unknown"]

        # Act & Assert
        for plugin_type in plugin_types:
            plugin = PluginInfo(
                name=f"{plugin_type}_plugin",
                type=plugin_type,
                description=f"A {plugin_type} plugin",
                parameters_schema={}
            )
            assert plugin.type == plugin_type

    def test_plugin_info_missing_name(self):
        """Test validation error when name is missing"""
        # Arrange & Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            PluginInfo(
                type="extractor",
                description="Test description",
                parameters_schema={}
            )

        assert "name" in str(exc_info.value)

    def test_plugin_info_missing_type(self):
        """Test validation error when type is missing"""
        # Arrange & Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            PluginInfo(
                name="test_plugin",
                description="Test description",
                parameters_schema={}
            )

        assert "type" in str(exc_info.value)

    def test_plugin_info_missing_description(self):
        """Test validation error when description is missing"""
        # Arrange & Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            PluginInfo(
                name="test_plugin",
                type="extractor",
                parameters_schema={}
            )

        assert "description" in str(exc_info.value)

    def test_plugin_info_missing_parameters_schema(self):
        """Test validation error when parameters_schema is missing"""
        # Arrange & Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            PluginInfo(
                name="test_plugin",
                type="extractor",
                description="Test description"
            )

        assert "parameters_schema" in str(exc_info.value)

    def test_plugin_info_with_empty_parameters_schema(self):
        """Test that PluginInfo can be created with empty parameters_schema"""
        # Arrange & Act
        plugin = PluginInfo(
            name="test_plugin",
            type="extractor",
            description="Test description",
            parameters_schema={}
        )

        # Assert
        assert plugin.parameters_schema == {}

    def test_plugin_info_with_complex_parameters_schema(self):
        """Test creating PluginInfo with complex parameters_schema"""
        # Arrange
        complex_schema = {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "File path"
                },
                "encoding": {
                    "type": "string",
                    "default": "utf-8",
                    "enum": ["utf-8", "latin-1", "ascii"]
                },
                "options": {
                    "type": "object",
                    "properties": {
                        "delimiter": {"type": "string"},
                        "header": {"type": "boolean"}
                    }
                }
            },
            "required": ["path"]
        }

        # Act
        plugin = PluginInfo(
            name="csv_reader",
            type="extractor",
            description="Reads CSV files",
            parameters_schema=complex_schema
        )

        # Assert
        assert plugin.parameters_schema == complex_schema
        assert plugin.parameters_schema["properties"]["path"]["type"] == "string"
        assert "delimiter" in plugin.parameters_schema["properties"]["options"]["properties"]

    def test_plugin_info_with_long_description(self):
        """Test creating PluginInfo with a long description"""
        # Arrange
        long_description = "A" * 1000

        # Act
        plugin = PluginInfo(
            name="test_plugin",
            type="extractor",
            description=long_description,
            parameters_schema={}
        )

        # Assert
        assert len(plugin.description) == 1000

    def test_plugin_info_with_unicode_characters(self):
        """Test creating PluginInfo with Unicode characters"""
        # Arrange & Act
        plugin = PluginInfo(
            name="日本語プラグイン",
            type="extractor",
            description="これは日本語の説明です",
            parameters_schema={"キー": "値"}
        )

        # Assert
        assert plugin.name == "日本語プラグイン"
        assert plugin.description == "これは日本語の説明です"
        assert plugin.parameters_schema["キー"] == "値"

    def test_plugin_info_json_serialization(self):
        """Test that PluginInfo can be serialized to JSON"""
        # Arrange
        plugin = PluginInfo(
            name="test_plugin",
            type="extractor",
            description="Test description",
            parameters_schema={"type": "object"}
        )

        # Act
        json_str = plugin.model_dump_json()

        # Assert
        assert "test_plugin" in json_str
        assert "extractor" in json_str
        assert "Test description" in json_str
        assert "type" in json_str

    def test_plugin_info_dict_conversion(self):
        """Test that PluginInfo can be converted to a dictionary"""
        # Arrange
        plugin = PluginInfo(
            name="test_plugin",
            type="extractor",
            description="Test description",
            parameters_schema={"type": "object"}
        )

        # Act
        plugin_dict = plugin.model_dump()

        # Assert
        assert plugin_dict["name"] == "test_plugin"
        assert plugin_dict["type"] == "extractor"
        assert plugin_dict["description"] == "Test description"
        assert plugin_dict["parameters_schema"]["type"] == "object"

    def test_plugin_info_from_dict(self):
        """Test creating PluginInfo from a dictionary"""
        # Arrange
        plugin_dict = {
            "name": "from_local_file",
            "type": "extractor",
            "description": "Reads a source file",
            "parameters_schema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"}
                },
                "required": ["path"]
            }
        }

        # Act
        plugin = PluginInfo(**plugin_dict)

        # Assert
        assert plugin.name == "from_local_file"
        assert plugin.type == "extractor"
        assert plugin.parameters_schema["required"] == ["path"]

    def test_plugin_info_extractor_example(self):
        """Test example of an Extractor type plugin"""
        # Arrange & Act
        plugin = PluginInfo(
            name="from_local_file",
            type="extractor",
            description="(File-based) Reads a source file (like CSV) and saves it as Parquet...",
            parameters_schema={
                "type": "object",
                "properties": {
                    "source_path": {"type": "string", "description": "Path to source file"}
                },
                "required": ["source_path"]
            }
        )

        # Assert
        assert plugin.type == "extractor"
        assert "source_path" in plugin.parameters_schema["properties"]

    def test_plugin_info_cleanser_example(self):
        """Test example of a Cleanser type plugin"""
        # Arrange & Act
        plugin = PluginInfo(
            name="remove_duplicates",
            type="cleanser",
            description="Removes duplicate rows from dataset",
            parameters_schema={
                "type": "object",
                "properties": {
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"}
                    }
                }
            }
        )

        # Assert
        assert plugin.type == "cleanser"

    def test_plugin_info_transformer_example(self):
        """Test example of a Transformer type plugin"""
        # Arrange & Act
        plugin = PluginInfo(
            name="normalize_columns",
            type="transformer",
            description="Normalizes column values",
            parameters_schema={
                "type": "object",
                "properties": {
                    "method": {
                        "type": "string",
                        "enum": ["min-max", "z-score"]
                    }
                }
            }
        )

        # Assert
        assert plugin.type == "transformer"

    def test_plugin_info_validator_example(self):
        """Test example of a Validator type plugin"""
        # Arrange & Act
        plugin = PluginInfo(
            name="check_data_quality",
            type="validator",
            description="Validates data quality",
            parameters_schema={
                "type": "object",
                "properties": {
                    "rules": {
                        "type": "array",
                        "items": {"type": "object"}
                    }
                }
            }
        )

        # Assert
        assert plugin.type == "validator"

    def test_plugin_info_loader_example(self):
        """Test example of a Loader type plugin"""
        # Arrange & Act
        plugin = PluginInfo(
            name="to_database",
            type="loader",
            description="Loads data into database",
            parameters_schema={
                "type": "object",
                "properties": {
                    "connection_string": {"type": "string"},
                    "table_name": {"type": "string"}
                },
                "required": ["connection_string", "table_name"]
            }
        )

        # Assert
        assert plugin.type == "loader"
        assert len(plugin.parameters_schema["required"]) == 2

    def test_plugin_info_unknown_type_example(self):
        """Test example of an Unknown type plugin"""
        # Arrange & Act
        plugin = PluginInfo(
            name="custom_plugin",
            type="unknown",
            description="A custom plugin with unknown type",
            parameters_schema={}
        )

        # Assert
        assert plugin.type == "unknown"

    def test_plugin_info_with_nested_schema_properties(self):
        """Test PluginInfo with nested schema properties"""
        # Arrange
        nested_schema = {
            "type": "object",
            "properties": {
                "config": {
                    "type": "object",
                    "properties": {
                        "settings": {
                            "type": "object",
                            "properties": {
                                "option1": {"type": "string"},
                                "option2": {"type": "integer"}
                            }
                        }
                    }
                }
            }
        }

        # Act
        plugin = PluginInfo(
            name="nested_plugin",
            type="transformer",
            description="Plugin with nested schema",
            parameters_schema=nested_schema
        )

        # Assert
        assert plugin.parameters_schema["properties"]["config"]["properties"]["settings"]["properties"]["option1"]["type"] == "string"

    def test_plugin_info_with_array_schema(self):
        """Test PluginInfo with an array schema"""
        # Arrange
        array_schema = {
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string"},
                            "value": {"type": "number"}
                        }
                    }
                }
            }
        }

        # Act
        plugin = PluginInfo(
            name="array_plugin",
            type="transformer",
            description="Plugin with array schema",
            parameters_schema=array_schema
        )

        # Assert
        assert plugin.parameters_schema["properties"]["items"]["type"] == "array"

    def test_plugin_info_special_characters_in_name(self):
        """Test PluginInfo with special characters in name"""
        # Arrange & Act
        plugin = PluginInfo(
            name="plugin-with-dashes_and_underscores.v2",
            type="extractor",
            description="Plugin with special characters",
            parameters_schema={}
        )

        # Assert
        assert plugin.name == "plugin-with-dashes_and_underscores.v2"

    def test_plugin_info_empty_string_description(self):
        """Test creating PluginInfo with an empty string description"""
        # Arrange & Act
        plugin = PluginInfo(
            name="test_plugin",
            type="extractor",
            description="",
            parameters_schema={}
        )

        # Assert
        assert plugin.description == ""

    def test_plugin_info_comparison(self):
        """Test comparison of two PluginInfo instances"""
        # Arrange
        plugin1 = PluginInfo(
            name="test_plugin",
            type="extractor",
            description="Test",
            parameters_schema={}
        )

        plugin2 = PluginInfo(
            name="test_plugin",
            type="extractor",
            description="Test",
            parameters_schema={}
        )

        # Act & Assert
        assert plugin1.name == plugin2.name
        assert plugin1.type == plugin2.type
        assert plugin1.description == plugin2.description


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
