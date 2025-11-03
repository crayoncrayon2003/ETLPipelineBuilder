import pytest
from pydantic import ValidationError
from api.schemas.pipeline import PipelineNode, PipelineEdge, PipelineDefinition


class TestPipelineNode:
    """Test class for the PipelineNode schema"""

    def test_pipeline_node_valid_creation(self):
        """Test that a valid PipelineNode can be created"""
        # Arrange & Act
        node = PipelineNode(
            id="node-1",
            plugin="test_plugin",
            params={"key": "value"}
        )

        # Assert
        assert node.id == "node-1"
        assert node.plugin == "test_plugin"
        assert node.params == {"key": "value"}

    def test_pipeline_node_without_params(self):
        """Test that a PipelineNode can be created without params"""
        # Arrange & Act
        node = PipelineNode(
            id="node-1",
            plugin="test_plugin"
        )

        # Assert
        assert node.id == "node-1"
        assert node.plugin == "test_plugin"
        assert node.params == {}  # default_factory=dict

    def test_pipeline_node_with_empty_params(self):
        """Test that a PipelineNode can be created with empty params"""
        # Arrange & Act
        node = PipelineNode(
            id="node-1",
            plugin="test_plugin",
            params={}
        )

        # Assert
        assert node.params == {}

    def test_pipeline_node_with_complex_params(self):
        """Test that a PipelineNode can be created with complex params"""
        # Arrange
        complex_params = {
            "string": "value",
            "number": 123,
            "boolean": True,
            "list": [1, 2, 3],
            "nested": {
                "key": "value",
                "deep": {
                    "level": 3
                }
            }
        }

        # Act
        node = PipelineNode(
            id="node-1",
            plugin="test_plugin",
            params=complex_params
        )

        # Assert
        assert node.params == complex_params
        assert node.params["nested"]["deep"]["level"] == 3

    def test_pipeline_node_missing_id(self):
        """Test validation error when id is missing"""
        # Arrange & Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            PipelineNode(
                plugin="test_plugin",
                params={}
            )
        
        # Ensure 'id' is included in the error
        assert "id" in str(exc_info.value)

    def test_pipeline_node_missing_plugin(self):
        """Test validation error when plugin is missing"""
        # Arrange & Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            PipelineNode(
                id="node-1",
                params={}
            )
        
        # Ensure 'plugin' is included in the error
        assert "plugin" in str(exc_info.value)

    def test_pipeline_node_with_special_characters_in_id(self):
        """Test that a PipelineNode can be created with special characters in the ID"""
        # Arrange & Act
        node = PipelineNode(
            id="node-extractor-csv-1",
            plugin="from_local_file",
            params={"source_path": "data/input/source.csv"}
        )

        # Assert
        assert node.id == "node-extractor-csv-1"

    def test_pipeline_node_json_serialization(self):
        """Test that a PipelineNode can be serialized to JSON"""
        # Arrange
        node = PipelineNode(
            id="node-1",
            plugin="test_plugin",
            params={"key": "value"}
        )

        # Act
        json_str = node.model_dump_json()

        # Assert
        assert "node-1" in json_str
        assert "test_plugin" in json_str
        assert "key" in json_str

    def test_pipeline_node_dict_conversion(self):
        """Test that a PipelineNode can be converted to a dictionary"""
        # Arrange
        node = PipelineNode(
            id="node-1",
            plugin="test_plugin",
            params={"key": "value"}
        )

        # Act
        node_dict = node.model_dump()

        # Assert
        assert node_dict["id"] == "node-1"
        assert node_dict["plugin"] == "test_plugin"
        assert node_dict["params"]["key"] == "value"


class TestPipelineEdge:
    """Test class for the PipelineEdge schema"""

    def test_pipeline_edge_valid_creation(self):
        """Test that a valid PipelineEdge can be created"""
        # Arrange & Act
        edge = PipelineEdge(
            source_node_id="node-1",
            target_node_id="node-2",
            target_input_name="input_data"
        )

        # Assert
        assert edge.source_node_id == "node-1"
        assert edge.target_node_id == "node-2"
        assert edge.target_input_name == "input_data"

    def test_pipeline_edge_missing_source_node_id(self):
        """Test validation error when source_node_id is missing"""
        # Arrange & Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            PipelineEdge(
                target_node_id="node-2",
                target_input_name="input_data"
            )
        
        assert "source_node_id" in str(exc_info.value)

    def test_pipeline_edge_missing_target_node_id(self):
        """Test validation error when target_node_id is missing"""
        # Arrange & Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            PipelineEdge(
                source_node_id="node-1",
                target_input_name="input_data"
            )
        
        assert "target_node_id" in str(exc_info.value)

    def test_pipeline_edge_missing_target_input_name(self):
        """Test validation error when target_input_name is missing"""
        # Arrange & Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            PipelineEdge(
                source_node_id="node-1",
                target_node_id="node-2"
            )
        
        assert "target_input_name" in str(exc_info.value)

    def test_pipeline_edge_with_complex_node_ids(self):
        """Test that a PipelineEdge can be created with complex node IDs"""
        # Arrange & Act
        edge = PipelineEdge(
            source_node_id="node-extractor-csv-1",
            target_node_id="node-validator-quality-1",
            target_input_name="input_data"
        )

        # Assert
        assert edge.source_node_id == "node-extractor-csv-1"
        assert edge.target_node_id == "node-validator-quality-1"

    def test_pipeline_edge_json_serialization(self):
        """Test that a PipelineEdge can be serialized to JSON"""
        # Arrange
        edge = PipelineEdge(
            source_node_id="node-1",
            target_node_id="node-2",
            target_input_name="input_data"
        )

        # Act
        json_str = edge.model_dump_json()

        # Assert
        assert "node-1" in json_str
        assert "node-2" in json_str
        assert "input_data" in json_str

    def test_pipeline_edge_dict_conversion(self):
        """Test that a PipelineEdge can be converted to a dictionary"""
        # Arrange
        edge = PipelineEdge(
            source_node_id="node-1",
            target_node_id="node-2",
            target_input_name="input_data"
        )

        # Act
        edge_dict = edge.model_dump()

        # Assert
        assert edge_dict["source_node_id"] == "node-1"
        assert edge_dict["target_node_id"] == "node-2"
        assert edge_dict["target_input_name"] == "input_data"


class TestPipelineDefinition:
    """Test class for the PipelineDefinition schema"""

    def test_pipeline_definition_valid_creation(self):
        """Test that a valid PipelineDefinition can be created"""
        # Arrange & Act
        pipeline = PipelineDefinition(
            name="Test Pipeline",
            nodes=[
                PipelineNode(
                    id="node-1",
                    plugin="test_plugin",
                    params={"key": "value"}
                )
            ],
            edges=[]
        )

        # Assert
        assert pipeline.name == "Test Pipeline"
        assert len(pipeline.nodes) == 1
        assert len(pipeline.edges) == 0

    def test_pipeline_definition_with_multiple_nodes(self):
        """Test creating a PipelineDefinition with multiple nodes"""
        # Arrange & Act
        pipeline = PipelineDefinition(
            name="Multi Node Pipeline",
            nodes=[
                PipelineNode(id="node-1", plugin="plugin1", params={}),
                PipelineNode(id="node-2", plugin="plugin2", params={}),
                PipelineNode(id="node-3", plugin="plugin3", params={})
            ],
            edges=[]
        )

        # Assert
        assert len(pipeline.nodes) == 3
        assert pipeline.nodes[0].id == "node-1"
        assert pipeline.nodes[1].id == "node-2"
        assert pipeline.nodes[2].id == "node-3"

    def test_pipeline_definition_with_edges(self):
        """Test creating a PipelineDefinition with edges"""
        # Arrange & Act
        pipeline = PipelineDefinition(
            name="Pipeline With Edges",
            nodes=[
                PipelineNode(id="node-1", plugin="plugin1", params={}),
                PipelineNode(id="node-2", plugin="plugin2", params={})
            ],
            edges=[
                PipelineEdge(
                    source_node_id="node-1",
                    target_node_id="node-2",
                    target_input_name="input_data"
                )
            ]
        )

        # Assert
        assert len(pipeline.nodes) == 2
        assert len(pipeline.edges) == 1
        assert pipeline.edges[0].source_node_id == "node-1"
        assert pipeline.edges[0].target_node_id == "node-2"

    def test_pipeline_definition_missing_name(self):
        """Test validation error when name is missing"""
        # Arrange & Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            PipelineDefinition(
                nodes=[],
                edges=[]
            )
        
        assert "name" in str(exc_info.value)

    def test_pipeline_definition_missing_nodes(self):
        """Test validation error when nodes are missing"""
        # Arrange & Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            PipelineDefinition(
                name="Test Pipeline",
                edges=[]
            )
        
        assert "nodes" in str(exc_info.value)

    def test_pipeline_definition_missing_edges(self):
        """Test validation error when edges are missing"""
        # Arrange & Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            PipelineDefinition(
                name="Test Pipeline",
                nodes=[]
            )
        
        assert "edges" in str(exc_info.value)

    def test_pipeline_definition_with_empty_nodes_and_edges(self):
        """Test creating a PipelineDefinition with empty nodes and edges"""
        # Arrange & Act
        pipeline = PipelineDefinition(
            name="Empty Pipeline",
            nodes=[],
            edges=[]
        )

        # Assert
        assert pipeline.name == "Empty Pipeline"
        assert len(pipeline.nodes) == 0
        assert len(pipeline.edges) == 0

    def test_pipeline_definition_from_dict(self):
        """Test creating a PipelineDefinition from a dictionary"""
        # Arrange
        pipeline_dict = {
            "name": "Test Pipeline",
            "nodes": [
                {
                    "id": "node-1",
                    "plugin": "test_plugin",
                    "params": {"key": "value"}
                }
            ],
            "edges": []
        }

        # Act
        pipeline = PipelineDefinition(**pipeline_dict)

        # Assert
        assert pipeline.name == "Test Pipeline"
        assert len(pipeline.nodes) == 1
        assert pipeline.nodes[0].id == "node-1"

    def test_pipeline_definition_json_serialization(self):
        """Test that a PipelineDefinition can be serialized to JSON"""
        # Arrange
        pipeline = PipelineDefinition(
            name="Test Pipeline",
            nodes=[
                PipelineNode(id="node-1", plugin="plugin1", params={"key": "value"})
            ],
            edges=[
                PipelineEdge(
                    source_node_id="node-1",
                    target_node_id="node-2",
                    target_input_name="input_data"
                )
            ]
        )

        # Act
        json_str = pipeline.model_dump_json()

        # Assert
        assert "Test Pipeline" in json_str
        assert "node-1" in json_str
        assert "plugin1" in json_str
        assert "input_data" in json_str

    def test_pipeline_definition_dict_conversion(self):
        """Test that a PipelineDefinition can be converted to a dictionary"""
        # Arrange
        pipeline = PipelineDefinition(
            name="Test Pipeline",
            nodes=[
                PipelineNode(id="node-1", plugin="plugin1", params={})
            ],
            edges=[]
        )

        # Act
        pipeline_dict = pipeline.model_dump()

        # Assert
        assert pipeline_dict["name"] == "Test Pipeline"
        assert len(pipeline_dict["nodes"]) == 1
        assert pipeline_dict["nodes"][0]["id"] == "node-1"

    def test_pipeline_definition_complex_dag(self):
        """Test creating a PipelineDefinition with a complex DAG structure"""
        # Arrange & Act
        pipeline = PipelineDefinition(
            name="Complex DAG Pipeline",
            nodes=[
                PipelineNode(id="node-1", plugin="extractor", params={}),
                PipelineNode(id="node-2", plugin="transformer", params={}),
                PipelineNode(id="node-3", plugin="validator", params={}),
                PipelineNode(id="node-4", plugin="loader", params={})
            ],
            edges=[
                PipelineEdge(
                    source_node_id="node-1",
                    target_node_id="node-2",
                    target_input_name="input_data"
                ),
                PipelineEdge(
                    source_node_id="node-2",
                    target_node_id="node-3",
                    target_input_name="input_data"
                ),
                PipelineEdge(
                    source_node_id="node-3",
                    target_node_id="node-4",
                    target_input_name="input_data"
                )
            ]
        )

        # Assert
        assert len(pipeline.nodes) == 4
        assert len(pipeline.edges) == 3
        # Verify path exists from first node to last node
        assert pipeline.edges[0].source_node_id == "node-1"
        assert pipeline.edges[-1].target_node_id == "node-4"

    def test_pipeline_definition_with_unicode_characters(self):
        """Test creating a PipelineDefinition with Unicode characters"""
        # Arrange & Act
        pipeline = PipelineDefinition(
            name="テストパイプライン",
            nodes=[
                PipelineNode(
                    id="ノード-1",
                    plugin="プラグイン1",
                    params={"キー": "値"}
                )
            ],
            edges=[]
        )

        # Assert
        assert pipeline.name == "テストパイプライン"
        assert pipeline.nodes[0].id == "ノード-1"
        assert pipeline.nodes[0].params["キー"] == "値"

    def test_pipeline_definition_with_very_long_name(self):
        """Test creating a PipelineDefinition with a very long name"""
        # Arrange
        long_name = "a" * 1000

        # Act
        pipeline = PipelineDefinition(
            name=long_name,
            nodes=[],
            edges=[]
        )

        # Assert
        assert len(pipeline.name) == 1000

    def test_pipeline_definition_nested_params(self):
        """Test creating a PipelineDefinition with nested params"""
        # Arrange & Act
        pipeline = PipelineDefinition(
            name="Nested Params Pipeline",
            nodes=[
                PipelineNode(
                    id="node-1",
                    plugin="plugin1",
                    params={
                        "level1": {
                            "level2": {
                                "level3": {
                                    "value": "deep"
                                }
                            }
                        }
                    }
                )
            ],
            edges=[]
        )

        # Assert
        assert pipeline.nodes[0].params["level1"]["level2"]["level3"]["value"] == "deep"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
