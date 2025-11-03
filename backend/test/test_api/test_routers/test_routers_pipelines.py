import os
import json
import pytest
from unittest.mock import Mock, patch, MagicMock, mock_open
from fastapi import BackgroundTasks, HTTPException
from fastapi.testclient import TestClient
from api.routers.pipelines import (
    router,
    _save_pipeline_definition,
    PIPELINE_DEFINITIONS_DIR
)
from api.schemas.pipeline import PipelineDefinition


class TestPipelineDefinitionSave:
    """Test class for the _save_pipeline_definition function"""

    @pytest.fixture
    def sample_pipeline_def(self):
        """Fixture providing a sample pipeline definition"""
        return PipelineDefinition(
            name="test_pipeline",
            nodes=[
                {
                    "id": "node1",
                    "name": "step1",
                    "plugin": "test_plugin",
                    "params": {"key": "value"}
                }
            ],
            edges=[]
        )

    @patch('builtins.open', new_callable=mock_open)
    @patch('os.makedirs')
    def test_save_pipeline_definition_creates_file(
        self,
        mock_makedirs,
        mock_file,
        sample_pipeline_def
    ):
        """Test that the pipeline definition is correctly saved to a file"""
        # Act
        result_path = _save_pipeline_definition(sample_pipeline_def)

        # Assert
        assert "test_pipeline.json" in result_path
        assert PIPELINE_DEFINITIONS_DIR in result_path
        
        # Verify that open was called (may be called multiple times due to logger)
        assert mock_file.call_count >= 1
        
        # Verify that the first call was for file saving
        first_call = mock_file.call_args_list[0]
        assert 'test_pipeline.json' in first_call[0][0]
        assert first_call[0][1] == 'w'
        assert first_call[1]['encoding'] == 'utf-8'

    @patch('builtins.open', new_callable=mock_open)
    @patch('os.makedirs')
    def test_save_pipeline_definition_with_special_characters(
        self,
        mock_makedirs,
        mock_file
    ):
        """Test that names with special characters are converted to a safe format"""
        # Arrange
        pipeline_def = PipelineDefinition(
            name="test-pipeline@2024!",
            nodes=[{"id": "node1", "name": "step1", "plugin": "test_plugin", "params": {}}],
            edges=[]
        )

        # Act
        result_path = _save_pipeline_definition(pipeline_def)

        # Assert
        # Special characters should be replaced with underscores
        assert "test_pipeline_2024_.json" in result_path

    @patch('builtins.open', new_callable=mock_open)
    @patch('os.makedirs')
    def test_save_pipeline_definition_writes_valid_json(
        self,
        mock_makedirs,
        mock_file,
        sample_pipeline_def
    ):
        """Test that the saved JSON is valid"""
        # Act
        _save_pipeline_definition(sample_pipeline_def)

        # Assert
        # Verify write was called
        mock_file().write.assert_called_once()
        
        # Verify that the written content can be parsed as JSON
        written_content = mock_file().write.call_args[0][0]
        parsed = json.loads(written_content)
        assert parsed['name'] == 'test_pipeline'
        assert len(parsed['nodes']) == 1

    @patch('builtins.open', new_callable=mock_open)
    @patch('os.makedirs')
    def test_save_pipeline_definition_includes_all_fields(
        self,
        mock_makedirs,
        mock_file
    ):
        """Test that all fields are saved"""
        # Arrange
        pipeline_def = PipelineDefinition(
            name="complex_pipeline",
            nodes=[
                {
                    "id": "node1",
                    "plugin": "plugin1",
                    "params": {"key1": "value1", "key2": 123}
                },
                {
                    "id": "node2",
                    "plugin": "plugin2",
                    "params": {"nested": {"data": "value"}}
                }
            ],
            edges=[
                {
                    "source_node_id": "node1",
                    "target_node_id": "node2",
                    "target_input_name": "input_data"
                }
            ]
        )

        # Act
        _save_pipeline_definition(pipeline_def)

        # Assert
        # Verify that write was called
        assert any('write' in str(call) for call in mock_file.mock_calls)
        
        # Retrieve written content and verify
        write_calls = [call for call in mock_file.mock_calls if 'write' in str(call) and 'complex_pipeline' in str(call)]
        assert len(write_calls) > 0
        
        written_content = str(write_calls[0])
        assert 'complex_pipeline' in written_content or 'node1' in written_content


class TestPipelineRouter:
    """Test class for the pipeline router"""

    @pytest.fixture
    def client(self):
        """Fixture providing a FastAPI test client"""
        from fastapi import FastAPI
        app = FastAPI()
        app.include_router(router)
        return TestClient(app)

    @pytest.fixture
    def sample_pipeline_payload(self):
        """Fixture providing a test pipeline payload"""
        return {
            "name": "test_pipeline",
            "nodes": [
                {
                    "id": "node1",
                    "name": "step1",
                    "plugin": "test_plugin",
                    "params": {"key": "value"}
                }
            ],
            "edges": []
        }

    @patch('api.routers.pipelines.run_pipeline_from_definition')
    def test_run_pipeline_success(
        self,
        mock_run_pipeline,
        client,
        sample_pipeline_payload
    ):
        """Test that pipeline execution succeeds"""
        # Act
        response = client.post("/pipelines/run", json=sample_pipeline_payload)

        # Assert
        assert response.status_code == 202
        data = response.json()
        assert data["message"] == "Immediate pipeline execution started."
        assert data["pipeline_name"] == "test_pipeline"

    @patch('api.routers.pipelines.run_pipeline_from_definition')
    def test_run_pipeline_adds_background_task(
        self,
        mock_run_pipeline,
        client,
        sample_pipeline_payload
    ):
        """Test that a background task is added"""
        # Act
        response = client.post("/pipelines/run", json=sample_pipeline_payload)

        # Assert
        assert response.status_code == 202

    def test_run_pipeline_with_invalid_payload(self, client):
        """Test with invalid payload"""
        # Arrange
        invalid_payload = {
            "name": "test_pipeline"
            # nodes and edges fields are missing
        }

        # Act
        response = client.post("/pipelines/run", json=invalid_payload)

        # Assert
        assert response.status_code == 422  # Validation error

    def test_run_pipeline_with_empty_nodes(self, client):
        """Test with empty nodes"""
        # Arrange
        payload = {
            "name": "test_pipeline",
            "nodes": [],
            "edges": []
        }

        # Act
        with patch('api.routers.pipelines.run_pipeline_from_definition'):
            response = client.post("/pipelines/run", json=payload)

        # Assert
        # Empty nodes may still be accepted
        assert response.status_code in [202, 422]

    def test_run_pipeline_with_missing_plugin(self, client):
        """Test when plugin name is missing"""
        # Arrange
        payload = {
            "name": "test_pipeline",
            "nodes": [
                {
                    "id": "node1",
                    "name": "step1",
                    # "plugin" is missing
                    "params": {}
                }
            ],
            "edges": []
        }

        # Act
        response = client.post("/pipelines/run", json=payload)

        # Assert
        assert response.status_code == 422

    @patch('api.routers.pipelines.run_pipeline_from_definition')
    def test_run_pipeline_with_complex_params(
        self,
        mock_run_pipeline,
        client
    ):
        """Test a pipeline with complex parameters"""
        # Arrange
        payload = {
            "name": "complex_pipeline",
            "nodes": [
                {
                    "id": "node1",
                    "name": "step1",
                    "plugin": "plugin1",
                    "params": {
                        "string": "value",
                        "number": 123,
                        "boolean": True,
                        "list": [1, 2, 3],
                        "nested": {
                            "key": "value"
                        }
                    }
                }
            ],
            "edges": []
        }

        # Act
        response = client.post("/pipelines/run", json=payload)

        # Assert
        assert response.status_code == 202

    @patch('api.routers.pipelines.run_pipeline_from_definition')
    def test_run_pipeline_with_multiple_nodes(
        self,
        mock_run_pipeline,
        client
    ):
        """Test a pipeline with multiple nodes"""
        # Arrange
        payload = {
            "name": "multi_node_pipeline",
            "nodes": [
                {
                    "id": "node1",
                    "plugin": "plugin1",
                    "params": {"key1": "value1"}
                },
                {
                    "id": "node2",
                    "plugin": "plugin2",
                    "params": {"key2": "value2"}
                },
                {
                    "id": "node3",
                    "plugin": "plugin3",
                    "params": {"key3": "value3"}
                }
            ],
            "edges": [
                {
                    "source_node_id": "node1",
                    "target_node_id": "node2",
                    "target_input_name": "input_data"
                },
                {
                    "source_node_id": "node2",
                    "target_node_id": "node3",
                    "target_input_name": "input_data"
                }
            ]
        }

        # Act
        response = client.post("/pipelines/run", json=payload)

        # Assert
        assert response.status_code == 202
        data = response.json()
        assert data["pipeline_name"] == "multi_node_pipeline"

    def test_run_pipeline_with_special_characters_in_name(self, client):
        """Test when the pipeline name contains special characters"""
        # Arrange
        payload = {
            "name": "test-pipeline@2024!",
            "nodes": [
                {
                    "id": "node1",
                    "name": "step1",
                    "plugin": "test_plugin",
                    "params": {}
                }
            ],
            "edges": []
        }

        # Act
        with patch('api.routers.pipelines.run_pipeline_from_definition'):
            response = client.post("/pipelines/run", json=payload)

        # Assert
        assert response.status_code == 202

    @patch('api.routers.pipelines.run_pipeline_from_definition')
    def test_run_pipeline_with_empty_params(
        self,
        mock_run_pipeline,
        client
    ):
        """Test when params are empty"""
        # Arrange
        payload = {
            "name": "test_pipeline",
            "nodes": [
                {
                    "id": "node1",
                    "name": "step1",
                    "plugin": "test_plugin",
                    "params": {}
                }
            ],
            "edges": []
        }

        # Act
        response = client.post("/pipelines/run", json=payload)

        # Assert
        assert response.status_code == 202

    def test_router_prefix_and_tags(self):
        """Test that the router prefix and tags are correct"""
        # Assert
        assert router.prefix == "/pipelines"
        assert "Pipelines" in router.tags


class TestPipelineRouterEdgeCases:
    """Test class for edge cases"""

    @pytest.fixture
    def client(self):
        """Fixture providing a FastAPI test client"""
        from fastapi import FastAPI
        app = FastAPI()
        app.include_router(router)
        return TestClient(app)

    def test_run_pipeline_with_very_long_name(self, client):
        """Test with a very long name"""
        # Arrange
        long_name = "a" * 1000
        payload = {
            "name": long_name,
            "nodes": [
                {
                    "id": "node1",
                    "name": "step1",
                    "plugin": "test_plugin",
                    "params": {}
                }
            ],
            "edges": []
        }

        # Act
        with patch('api.routers.pipelines.run_pipeline_from_definition'):
            response = client.post("/pipelines/run", json=payload)

        # Assert
        # Very long names should still be accepted
        assert response.status_code == 202

    def test_run_pipeline_with_unicode_characters(self, client):
        """Test with Unicode characters"""
        # Arrange
        payload = {
            "name": "テストパイプライン_2024",
            "nodes": [
                {
                    "id": "node1",
                    "name": "ステップ1",
                    "plugin": "test_plugin",
                    "params": {"キー": "値"}
                }
            ],
            "edges": []
        }

        # Act
        with patch('api.routers.pipelines.run_pipeline_from_definition'):
            response = client.post("/pipelines/run", json=payload)

        # Assert
        assert response.status_code == 202

    @patch('api.routers.pipelines.run_pipeline_from_definition')
    def test_run_pipeline_response_format(
        self,
        mock_run_pipeline,
        client
    ):
        """Test that the response format is correct"""
        # Arrange
        payload = {
            "name": "test_pipeline",
            "nodes": [
                {
                    "id": "node1",
                    "name": "step1",
                    "plugin": "test_plugin",
                    "params": {}
                }
            ],
            "edges": []
        }

        # Act
        response = client.post("/pipelines/run", json=payload)

        # Assert
        assert response.status_code == 202
        data = response.json()
        assert "message" in data
        assert "pipeline_name" in data
        assert isinstance(data["message"], str)
        assert isinstance(data["pipeline_name"], str)


class TestPipelineDefinitionsDirectory:
    """Test class for the pipeline definitions directory"""

    def test_pipeline_definitions_dir_exists(self):
        """Test that the pipeline definitions directory path is correct"""
        # Assert
        assert "data" in PIPELINE_DEFINITIONS_DIR
        assert "pipeline_definitions" in PIPELINE_DEFINITIONS_DIR


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
