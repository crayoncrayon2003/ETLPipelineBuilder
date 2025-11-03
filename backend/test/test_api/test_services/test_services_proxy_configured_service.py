import os
import tempfile
import json
import pytest
from unittest.mock import patch, MagicMock

from api.services.proxy_configured_service import process_configured_request
from api.schemas.pipeline import PipelineDefinition, PipelineNode, PipelineEdge
from core.data_container.container import DataContainer


class TestProcessConfiguredRequest:
    """Test class for process_configured_request function"""

    @patch("api.services.proxy_configured_service.storage_adapter.read_text")
    @patch("api.services.proxy_configured_service.StepExecutor.execute_step")
    def test_process_configured_request_single_node(
        self, mock_execute_step, mock_read_text
    ):
        # Mock setup
        node_id = "node1"
        plugin_name = "test_plugin"

        pipeline_dict = {
            "name": "Test Pipeline",
            "nodes": [{"id": node_id, "plugin": plugin_name, "params": {}}],
            "edges": []
        }
        mock_read_text.return_value = json.dumps(pipeline_dict)

        mock_container = MagicMock(spec=DataContainer)
        mock_container.metadata = {"mocked": True}
        mock_container.get_primary_file_path.return_value = "/mock/path/file.dat"
        mock_execute_step.return_value = mock_container

        # Input data
        body_bytes = b"test data"
        headers = {"Authorization": "Bearer token"}
        project_root = "/home/project"
        config_path = "/mock/config.json"

        result = process_configured_request(body_bytes, config_path, headers, project_root)

        # Check that read_text was called
        mock_read_text.assert_called_once_with(config_path)

        # Check that execute_step was called
        mock_execute_step.assert_called_once()
        called_args = mock_execute_step.call_args[0][0]  # step dict passed to execute_step
        assert called_args["name"] == node_id
        assert called_args["plugin"] == plugin_name

        # Assert return value
        assert result["status"] == "ok"
        assert result["final_metadata"] == {"mocked": True}
        assert result["primary_file"] == "/mock/path/file.dat"

    @patch("api.services.proxy_configured_service.storage_adapter.read_text")
    @patch("api.services.proxy_configured_service.StepExecutor.execute_step")
    def test_process_configured_request_with_edges(
        self, mock_execute_step, mock_read_text
    ):
        # Pipeline with DAG structure
        pipeline_dict = {
            "name": "Edge Pipeline",
            "nodes": [
                {"id": "node1", "plugin": "plugin1", "params": {}},
                {"id": "node2", "plugin": "plugin2", "params": {}}
            ],
            "edges": [
                {"source_node_id": "node1", "target_node_id": "node2", "target_input_name": "input1"}
            ]
        }
        mock_read_text.return_value = json.dumps(pipeline_dict)

        # Mock return values for execute_step
        container_node1 = MagicMock(spec=DataContainer)
        container_node1.metadata = {"node": "1"}
        container_node2 = MagicMock(spec=DataContainer)
        container_node2.metadata = {"node": "2"}
        mock_execute_step.side_effect = [container_node1, container_node2]

        body_bytes = b"test data"
        headers = {"Authorization": "Bearer token"}
        project_root = "/home/project"
        config_path = "/mock/config.json"

        result = process_configured_request(body_bytes, config_path, headers, project_root)

        # execute_step is called twice
        assert mock_execute_step.call_count == 2

        # Return the container from the final node
        assert result["status"] == "ok"
        assert result["final_metadata"] == {"node": "2"}
        assert result["primary_file"] == container_node2.get_primary_file_path()

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
