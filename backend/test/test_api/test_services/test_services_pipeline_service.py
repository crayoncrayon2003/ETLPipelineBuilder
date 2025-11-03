import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

import pytest
from unittest.mock import patch, MagicMock

from api.services.pipeline_service import _normalize_path, _submit_node_task, run_pipeline_from_definition
from api.schemas.pipeline import PipelineDefinition, PipelineNode, PipelineEdge


class TestNormalizePath:
    """Test class for _normalize_path function"""

    @pytest.mark.parametrize(
        "input_path,project_root,expected",
        [
            ("C:/Users/test/file.txt", "/home/user", "/mnt/c/Users/test/file.txt"),
            ("//wsl$/Ubuntu/home/user/file.txt", "/home/user", "/home/user/file.txt"),
            ("relative/file.txt", "/home/project", "/home/project/relative/file.txt"),
            ("/absolute/path/file.txt", "/home/project", "/absolute/path/file.txt"),
        ]
    )
    def test_normalize_path_various_formats(self, input_path, project_root, expected):
        result = _normalize_path(input_path, project_root)
        assert result == expected


class TestSubmitNodeTask:
    """Test class for _submit_node_task function"""

    @patch("api.services.pipeline_service.execute_step_api_task.submit")
    def test_submit_node_task_calls_submit(self, mock_submit):
        # Mock return value
        mock_future = MagicMock()
        mock_submit.return_value = mock_future

        node1 = PipelineNode(id="node1", plugin="plugin1", params={})
        node2 = PipelineNode(id="node2", plugin="plugin2", params={})
        edge = PipelineEdge(source_node_id="node1", target_node_id="node2", target_input_name="input1")

        nodes_map = {"node1": node1, "node2": node2}
        edges = [edge]
        project_root = "/home/project"

        # Submit node2 (node1 is upstream)
        future = _submit_node_task("node2", nodes_map, edges, project_root)

        # Check that submit was called with node2
        mock_submit.assert_called_with(
            step_name="node2",
            plugin_name="plugin2",
            params={},
            inputs={"input1": mock_future}  # future from node1 is passed
        )
        assert future == mock_future


class TestRunPipelineFromDefinition:
    """Test class for run_pipeline_from_definition function"""

    @patch("api.services.pipeline_service._submit_node_task")
    def test_run_pipeline_from_definition_calls_submit(self, mock_submit):
        # Mock PipelineDefinition
        node1 = PipelineNode(id="n1", plugin="p1", params={})
        node2 = PipelineNode(id="n2", plugin="p2", params={})
        edges = []
        pipeline_def = PipelineDefinition(name="TestFlow", nodes=[node1, node2], edges=edges)

        run_pipeline_from_definition(pipeline_def, "/home/project")

        # _submit_node_task is called for 2 nodes
        assert mock_submit.call_count == 2
        mock_submit.assert_any_call("n1", {"n1": node1, "n2": node2}, edges, "/home/project")
        mock_submit.assert_any_call("n2", {"n1": node1, "n2": node2}, edges, "/home/project")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
