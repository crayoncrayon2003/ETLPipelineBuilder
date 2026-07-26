import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

import pytest
from unittest.mock import patch, MagicMock, ANY, call

from api.services.pipeline_service import (
    _normalize_path,
    _submit_node_task,
    execute_step_api_task,
    run_pipeline_from_definition,
)
from api.schemas.pipeline import PipelineDefinition, PipelineNode, PipelineEdge
from core.data_container.container import DataContainer, DataContainerStatus


class TestNormalizePath:
    """Test class for _normalize_path function"""

    @pytest.mark.parametrize(
        "input_path,project_root,expected",
        [
            ("C:/Users/test/file.txt", "/home/user", "/mnt/c/Users/test/file.txt"),
            ("//wsl$/Ubuntu/home/user/file.txt", "/home/user", "/home/user/file.txt"),
            ("relative/file.txt", "/home/project", "/home/project/relative/file.txt"),
            ("/absolute/path/file.txt", "/home/project", "/absolute/path/file.txt"),
            ("s3://bucket/file.csv", "/home/project", "s3://bucket/file.csv"),
            ("memory://run/file.csv", "/home/project", "memory://run/file.csv"),
            ("https://example.test/file.csv", "/home/project", "https://example.test/file.csv"),
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
        edge = PipelineEdge(source_node_id="node1", target_node_id="node2")

        nodes_map = {"node1": node1, "node2": node2}
        edges = [edge]
        project_root = "/home/project"

        node_results_cache = {}
        future = _submit_node_task("node2", nodes_map, edges, project_root, node_results_cache)

        mock_submit.assert_called_with(
            step_name="node2",
            plugin_name="plugin2",
            params={},
            inputs={"input_data": mock_future}  # target_input_name廃止により固定キー
        )
        assert future == mock_future


class TestExecuteStepApiTask:

    @patch("api.services.pipeline_service.StepExecutor.execute_step")
    def test_error_container_is_raised_as_task_failure(self, mock_execute_step):
        result = DataContainer(status=DataContainerStatus.ERROR)
        result.add_error("plugin failed")
        mock_execute_step.return_value = result

        with pytest.raises(RuntimeError, match="plugin failed"):
            execute_step_api_task.fn(
                step_name="node1",
                plugin_name="plugin1",
                params={},
                inputs={},
            )


class TestRunPipelineFromDefinition:
    """Test class for run_pipeline_from_definition function"""

    @patch("api.services.pipeline_service._submit_node_task")
    @patch("api.services.pipeline_service.flow")
    def test_run_pipeline_from_definition_calls_submit(self, mock_flow, mock_submit):
        # Unit-test the graph construction without starting Prefect's runtime.
        mock_flow.side_effect = lambda **kwargs: lambda func: func
        node1 = PipelineNode(id="n1", plugin="p1", params={})
        node2 = PipelineNode(id="n2", plugin="p2", params={})
        edges = []
        pipeline_def = PipelineDefinition(name="TestFlow", nodes=[node1, node2], edges=edges)

        run_pipeline_from_definition(pipeline_def, "/home/project")

        # _submit_node_task は node_results_cache を第5引数として受け取る。
        # node_results_cache は dynamic_etl_flow 内でローカル生成されるため
        # テストから直接参照できない。
        # ANY でマッチさせて「正しいノード・引数で呼ばれたか」を確認する。
        assert mock_submit.call_count == 2

        # node_id / nodes_map / edges / project_root の4引数を確認し
        # node_results_cache (第5引数) は ANY で受け入れる
        expected_nodes_map = {"n1": node1, "n2": node2}
        mock_submit.assert_any_call("n1", expected_nodes_map, edges, "/home/project", ANY)
        mock_submit.assert_any_call("n2", expected_nodes_map, edges, "/home/project", ANY)

        # node_results_cache が dict であることを確認
        for actual_call in mock_submit.call_args_list:
            cache_arg = actual_call.args[4] if len(actual_call.args) > 4 else actual_call.kwargs.get("node_results_cache")
            assert isinstance(cache_arg, dict), "node_results_cache は dict でなければならない"

        # 全呼び出しで同一の node_results_cache インスタンスが渡されることを確認
        # (同一フロー内で共有されるキャッシュであることの保証)
        caches = [c.args[4] for c in mock_submit.call_args_list]
        assert caches[0] is caches[1], "全ノードで同一の node_results_cache が共有される"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
