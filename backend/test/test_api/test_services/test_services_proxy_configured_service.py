import os
import tempfile
import json
import pytest
from unittest.mock import patch, MagicMock, call

from api.services.proxy_configured_service import process_configured_request
from api.schemas.pipeline import PipelineDefinition, PipelineNode, PipelineEdge
from core.data_container.container import DataContainer, DataContainerStatus


# ======================================================================
# ヘルパー
# ======================================================================

def _make_pipeline_json(nodes, edges=None):
    """pipeline_dict を JSON 文字列で返すヘルパー"""
    return json.dumps({
        "name": "Test Pipeline",
        "nodes": nodes,
        "edges": edges or [],
    })

def _make_ok_container(metadata=None, primary_file="/mock/path/file.dat"):
    """正常終了の DataContainer モックを返すヘルパー"""
    c = MagicMock(spec=DataContainer)
    c.status = DataContainerStatus.SUCCESS
    c.metadata = metadata or {"mocked": True}
    c.get_primary_file_path.return_value = primary_file
    return c

def _make_error_container(errors=None):
    """ERROR ステータスの DataContainer モックを返すヘルパー"""
    c = MagicMock(spec=DataContainer)
    c.status = DataContainerStatus.ERROR
    # `errors or [...]` だと errors=[] (空リスト) が falsy で
    # デフォルト値に置き換わってしまうため is not None で判定する
    c.errors = errors if errors is not None else ["something went wrong"]
    return c

COMMON_ARGS = dict(
    body_bytes=b"test data",
    headers={"Authorization": "Bearer token"},
    project_root="/home/project",
    config_path="/mock/config.json",
)


# ======================================================================
# TestProcessConfiguredRequest
# ======================================================================
class TestProcessConfiguredRequest:

    @patch("api.services.proxy_configured_service.storage_adapter.read_text")
    @patch("api.services.proxy_configured_service.StepExecutor.execute_step")
    def test_process_configured_request_single_node(
        self, mock_execute_step, mock_read_text
    ):
        """単一ノードのパイプラインが正常に実行される"""
        node_id = "node1"
        plugin_name = "test_plugin"

        mock_read_text.return_value = _make_pipeline_json(
            nodes=[{"id": node_id, "plugin": plugin_name, "params": {}}]
        )
        mock_container = _make_ok_container()
        mock_execute_step.return_value = mock_container

        result = process_configured_request(**COMMON_ARGS)

        mock_read_text.assert_called_once_with(COMMON_ARGS["config_path"])
        mock_execute_step.assert_called_once()
        called_args = mock_execute_step.call_args[0][0]
        assert called_args["name"] == node_id
        assert called_args["plugin"] == plugin_name

        assert result["status"] == "ok"
        assert result["final_metadata"] == {"mocked": True}
        assert result["primary_file"] == "/mock/path/file.dat"

    @patch("api.services.proxy_configured_service.storage_adapter.read_text")
    @patch("api.services.proxy_configured_service.StepExecutor.execute_step")
    def test_process_configured_request_with_edges(
        self, mock_execute_step, mock_read_text
    ):
        """エッジありの DAG で最終ノードの結果を返す"""
        mock_read_text.return_value = _make_pipeline_json(
            nodes=[
                {"id": "node1", "plugin": "plugin1", "params": {}},
                {"id": "node2", "plugin": "plugin2", "params": {}},
            ],
            edges=[
                {"source_node_id": "node1", "target_node_id": "node2", "target_input_name": "input1"}
            ],
        )
        container_node1 = _make_ok_container(metadata={"node": "1"}, primary_file="/mock/node1.dat")
        container_node2 = _make_ok_container(metadata={"node": "2"}, primary_file="/mock/node2.dat")
        mock_execute_step.side_effect = [container_node1, container_node2]

        result = process_configured_request(**COMMON_ARGS)

        assert mock_execute_step.call_count == 2
        assert result["status"] == "ok"
        assert result["final_metadata"] == {"node": "2"}
        assert result["primary_file"] == container_node2.get_primary_file_path()

    # ------------------------------------------------------------------
    # nodes が空のとき ValueError (早期バリデーション)
    # ------------------------------------------------------------------
    @patch("api.services.proxy_configured_service.storage_adapter.read_text")
    def test_empty_nodes_raises_value_error(self, mock_read_text):
        """nodes=[] のとき ValueError を raise する"""
        mock_read_text.return_value = _make_pipeline_json(nodes=[])

        with pytest.raises(ValueError, match="nodes is empty"):
            process_configured_request(**COMMON_ARGS)

    # ------------------------------------------------------------------
    # final_container.status == ERROR のとき RuntimeError
    # ------------------------------------------------------------------
    @patch("api.services.proxy_configured_service.storage_adapter.read_text")
    @patch("api.services.proxy_configured_service.StepExecutor.execute_step")
    def test_error_status_raises_runtime_error(self, mock_execute_step, mock_read_text):
        """プラグインが ERROR ステータスを返したとき RuntimeError を raise する"""
        mock_read_text.return_value = _make_pipeline_json(
            nodes=[{"id": "node1", "plugin": "plugin1", "params": {}}]
        )
        mock_execute_step.return_value = _make_error_container(errors=["plugin failed"])

        with pytest.raises(RuntimeError, match="plugin failed"):
            process_configured_request(**COMMON_ARGS)

    @patch("api.services.proxy_configured_service.storage_adapter.read_text")
    @patch("api.services.proxy_configured_service.StepExecutor.execute_step")
    def test_error_status_with_no_errors_list(self, mock_execute_step, mock_read_text):
        """ERROR ステータスで errors リストが空のとき 'unknown error' を含むメッセージ"""
        mock_read_text.return_value = _make_pipeline_json(
            nodes=[{"id": "node1", "plugin": "plugin1", "params": {}}]
        )
        mock_execute_step.return_value = _make_error_container(errors=[])

        with pytest.raises(RuntimeError, match="unknown error"):
            process_configured_request(**COMMON_ARGS)

    # ------------------------------------------------------------------
    # execute_step が None を返したとき RuntimeError
    # ------------------------------------------------------------------
    @patch("api.services.proxy_configured_service.storage_adapter.read_text")
    @patch("api.services.proxy_configured_service.StepExecutor.execute_step")
    def test_none_result_raises_runtime_error(self, mock_execute_step, mock_read_text):
        """execute_step が None を返したとき RuntimeError を raise する"""
        mock_read_text.return_value = _make_pipeline_json(
            nodes=[{"id": "node1", "plugin": "plugin1", "params": {}}]
        )
        mock_execute_step.return_value = None

        with pytest.raises(RuntimeError, match="no result"):
            process_configured_request(**COMMON_ARGS)

    # ------------------------------------------------------------------
    # try/finally で一時ファイルが必ず削除される
    # ------------------------------------------------------------------
    @patch("api.services.proxy_configured_service.storage_adapter.read_text")
    @patch("api.services.proxy_configured_service.StepExecutor.execute_step")
    def test_temp_file_deleted_on_success(self, mock_execute_step, mock_read_text):
        """正常終了後に一時ファイルが削除される"""
        mock_read_text.return_value = _make_pipeline_json(
            nodes=[{"id": "node1", "plugin": "plugin1", "params": {}}]
        )
        mock_execute_step.return_value = _make_ok_container()

        created_paths = []
        original_mkstemp = tempfile.mkstemp

        def capturing_mkstemp(**kwargs):
            fd, path = original_mkstemp(**kwargs)
            created_paths.append(path)
            return fd, path

        with patch("api.services.proxy_configured_service.tempfile.mkstemp", side_effect=capturing_mkstemp):
            process_configured_request(**COMMON_ARGS)

        assert len(created_paths) == 1
        assert not os.path.exists(created_paths[0]), "一時ファイルが削除されていない"

    @patch("api.services.proxy_configured_service.storage_adapter.read_text")
    @patch("api.services.proxy_configured_service.StepExecutor.execute_step")
    def test_temp_file_deleted_on_exception(self, mock_execute_step, mock_read_text):
        """例外発生時も一時ファイルが削除される"""
        mock_read_text.return_value = _make_pipeline_json(
            nodes=[{"id": "node1", "plugin": "plugin1", "params": {}}]
        )
        mock_execute_step.side_effect = RuntimeError("unexpected error")

        created_paths = []
        original_mkstemp = tempfile.mkstemp

        def capturing_mkstemp(**kwargs):
            fd, path = original_mkstemp(**kwargs)
            created_paths.append(path)
            return fd, path

        with patch("api.services.proxy_configured_service.tempfile.mkstemp", side_effect=capturing_mkstemp):
            with pytest.raises(RuntimeError, match="unexpected error"):
                process_configured_request(**COMMON_ARGS)

        assert len(created_paths) == 1
        assert not os.path.exists(created_paths[0]), "例外時も一時ファイルが削除されていない"

    # ------------------------------------------------------------------
    # リクエストごとにキャッシュが独立している (並行処理安全性)
    # ------------------------------------------------------------------
    @patch("api.services.proxy_configured_service.storage_adapter.read_text")
    @patch("api.services.proxy_configured_service.StepExecutor.execute_step")
    def test_cache_is_independent_per_request(self, mock_execute_step, mock_read_text):
        """2回連続呼び出しでキャッシュが混在しない (グローバル状態がない)"""
        mock_read_text.return_value = _make_pipeline_json(
            nodes=[{"id": "node1", "plugin": "plugin1", "params": {}}]
        )
        container_first  = _make_ok_container(metadata={"req": "first"},  primary_file="/first.dat")
        container_second = _make_ok_container(metadata={"req": "second"}, primary_file="/second.dat")
        mock_execute_step.side_effect = [container_first, container_second]

        result1 = process_configured_request(**COMMON_ARGS)
        result2 = process_configured_request(**COMMON_ARGS)

        # 各リクエストが自分のコンテナ結果のみを返す
        assert result1["final_metadata"] == {"req": "first"}
        assert result2["final_metadata"] == {"req": "second"}
        assert result1["primary_file"] != result2["primary_file"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])