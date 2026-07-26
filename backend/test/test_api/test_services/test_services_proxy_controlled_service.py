import os
import tempfile
import json
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from core.data_container.container import DataContainer, DataContainerStatus
from api.services.proxy_controlled_service import process_controlled_request, get_suffix_from_headers


# ======================================================================
# ヘルパー
# ======================================================================

def _make_ok_container(metadata=None, file_path="/mock/path/file.csv"):
    """正常終了の DataContainer モックを返すヘルパー"""
    c = MagicMock(spec=DataContainer)
    c.status = DataContainerStatus.SUCCESS  # status を明示的に設定
    c.metadata = metadata or {}
    c.file_paths = [file_path]
    c.get_primary_file_path.return_value = file_path
    return c

def _make_payload(steps, storage=None):
    return {"steps": steps, "storage": storage or {}}


# ======================================================================
# TestGetSuffixFromHeaders
# ======================================================================
class TestGetSuffixFromHeaders:

    @pytest.mark.parametrize(
        "content_type,expected_suffix",
        [
            ("text/csv",                ".csv"),
            ("application/json",        ".json"),
            ("application/parquet",     ".parquet"),
            ("application/octet-stream", ".bin"),
            ("text/plain",              ".txt"),
            ("text/csv; charset=utf-8", ".csv"),
            (" Application/JSON ; Charset=UTF-8 ", ".json"),
            ("unknown/type",            ".bin"),
        ]
    )
    def test_get_suffix_from_headers(self, content_type, expected_suffix):
        headers = {"content-type": content_type}
        assert get_suffix_from_headers(headers) == expected_suffix


# ======================================================================
# TestProcessControlledRequest
# ======================================================================
class TestProcessControlledRequest:

    @patch("api.services.proxy_controlled_service.StepExecutor.execute_step")
    def test_single_step_request(self, mock_execute_step):
        """単一ステップが正常に実行される"""
        mock_container = _make_ok_container(metadata={"step": "1"})
        mock_execute_step.return_value = mock_container

        body_bytes = b"test data"
        headers = {"content-type": "text/csv"}
        payload = _make_payload(steps=[{"plugin": "csv_plugin", "params": {}}])

        result = process_controlled_request(body_bytes, payload, headers)

        mock_execute_step.assert_called_once()
        called_step = mock_execute_step.call_args[0][0]
        assert called_step["plugin"] == "csv_plugin"
        assert called_step["name"].startswith("controlled_step_0_csv_plugin")

        # input_path の強制注入は廃止済み。params は空のまま渡される。
        step_params = called_step["params"]
        assert "input_path" not in step_params

        assert result["status"] == "ok"
        assert result["final_metadata"] == {"step": "1"}
        assert result["primary_file"] == "/mock/path/file.csv"

    @patch("api.services.proxy_controlled_service.StepExecutor.execute_step")
    def test_multiple_steps_request(self, mock_execute_step):
        """複数ステップで最終コンテナの結果が返る"""
        container1 = _make_ok_container(metadata={"step": "1"}, file_path="/mock/path/file1.csv")
        container2 = _make_ok_container(metadata={"step": "2"}, file_path="/mock/path/file2.csv")
        mock_execute_step.side_effect = [container1, container2]

        body_bytes = b"test data"
        headers = {"content-type": "text/csv"}
        payload = _make_payload(steps=[
            {"plugin": "csv_plugin_1", "params": {}},
            {"plugin": "csv_plugin_2", "params": {}},
        ])

        result = process_controlled_request(body_bytes, payload, headers)

        assert mock_execute_step.call_count == 2
        assert result["status"] == "ok"
        assert result["final_metadata"] == {"step": "2"}
        assert result["primary_file"] == "/mock/path/file2.csv"

    # ------------------------------------------------------------------
    # steps が空のとき ValueError
    # ------------------------------------------------------------------
    def test_empty_steps_raises_value_error(self):
        """steps=[] のとき ValueError を raise する"""
        with pytest.raises(ValueError, match="steps is empty"):
            process_controlled_request(
                body_bytes=b"data",
                payload=_make_payload(steps=[]),
                headers={}
            )

    # ------------------------------------------------------------------
    # result.status == ERROR のとき RuntimeError
    # ------------------------------------------------------------------
    @patch("api.services.proxy_controlled_service.StepExecutor.execute_step")
    def test_step_returns_error_status_raises_runtime_error(self, mock_execute_step):
        """execute_step が ERROR ステータスを返したとき RuntimeError"""
        error_container = MagicMock(spec=DataContainer)
        error_container.status = DataContainerStatus.ERROR
        error_container.errors = ["something went wrong"]
        mock_execute_step.return_value = error_container

        with pytest.raises(RuntimeError, match="something went wrong"):
            process_controlled_request(
                body_bytes=b"data",
                payload=_make_payload(steps=[{"plugin": "plugin1", "params": {}}]),
                headers={}
            )

    # ------------------------------------------------------------------
    # 中間 result.file_paths が空でも DataContainer.data で後続へ渡せる
    # ------------------------------------------------------------------
    @patch("api.services.proxy_controlled_service.StepExecutor.execute_step")
    def test_data_only_intermediate_container_is_passed_to_next_step(
        self, mock_execute_step
    ):
        """file_pathsなしの中間DataContainerを後続ステップへ渡せる"""
        data_only_container = DataContainer(status=DataContainerStatus.SUCCESS)
        data_only_container.data = pd.DataFrame({"in_memory": ["value"]})
        final_container = _make_ok_container(file_path="/mock/final.csv")
        mock_execute_step.side_effect = [data_only_container, final_container]

        result = process_controlled_request(
            body_bytes=b"data",
            payload=_make_payload(
                steps=[
                    {"plugin": "data_producer", "params": {}},
                    {"plugin": "data_consumer", "params": {}},
                ]
            ),
            headers={},
        )

        second_inputs = mock_execute_step.call_args_list[1].kwargs["inputs"]
        assert second_inputs["input_data"] is data_only_container
        assert result["primary_file"] == "/mock/final.csv"

    @patch("api.services.proxy_controlled_service.StepExecutor.execute_step")
    def test_final_result_still_requires_file_path(self, mock_execute_step):
        """既存レスポンスIFのprimary_fileを維持するため最終結果にはパスが必要"""
        final_container = DataContainer(status=DataContainerStatus.SUCCESS)
        final_container.data = pd.DataFrame({"in_memory": ["value"]})
        mock_execute_step.return_value = final_container

        with pytest.raises(RuntimeError, match="Final container has no file paths"):
            process_controlled_request(
                body_bytes=b"data",
                payload=_make_payload(
                    steps=[{"plugin": "data_producer", "params": {}}]
                ),
                headers={},
            )

    # ------------------------------------------------------------------
    # execute_step が None を返したとき RuntimeError
    # ------------------------------------------------------------------
    @patch("api.services.proxy_controlled_service.StepExecutor.execute_step")
    def test_step_returns_none_raises_runtime_error(self, mock_execute_step):
        """execute_step が None を返したとき RuntimeError"""
        mock_execute_step.return_value = None

        with pytest.raises(RuntimeError, match="returned no result"):
            process_controlled_request(
                body_bytes=b"data",
                payload=_make_payload(steps=[{"plugin": "plugin1", "params": {}}]),
                headers={}
            )

    # ------------------------------------------------------------------
    # try/finally で一時ファイルが必ず削除される
    # ------------------------------------------------------------------
    @patch("api.services.proxy_controlled_service.StepExecutor.execute_step")
    def test_temp_file_deleted_on_success(self, mock_execute_step):
        """正常終了後に一時ファイルが削除される"""
        mock_execute_step.return_value = _make_ok_container()

        created_paths = []
        original_mkstemp = tempfile.mkstemp

        def capturing_mkstemp(**kwargs):
            fd, path = original_mkstemp(**kwargs)
            created_paths.append(path)
            return fd, path

        with patch("api.services.proxy_controlled_service.tempfile.mkstemp",
                   side_effect=capturing_mkstemp):
            process_controlled_request(
                body_bytes=b"data",
                payload=_make_payload(steps=[{"plugin": "plugin1", "params": {}}]),
                headers={"content-type": "text/csv"}
            )

        assert len(created_paths) == 1
        assert not os.path.exists(created_paths[0]), "一時ファイルが削除されていない"

    @patch("api.services.proxy_controlled_service.StepExecutor.execute_step")
    def test_temp_file_deleted_on_exception(self, mock_execute_step):
        """例外発生時も一時ファイルが削除される"""
        mock_execute_step.side_effect = RuntimeError("unexpected error")

        created_paths = []
        original_mkstemp = tempfile.mkstemp

        def capturing_mkstemp(**kwargs):
            fd, path = original_mkstemp(**kwargs)
            created_paths.append(path)
            return fd, path

        with patch("api.services.proxy_controlled_service.tempfile.mkstemp",
                   side_effect=capturing_mkstemp):
            with pytest.raises(RuntimeError, match="unexpected error"):
                process_controlled_request(
                    body_bytes=b"data",
                    payload=_make_payload(steps=[{"plugin": "plugin1", "params": {}}]),
                    headers={}
                )

        assert len(created_paths) == 1
        assert not os.path.exists(created_paths[0]), "例外時も一時ファイルが削除されていない"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
