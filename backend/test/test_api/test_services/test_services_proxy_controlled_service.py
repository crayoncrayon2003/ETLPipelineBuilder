import os
import tempfile
import json
import pytest
from unittest.mock import patch, MagicMock
from core.data_container.container import DataContainer
from api.services.proxy_controlled_service import process_controlled_request, get_suffix_from_headers


# ======================================================================
# ヘルパー
# ======================================================================

def _make_ok_container(metadata=None, file_path="/mock/path/file.csv"):
    """正常終了の DataContainer モックを返すヘルパー"""
    c = MagicMock(spec=DataContainer)
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

        step_params = called_step["params"]
        assert step_params["input_path"].endswith(".csv")

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
    # (+) result.file_paths が空のとき RuntimeError
    # ------------------------------------------------------------------
    @patch("api.services.proxy_controlled_service.StepExecutor.execute_step")
    def test_step_returns_no_file_paths_raises_runtime_error(self, mock_execute_step):
        """execute_step が file_paths 空のコンテナを返したとき RuntimeError"""
        empty_container = MagicMock(spec=DataContainer)
        empty_container.file_paths = []  # 空
        mock_execute_step.return_value = empty_container

        with pytest.raises(RuntimeError, match="failed or returned no file paths"):
            process_controlled_request(
                body_bytes=b"data",
                payload=_make_payload(steps=[{"plugin": "plugin1", "params": {}}]),
                headers={}
            )

    @patch("api.services.proxy_controlled_service.StepExecutor.execute_step")
    def test_step_returns_none_raises_runtime_error(self, mock_execute_step):
        """execute_step が None を返したとき RuntimeError"""
        mock_execute_step.return_value = None

        with pytest.raises(RuntimeError, match="failed or returned no file paths"):
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