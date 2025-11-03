import os
import json
import pytest
from unittest.mock import patch, MagicMock
from core.data_container.container import DataContainer
from api.services.proxy_controlled_service import process_controlled_request, get_suffix_from_headers

class TestProcessControlledRequest:
    """Test class for process_controlled_request function"""

    @patch("api.services.proxy_controlled_service.StepExecutor.execute_step")
    def test_single_step_request(self, mock_execute_step):
        # Mock return value
        mock_container = MagicMock(spec=DataContainer)
        mock_container.metadata = {"step": "1"}
        mock_container.file_paths = ["/mock/path/file.csv"]
        mock_container.get_primary_file_path.return_value = "/mock/path/file.csv"
        mock_execute_step.return_value = mock_container

        body_bytes = b"test data"
        headers = {"content-type": "text/csv"}
        payload = {
            "steps": [{"plugin": "csv_plugin", "params": {}}],
            "storage": {}
        }

        result = process_controlled_request(body_bytes, payload, headers)

        # Check that execute_step was called
        mock_execute_step.assert_called_once()
        called_step = mock_execute_step.call_args[0][0]
        assert called_step["plugin"] == "csv_plugin"
        assert called_step["name"].startswith("controlled_step_0_csv_plugin")

        # Check that MIME type suffix is reflected
        step_params = called_step["params"]
        assert step_params["input_path"].endswith(".csv")

        # Assert return value
        assert result["status"] == "ok"
        assert result["final_metadata"] == {"step": "1"}
        assert result["primary_file"] == "/mock/path/file.csv"

    @patch("api.services.proxy_controlled_service.StepExecutor.execute_step")
    def test_multiple_steps_request(self, mock_execute_step):
        # Return containers for step 1 and step 2
        container1 = MagicMock(spec=DataContainer)
        container1.metadata = {"step": "1"}
        container1.file_paths = ["/mock/path/file1.csv"]
        container1.get_primary_file_path.return_value = "/mock/path/file1.csv"

        container2 = MagicMock(spec=DataContainer)
        container2.metadata = {"step": "2"}
        container2.file_paths = ["/mock/path/file2.csv"]
        container2.get_primary_file_path.return_value = "/mock/path/file2.csv"

        mock_execute_step.side_effect = [container1, container2]

        body_bytes = b"test data"
        headers = {"content-type": "text/csv"}
        payload = {
            "steps": [
                {"plugin": "csv_plugin_1", "params": {}},
                {"plugin": "csv_plugin_2", "params": {}}
            ],
            "storage": {}
        }

        result = process_controlled_request(body_bytes, payload, headers)

        # Check that execute_step was called twice
        assert mock_execute_step.call_count == 2

        # Return container from the final step
        assert result["status"] == "ok"
        assert result["final_metadata"] == {"step": "2"}
        assert result["primary_file"] == "/mock/path/file2.csv"

    @pytest.mark.parametrize(
        "content_type,expected_suffix",
        [
            ("text/csv", ".csv"),
            ("application/json", ".json"),
            ("application/parquet", ".parquet"),
            ("application/octet-stream", ".bin"),
            ("text/plain", ".txt"),
            ("unknown/type", ".bin")
        ]
    )
    def test_get_suffix_from_headers(self, content_type, expected_suffix):
        headers = {"content-type": content_type}
        suffix = get_suffix_from_headers(headers)
        assert suffix == expected_suffix

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
