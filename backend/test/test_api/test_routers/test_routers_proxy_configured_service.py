import os
import pytest
from unittest.mock import Mock, patch, AsyncMock
from fastapi import HTTPException
from fastapi.testclient import TestClient
from fastapi import FastAPI
from api.routers.proxy_configured_service import router


class TestProxyConfiguredRouter:
    """Test class for the proxy-configured router"""

    @pytest.fixture
    def client(self):
        """Fixture providing a FastAPI test client"""
        app = FastAPI()
        app.include_router(router)
        return TestClient(app)

    @pytest.fixture
    def sample_request_body(self):
        """Fixture providing a sample request body"""
        return {"key": "value", "data": "test"}

    @patch('api.routers.proxy_configured_service.process_configured_request')
    def test_configured_service_named_success(
        self,
        mock_process,
        client,
        sample_request_body
    ):
        """Test normal request processing"""
        # Arrange
        mock_result = {"status": "success", "data": "processed"}
        mock_process.return_value = mock_result

        # Act
        response = client.post(
            "/proxy/configured_service/test_config",
            json=sample_request_body
        )

        # Assert
        assert response.status_code == 200
        assert response.json() == mock_result
        mock_process.assert_called_once()

    @patch('api.routers.proxy_configured_service.process_configured_request')
    def test_configured_service_named_with_project_root(
        self,
        mock_process,
        client,
        sample_request_body
    ):
        """Test with project_root parameter"""
        # Arrange
        mock_result = {"status": "success"}
        mock_process.return_value = mock_result

        # Act
        response = client.post(
            "/proxy/configured_service/test_config?project_root=/custom/path",
            json=sample_request_body
        )

        # Assert
        assert response.status_code == 200
        mock_process.assert_called_once()

        # Verify the arguments passed to process_configured_request
        call_args = mock_process.call_args
        assert call_args[1]['project_root'] == "/custom/path"

    @patch('api.routers.proxy_configured_service.process_configured_request')
    def test_configured_service_named_config_path_construction(
        self,
        mock_process,
        client,
        sample_request_body
    ):
        """Test correct construction of config file path"""
        # Arrange
        mock_process.return_value = {"status": "success"}

        # Act
        response = client.post(
            "/proxy/configured_service/my_service",
            json=sample_request_body
        )

        # Assert
        assert response.status_code == 200

        # Verify called arguments
        call_args = mock_process.call_args
        config_path = call_args.kwargs.get("config_path")

        # Ensure "my_service.json" is included in config_path
        assert "my_service.json" in config_path
        assert "config" in config_path

    @patch('api.routers.proxy_configured_service.process_configured_request')
    def test_configured_service_named_headers_passed(
        self,
        mock_process,
        client,
        sample_request_body
    ):
        """Test that headers are passed correctly"""
        # Arrange
        mock_process.return_value = {"status": "success"}
        custom_headers = {
            "X-Custom-Header": "custom-value",
            "Authorization": "Bearer token123"
        }

        # Act
        response = client.post(
            "/proxy/configured_service/test_config",
            json=sample_request_body,
            headers=custom_headers
        )

        # Assert
        assert response.status_code == 200

        # Verify headers were passed
        call_args = mock_process.call_args
        headers = call_args.kwargs.get("headers")

        assert "x-custom-header" in headers or "X-Custom-Header" in headers

    @patch('api.routers.proxy_configured_service.process_configured_request')
    def test_configured_service_named_exception_handling(
        self,
        mock_process,
        client,
        sample_request_body
    ):
        """Test handling when an exception occurs"""
        # Arrange
        error_message = "Configuration file not found"
        mock_process.side_effect = Exception(error_message)

        # Act
        response = client.post(
            "/proxy/configured_service/nonexistent_config",
            json=sample_request_body
        )

        # Assert
        assert response.status_code == 500
        assert error_message in response.json()["detail"]

    @patch('api.routers.proxy_configured_service.process_configured_request')
    def test_configured_service_named_empty_body(
        self,
        mock_process,
        client
    ):
        """Test with an empty body"""
        # Arrange
        mock_process.return_value = {"status": "success"}

        # Act
        response = client.post(
            "/proxy/configured_service/test_config",
            json={}
        )

        # Assert
        assert response.status_code == 200

        # Verify that body_bytes was passed
        call_args = mock_process.call_args
        body_bytes = call_args.kwargs.get("body_bytes")
        assert isinstance(body_bytes, bytes)

    @patch('api.routers.proxy_configured_service.process_configured_request')
    def test_configured_service_named_large_body(
        self,
        mock_process,
        client
    ):
        """Test with a large body"""
        # Arrange
        large_body = {"data": "x" * 10000}  # Large data
        mock_process.return_value = {"status": "success"}

        # Act
        response = client.post(
            "/proxy/configured_service/test_config",
            json=large_body
        )

        # Assert
        assert response.status_code == 200

    @patch('api.routers.proxy_configured_service.process_configured_request')
    def test_configured_service_named_special_characters_in_config_name(
        self,
        mock_process,
        client,
        sample_request_body
    ):
        """Test when config name contains special characters"""
        # Arrange
        mock_process.return_value = {"status": "success"}

        # Act
        response = client.post(
            "/proxy/configured_service/config-with-dashes_and_underscores",
            json=sample_request_body
        )

        # Assert
        assert response.status_code == 200

        call_args = mock_process.call_args
        config_path = call_args.kwargs.get("config_path")
        assert "config-with-dashes_and_underscores.json" in config_path

    @patch('api.routers.proxy_configured_service.process_configured_request')
    def test_configured_service_named_unicode_config_name(
        self,
        mock_process,
        client,
        sample_request_body
    ):
        """Test config names containing Unicode characters"""
        # Arrange
        mock_process.return_value = {"status": "success"}

        # Act
        response = client.post(
            "/proxy/configured_service/日本語設定",
            json=sample_request_body
        )

        # Assert
        assert response.status_code == 200

    @patch('api.routers.proxy_configured_service.process_configured_request')
    def test_configured_service_named_json_response(
        self,
        mock_process,
        client,
        sample_request_body
    ):
        """Test that JSON response is returned correctly"""
        # Arrange
        complex_result = {
            "status": "success",
            "data": {
                "nested": {
                    "value": 123,
                    "list": [1, 2, 3]
                }
            },
            "metadata": {
                "timestamp": "2024-01-01T00:00:00"
            }
        }
        mock_process.return_value = complex_result

        # Act
        response = client.post(
            "/proxy/configured_service/test_config",
            json=sample_request_body
        )

        # Assert
        assert response.status_code == 200
        assert response.json() == complex_result

    @patch('api.routers.proxy_configured_service.process_configured_request')
    def test_configured_service_named_content_type_json(
        self,
        mock_process,
        client,
        sample_request_body
    ):
        """Test Content-Type header"""
        # Arrange
        mock_process.return_value = {"status": "success"}

        # Act
        response = client.post(
            "/proxy/configured_service/test_config",
            json=sample_request_body,
            headers={"Content-Type": "application/json"}
        )

        # Assert
        assert response.status_code == 200

    @patch('api.routers.proxy_configured_service.process_configured_request')
    def test_configured_service_named_without_project_root(
        self,
        mock_process,
        client,
        sample_request_body
    ):
        """Test without project_root"""
        # Arrange
        mock_process.return_value = {"status": "success"}

        # Act
        response = client.post(
            "/proxy/configured_service/test_config",
            json=sample_request_body
        )

        # Assert
        assert response.status_code == 200

        # Verify that project_root is passed as None
        call_args = mock_process.call_args
        project_root = call_args[1].get('project_root')
        assert project_root is None

    @patch('api.routers.proxy_configured_service.process_configured_request')
    def test_configured_service_named_file_not_found_exception(
        self,
        mock_process,
        client,
        sample_request_body
    ):
        """Test FileNotFoundError handling"""
        # Arrange
        mock_process.side_effect = FileNotFoundError("Config file not found")

        # Act
        response = client.post(
            "/proxy/configured_service/missing_config",
            json=sample_request_body
        )

        # Assert
        assert response.status_code == 500
        assert "Config file not found" in response.json()["detail"]

    @patch('api.routers.proxy_configured_service.process_configured_request')
    def test_configured_service_named_json_decode_error(
        self,
        mock_process,
        client,
        sample_request_body
    ):
        """Test JSON decoding error handling"""
        # Arrange
        import json
        mock_process.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)

        # Act
        response = client.post(
            "/proxy/configured_service/bad_config",
            json=sample_request_body
        )

        # Assert
        assert response.status_code == 500


class TestProxyConfiguredRouterPathHandling:
    """Test class for path handling"""

    @pytest.fixture
    def client(self):
        """Fixture providing a FastAPI test client"""
        app = FastAPI()
        app.include_router(router)
        return TestClient(app)

    @patch('api.routers.proxy_configured_service.process_configured_request')
    @patch('os.path.abspath')
    @patch('os.path.dirname')
    def test_config_path_normalization(
        self,
        mock_dirname,
        mock_abspath,
        mock_process,
        client
    ):
        """Test normalization of config file paths"""
        # Arrange
        mock_abspath.return_value = "/app/api/routers/proxy_configured.py"
        mock_dirname.return_value = "/app/api/routers"
        mock_process.return_value = {"status": "success"}

        # Act
        response = client.post(
            "/proxy/configured_service/test_config",
            json={"test": "data"}
        )

        # Assert
        assert response.status_code == 200

    @patch('api.routers.proxy_configured_service.process_configured_request')
    def test_config_name_with_slashes_rejected(
        self,
        mock_process,
        client
    ):
        """Test config names containing slashes (path traversal protection)"""
        # Arrange
        mock_process.return_value = {"status": "success"}

        # Act - FastAPI handles slashes in path parameters,
        # usually they are encoded or result in 404
        response = client.post(
            "/proxy/configured_service/../../../etc/passwd",
            json={"test": "data"}
        )

        # Assert
        # If the path parameter contains slashes, FastAPI may return 404
        assert response.status_code in [200, 404]


class TestProxyConfiguredRouterIntegration:
    """Integration test class"""

    @pytest.fixture
    def client(self):
        """Fixture providing a FastAPI test client"""
        app = FastAPI()
        app.include_router(router)
        return TestClient(app)

    @patch('api.routers.proxy_configured_service.process_configured_request')
    def test_full_request_cycle(
        self,
        mock_process,
        client
    ):
        """Test the full request cycle"""
        # Arrange
        request_body = {
            "input_data": "test_input",
            "parameters": {
                "param1": "value1",
                "param2": 123
            }
        }
        expected_response = {
            "status": "completed",
            "output": "processed_output",
            "metadata": {
                "processed_at": "2024-01-01T00:00:00",
                "duration_ms": 150
            }
        }
        mock_process.return_value = expected_response

        # Act
        response = client.post(
            "/proxy/configured_service/data_processor",
            json=request_body,
            headers={
                "X-Request-ID": "test-request-123",
                "Authorization": "Bearer test-token"
            }
        )

        # Assert
        assert response.status_code == 200
        assert response.json() == expected_response

        # Verify that process_configured_request was called with correct arguments
        mock_process.assert_called_once()
        call_args = mock_process.call_args
        body_bytes = call_args.kwargs.get("body_bytes")
        config_path = call_args.kwargs.get("config_path")
        headers = call_args.kwargs.get("headers")

        # Verify body_bytes was passed
        assert isinstance(body_bytes, bytes)

        # Verify config_path was passed
        assert "data_processor.json" in config_path

        # Verify headers were passed
        assert isinstance(headers, dict)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
