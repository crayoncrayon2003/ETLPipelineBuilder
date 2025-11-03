import json
import pytest
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.routers import proxy_controlled_service

class TestProxyControlledService:
    """Test class for the Controlled Service"""

    @pytest.fixture
    def client(self):
        """FastAPI test client"""
        app = FastAPI()
        app.include_router(proxy_controlled_service.router)
        return TestClient(app)

    @pytest.fixture
    def sample_request_body(self):
        return {"key": "value", "data": "test"}

    @patch("api.routers.proxy_controlled_service.process_controlled_request")
    def test_controlled_service_success(self, mock_process, client, sample_request_body):
        """Basic success case"""
        mock_result = {"status": "success", "data": "processed"}
        mock_process.return_value = mock_result

        response = client.post("/proxy/controlled_service", json=sample_request_body)

        assert response.status_code == 200
        assert response.json() == mock_result

        mock_process.assert_called_once()
        call_args = mock_process.call_args
        body_bytes = call_args.kwargs.get("body_bytes")
        payload = call_args.kwargs.get("payload")
        headers = call_args.kwargs.get("headers")

        assert isinstance(body_bytes, bytes)
        assert payload == {"steps": [], "storage": {}}
        assert isinstance(headers, dict)

    @patch("api.routers.proxy_controlled_service.process_controlled_request")
    def test_controlled_service_with_steps_and_storage_dir(self, mock_process, client, sample_request_body):
        """When steps_json and storage_dir are specified"""
        mock_result = {"status": "success"}
        mock_process.return_value = mock_result

        steps = [{"name": "step1"}, {"name": "step2"}]
        steps_json = json.dumps(steps)
        storage_dir = "/tmp/storage_test"

        response = client.post(
            f"/proxy/controlled_service?steps_json={steps_json}&storage_dir={storage_dir}",
            json=sample_request_body
        )

        assert response.status_code == 200
        assert response.json() == mock_result
        mock_process.assert_called_once()

        if mock_process.call_args:
            call_args = mock_process.call_args
            payload = call_args.kwargs.get("payload")

            assert payload["steps"] == steps
            assert payload["storage"]["dir"] == storage_dir

    @patch("api.routers.proxy_controlled_service.process_controlled_request")
    def test_controlled_service_empty_body(self, mock_process, client):
        """Empty request body"""
        mock_process.return_value = {"status": "success"}

        response = client.post("/proxy/controlled_service", json={})

        assert response.status_code == 200
        mock_process.assert_called_once()

        if mock_process.call_args:
            call_args = mock_process.call_args
            body_bytes = call_args.kwargs.get("body_bytes")

            assert isinstance(body_bytes, bytes)

    def test_controlled_service_invalid_steps_json(self, client, sample_request_body):
        """Invalid JSON in steps_json"""
        response = client.post(
            "/proxy/controlled_service?steps_json={invalid_json",
            json=sample_request_body
        )
        assert response.status_code == 400
        assert "Invalid steps_json" in response.json()["detail"]

    @patch("api.routers.proxy_controlled_service.process_controlled_request")
    def test_controlled_service_exception_handling(self, mock_process, client, sample_request_body):
        """Handling exceptions"""
        mock_process.side_effect = Exception("Processing error")

        response = client.post("/proxy/controlled_service", json=sample_request_body)

        assert response.status_code == 500
        assert "Processing error" in response.json()["detail"]

    @patch("api.routers.proxy_controlled_service.process_controlled_request")
    def test_controlled_service_large_body(self, mock_process, client):
        """Large request body"""
        large_body = {"data": "x" * 10000}
        mock_process.return_value = {"status": "success"}

        response = client.post("/proxy/controlled_service", json=large_body)

        assert response.status_code == 200
        mock_process.assert_called_once()

        if mock_process.call_args:
            call_args = mock_process.call_args
            body_bytes = call_args.kwargs.get("body_bytes")

            assert isinstance(body_bytes, bytes)

    @patch("api.routers.proxy_controlled_service.process_controlled_request")
    def test_controlled_service_headers_passed(self, mock_process, client, sample_request_body):
        """Test that headers are passed correctly"""
        mock_process.return_value = {"status": "success"}
        custom_headers = {"X-Test-Header": "value", "Authorization": "Bearer token"}

        response = client.post(
            "/proxy/controlled_service",
            json=sample_request_body,
            headers=custom_headers
        )

        assert response.status_code == 200
        if mock_process.call_args:
            call_args = mock_process.call_args
            headers = call_args.kwargs.get("headers")

            assert "x-test-header" in {k.lower() for k in headers.keys()}

    @patch("api.routers.proxy_controlled_service.process_controlled_request")
    def test_controlled_service_without_optional_params(self, mock_process, client, sample_request_body):
        """Without optional parameters"""
        mock_process.return_value = {"status": "success"}

        response = client.post("/proxy/controlled_service", json=sample_request_body)

        assert response.status_code == 200
        if mock_process.call_args:
            call_args = mock_process.call_args
            payload = call_args.kwargs.get("payload")

            assert payload["steps"] == []
            assert payload["storage"] == {}

    @patch("api.routers.proxy_controlled_service.process_controlled_request")
    def test_controlled_service_with_special_characters_in_steps(self, mock_process, client, sample_request_body):
        """When steps contain special characters"""
        steps = [{"name": "ステップ①"}, {"name": "step@2"}]
        steps_json = json.dumps(steps)
        mock_process.return_value = {"status": "success"}

        response = client.post(f"/proxy/controlled_service?steps_json={steps_json}", json=sample_request_body)

        assert response.status_code == 200
        if mock_process.call_args:
            call_args = mock_process.call_args
            payload = call_args.kwargs.get("payload")

            assert payload["steps"] == steps

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
