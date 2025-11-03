import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.routers import schemas
from api.schemas.pipeline import PipelineDefinition

class TestSchemasRouter:
    """Test class for the Schemas Router"""

    @pytest.fixture
    def client(self):
        """FastAPI test client"""
        app = FastAPI()
        app.include_router(schemas.router)
        return TestClient(app)

    def test_get_pipeline_definition_schema(self, client):
        """Verify that the JSON Schema for PipelineDefinition can be retrieved"""
        response = client.get("/schemas/pipeline-definition")

        # Status code
        assert response.status_code == 200

        data = response.json()

        # Check basic structure of JSON Schema
        assert isinstance(data, dict)
        assert "title" in data
        assert "type" in data
        assert "properties" in data

        # Verify title directly from returned JSON Schema
        expected_title = data.get("title", "PipelineDefinition")
        assert expected_title == "PipelineDefinition"

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
