from fastapi import APIRouter
from typing import Dict, Any

from api.schemas.pipeline import PipelineDefinition

router = APIRouter(
    prefix="/schemas",
    tags=["Schemas"],
)

@router.get("/pipeline-definition", response_model=Dict[str, Any])
async def get_pipeline_definition_schema():
    """
    Returns the JSON Schema for the `PipelineDefinition` model.

    The frontend can use this schema to understand the expected structure
    for saving and submitting pipeline definitions.
    """
    # FastAPI/Pydantic v2 provides a method to get the JSON schema representation
    # of a Pydantic model.
    return PipelineDefinition.model_json_schema()