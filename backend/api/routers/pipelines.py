# backend/api/routers/pipelines.py

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pathlib import Path

# Import the specific Pydantic model directly from its source file.
from backend.api.schemas.pipeline import PipelineDefinition
from backend.api.services import pipeline_service

# The project root is now determined and passed in from the service layer,
# making the router more self-contained.
project_root = Path(__file__).resolve().parents[3]

router = APIRouter(
    prefix="/pipelines",
    tags=["Pipelines"],
)

@router.post("/run", status_code=202)
async def run_pipeline(
    pipeline_def: PipelineDefinition,
    background_tasks: BackgroundTasks
):
    """Schedules a pipeline for execution based on a received definition."""
    try:
        print(f"Received request to run pipeline: {pipeline_def.name}")
        background_tasks.add_task(
            pipeline_service.run_pipeline_from_definition,
            pipeline_def,
            project_root
        )
        return {"message": "Pipeline execution scheduled.", "pipeline_name": pipeline_def.name}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to schedule pipeline: {e}")