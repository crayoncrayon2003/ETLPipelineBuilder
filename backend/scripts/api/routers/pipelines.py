from fastapi import APIRouter, HTTPException, BackgroundTasks
from pathlib import Path

from api.schemas.pipeline import PipelineDefinition
from api.services import pipeline_service

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