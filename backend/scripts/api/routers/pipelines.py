# backend/api/routers/pipelines.py

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pathlib import Path
import json

# Import the specific Pydantic model directly from its source file.
from api.schemas.pipeline import PipelineDefinition
from api.services.pipeline_service import run_pipeline_from_definition

# The project root is determined here to resolve paths for saving definition files.
# It assumes this file is at backend/scripts/api/routers/pipelines.py
project_root = Path(__file__).resolve().parents[3]

# Create a new router instance for pipeline-related endpoints.
router = APIRouter(
    prefix="/pipelines",
    tags=["Pipelines"],
)

# Define a standardized directory for storing pipeline JSON definitions.
PIPELINE_DEFINITIONS_DIR = project_root / "data" / "pipeline_definitions"
PIPELINE_DEFINITIONS_DIR.mkdir(parents=True, exist_ok=True)


def _save_pipeline_definition(pipeline_def: PipelineDefinition) -> Path:
    """
    Helper function to save a pipeline definition to a standardized JSON file.
    This file can be used for batch execution or auditing.
    """
    # Create a filename-safe version of the pipeline name.
    safe_name = "".join(c if c.isalnum() else "_" for c in pipeline_def.name)
    file_path = PIPELINE_DEFINITIONS_DIR / f"{safe_name}.json"

    # Use the Pydantic model's `model_dump_json` for clean serialization.
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(pipeline_def.model_dump_json(indent=2))

    print(f"Pipeline definition saved to: {file_path}")
    return file_path


@router.post("/run", status_code=202)
async def run_pipeline(
    pipeline_def: PipelineDefinition,
    background_tasks: BackgroundTasks
):
    """
    Receives a pipeline definition and triggers an IMMEDIATE one-time run.
    """
    try:
        print(f"Received request for an immediate run of pipeline: {pipeline_def.name}")

        # Delegate the execution to the service layer, running it in the background.
        background_tasks.add_task(
            run_pipeline_from_definition,
            pipeline_def,
            project_root
        )

        return {
            "message": "Immediate pipeline execution started.",
            "pipeline_name": pipeline_def.name
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to start pipeline run: {e}")


