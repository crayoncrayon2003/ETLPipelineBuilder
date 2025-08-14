import sys
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# --- Python Path Setup ---
project_root = Path(__file__).resolve().parents[2]
scripts_path = project_root / "scripts"
if str(scripts_path) not in sys.path:
    sys.path.append(str(scripts_path))

from api.schemas import pipeline as pipeline_schema_module
from api.services import pipeline_service as pipeline_service_module

from core.plugin_manager.manager import framework_manager

from api.routers.plugins import router as plugins_router
from api.routers.pipelines import router as pipelines_router
from api.routers.schemas import router as schemas_router

app = FastAPI(
    title="ETL Framework API",
    description="An API to interact with the custom ETL framework.",
    version="1.0.0",
)

origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(plugins_router, prefix="/api/v1")
app.include_router(pipelines_router, prefix="/api/v1")
app.include_router(schemas_router, prefix="/api/v1")

@app.get("/", tags=["Status"])
async def read_root():
    """Root endpoint to check API status."""
    return {"status": "ok", "message": "Welcome!"}