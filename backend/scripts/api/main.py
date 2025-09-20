import os
import sys
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# --- Python Path Setup ---
current_file_path = os.path.abspath(__file__)
project_root = current_file_path
for _ in range(2):
    project_root = os.path.dirname(project_root)
scripts_path = os.path.join(project_root, "scripts")

if str(scripts_path) not in sys.path:
    sys.path.append(str(scripts_path))

from api.schemas import pipeline as pipeline_schema_module
from api.services import pipeline_service as pipeline_service_module

from core.plugin_manager.manager import framework_manager

from api.routers.plugins import router as plugins_router
from api.routers.pipelines import router as pipelines_router
from api.routers.proxy_controlled_service import router as controlled_router
from api.routers.proxy_configured_service import router as configured_router
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
app.include_router(controlled_router, prefix="/api/v1")
app.include_router(configured_router, prefix="/api/v1")

@app.get("/", tags=["Status"])
async def read_root():
    """Root endpoint to check API status."""
    return {"status": "ok", "message": "Welcome!"}

if __name__ == "__main__":
    uvicorn.run("api.main:app", host="127.0.0.1", port=8000, reload=True)