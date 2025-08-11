# backend/api/main.py

import sys
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# --- Python Path Setup ---
project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from backend.api.schemas import pipeline as pipeline_schema_module
from backend.api.services import pipeline_service as pipeline_service_module

# --- Import Routers ---
# Now, when these routers are imported, their own `import` statements will find
# the already-loaded modules.
from backend.api.routers.plugins import router as plugins_router
from backend.api.routers.pipelines import router as pipelines_router

# --- FastAPI App Initialization ---
app = FastAPI(
    title="ETL Framework API",
    description="An API to interact with the custom ETL framework.",
    version="1.0.0",
)

# Define the list of origins that are allowed to make cross-origin requests.
# For development, we allow our Vite server.
# For production, you would replace this with your actual frontend's domain.
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

# --- Include Routers ---
app.include_router(plugins_router, prefix="/api/v1")
app.include_router(pipelines_router, prefix="/api/v1")

# --- Root Endpoint ---
@app.get("/", tags=["Status"])
async def read_root():
    """Root endpoint to check API status."""
    return {"status": "ok", "message": "Welcome!"}