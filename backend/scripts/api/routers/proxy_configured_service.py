from fastapi import APIRouter, Request, HTTPException, Query
from typing import Optional
import os

from api.services.proxy_configured_service import process_configured_request

router = APIRouter()

@router.post("/proxy/configured_service/{config_name}", tags=["Proxy"])
async def configured_service_named(
    config_name: str,
    request: Request,
    project_root: Optional[str] = None
):
    body_bytes = await request.body()
    headers = dict(request.headers)

    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.normpath(os.path.join(current_dir, "..", "config", f"{config_name}.json"))

    try:
        result = process_configured_request(
            body_bytes=body_bytes,
            config_path=config_path,
            headers=headers,
            project_root=project_root
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
