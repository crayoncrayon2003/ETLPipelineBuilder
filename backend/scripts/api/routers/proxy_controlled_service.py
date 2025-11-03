from fastapi import APIRouter, Request, HTTPException, Query
from typing import Optional, Dict, Any
import json
import tempfile
import os

from api.services.proxy_controlled_service import process_controlled_request

router = APIRouter()

@router.post("/proxy/controlled_service", tags=["Proxy"])
async def controlled_service(
    request: Request,
    steps_json: Optional[str] = Query(None),
    storage_dir: Optional[str] = Query(None)
):
    body_bytes = await request.body()
    headers = dict(request.headers)

    try:
        steps = json.loads(steps_json) if steps_json else []
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid steps_json: {e}")

    payload = {
        "steps": steps,
        "storage": {"dir": storage_dir} if storage_dir else {}
    }

    try:
        result = process_controlled_request(
            body_bytes=body_bytes,
            payload=payload,
            headers=headers
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
