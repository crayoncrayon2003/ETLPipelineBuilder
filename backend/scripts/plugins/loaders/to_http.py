import os
import asyncio, aiohttp, json
from typing import Dict, Any, List, Optional
import pluggy
from pathlib import Path
import tempfile

from core.data_container.container import DataContainer
from core.infrastructure import storage_adapter

from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

hookimpl = pluggy.HookimplMarker("etl_framework")

class HttpLoader:
    """
    (Storage Aware) Loads data by sending lines from a file (local or S3)
    to an HTTP endpoint.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "to_http"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {"type": "string", "title": "Input JSONL File Path (local/s3)"},
                "url": {"type": "string", "title": "Target URL"},
                "method": {"type": "string", "title": "HTTP Method", "enum": ["POST", "PUT"], "default": "POST"},
                "concurrency": {"type": "integer", "title": "Concurrency", "default": 10},
                "headers": {"type": "object", "title": "HTTP Headers", "default": {}},
                "stop_on_fail": {"type": "boolean", "title": "Stop on first request failure", "default": True}
            },
            "required": ["input_path", "url"]
        }

    async def _send_request(self, session: aiohttp.ClientSession, payload: str, index: int):
        try:
            async with session.request(self.method, self.url, data=payload.encode('utf-8'), headers=self.headers) as response:
                if response.status >= 400:
                    error_text = await response.text()
                    logger.info(f"Request {index+1} failed with status {response.status}: {error_text[:200]}")
                    response.raise_for_status()
                logger.info(f"Request {index+1} succeeded.")
        except aiohttp.ClientError as e:
            logger.error(f"Request {index+1} failed with client error: {e}")
            if self.stop_on_fail: raise

    async def _main(self, payloads: List[str]):
        conn = aiohttp.TCPConnector(limit_per_host=self.concurrency)
        async with aiohttp.ClientSession(connector=conn) as session:
            tasks = [self._send_request(session, payload, i) for i, payload in enumerate(payloads)]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        final_errors = [res for res in results if isinstance(res, Exception)]
        if final_errors:
            raise RuntimeError(f"Some HTTP requests failed: {final_errors[0]}") from final_errors[0]


    @hookimpl
    def execute_plugin(self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]) -> Optional[DataContainer]:
        input_path = str(params.get("input_path"))
        self.url = params.get("url")
        self.method = params.get("method", "POST").upper()
        self.headers = params.get("headers", {})
        self.concurrency = params.get("concurrency", 10)
        self.stop_on_fail = params.get("stop_on_fail", True)

        if not input_path or not self.url:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'input_path' and 'url'.")
        if 'Content-Type' not in self.headers: self.headers['Content-Type'] = 'application/json'

        logger.info(f"Reading file '{input_path}' to send to HTTP endpoint using StorageAdapter...")
        try:
            file_content = storage_adapter.read_text(input_path)
            payloads = [line.strip() for line in file_content.splitlines() if line.strip()]
        except Exception as e:
            raise IOError(f"Failed to read input file '{input_path}' using StorageAdapter: {e}") from e

        if not payloads:
            logger.info("No data in file to load."); return None

        logger.info(f"Sending {len(payloads)} HTTP {self.method} requests with concurrency {self.concurrency}...")
        try:
            asyncio.run(self._main(payloads))
            logger.info("All HTTP requests processed successfully.")
        except Exception as e:
            logger.error(f"An error occurred during HTTP loading: {e}")
            raise
        return None