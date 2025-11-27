import os
import asyncio
import aiohttp
import json
from typing import Dict, Any, List
import pluggy

from core.data_container.container import DataContainer
from core.infrastructure import storage_adapter
from core.plugin_manager.base_plugin import BasePlugin

from utils.logger import setup_logger

logger = setup_logger(__name__)

hookimpl = pluggy.HookimplMarker("etl_framework")

def run_async(coro):
    try:
        loop = asyncio.get_running_loop()
        return loop.create_task(coro)
    except RuntimeError:
        return asyncio.run(coro)

class HttpLoader(BasePlugin):
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
                    logger.warning(f"[{self.get_plugin_name()}] Request {index+1} failed with status {response.status}: {error_text[:200]}")
                    response.raise_for_status()
                logger.info(f"[{self.get_plugin_name()}] Request {index+1} succeeded.")
        except aiohttp.ClientError as e:
            logger.error(f"[{self.get_plugin_name()}] Request {index+1} failed: {e}")
            if self.stop_on_fail:
                raise

    async def _main(self, payloads: List[str]):
        conn = aiohttp.TCPConnector(limit_per_host=self.concurrency)
        async with aiohttp.ClientSession(connector=conn) as session:
            tasks = [self._send_request(session, payload, i) for i, payload in enumerate(payloads)]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        errors = [e for e in results if isinstance(e, Exception)]
        if errors:
            raise RuntimeError(f"{len(errors)} HTTP requests failed. First error: {errors[0]}") from errors[0]

    def run(self, input_data: DataContainer, container: DataContainer) -> DataContainer:
        input_path = str(self.params.get("input_path"))
        self.url = self.params.get("url")
        self.method = self.params.get("method", "POST").upper()
        self.headers = self.params.get("headers", {})
        self.concurrency = self.params.get("concurrency", 10)
        self.stop_on_fail = self.params.get("stop_on_fail", True)

        if not input_path or not self.url:
            raise ValueError("Missing required parameters: 'input_path' and 'url'.")

        if 'Content-Type' not in self.headers:
            self.headers['Content-Type'] = 'application/json'

        try:
            file_content = storage_adapter.read_text(input_path)
            payloads = [line.strip() for line in file_content.splitlines() if line.strip()]
        except Exception as e:
            raise RuntimeError(f"Failed to read input file: {str(e)}")

        if not payloads:
            raise RuntimeError("Input file contains no valid lines.")

        logger.info(f"[{self.get_plugin_name()}] Sending {len(payloads)} HTTP {self.method} requests to {self.url} with concurrency {self.concurrency}...")

        try:
            run_async(self._main(payloads))
        except Exception as e:
            raise RuntimeError(f"HTTP loading failed: {str(e)}")

        return self.finalize_container(
            container,
            output_path=input_path,
            metadata={
                "input_path": input_path,
                "url": self.url,
                "method": self.method,
                "requests_sent": len(payloads),
                "concurrency": self.concurrency
            }
        )
