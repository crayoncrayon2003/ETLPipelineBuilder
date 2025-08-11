# backend/plugins/loaders/to_http.py

import asyncio, aiohttp, json
from typing import Dict, Any, List, Optional
import pluggy
from pathlib import Path

from core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class HttpLoader:
    """
    (File-based) Loads data by sending lines from a file to an HTTP endpoint.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "to_http"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {"type": "string", "title": "Input JSONL File Path"},
                "url": {"type": "string", "title": "Target URL"},
                "method": {"type": "string", "title": "HTTP Method", "enum": ["POST", "PUT"], "default": "POST"},
                "concurrency": {"type": "integer", "title": "Concurrency", "default": 10}
            },
            "required": ["input_path", "url"]
        }

    async def _send_request(self, session: aiohttp.ClientSession, payload: str, index: int):
        try:
            async with session.request(self.method, self.url, data=payload.encode('utf-8'), headers=self.headers) as response:
                if response.status >= 400:
                    error_text = await response.text()
                    print(f"Request {index+1} failed with status {response.status}: {error_text[:200]}")
                    response.raise_for_status()
                print(f"Request {index+1} succeeded with status {response.status}.")
        except aiohttp.ClientError as e:
            print(f"Request {index+1} failed with client error: {e}")
            if self.stop_on_fail: raise

    async def _main(self, payloads: List[str]):
        conn = aiohttp.TCPConnector(limit=self.concurrency)
        async with aiohttp.ClientSession(connector=conn) as session:
            tasks = [self._send_request(session, payload, i) for i, payload in enumerate(payloads)]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        final_errors = [res for res in results if isinstance(res, Exception)]
        if final_errors: raise final_errors[0]

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = Path(params.get("input_path"))
        self.url = params.get("url")
        self.method = params.get("method", "POST").upper()
        self.headers = params.get("headers", {})
        self.concurrency = params.get("concurrency", 10)
        self.stop_on_fail = params.get("stop_on_fail", True)

        if not input_path or not self.url:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'input_path' and 'url'.")
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found at: {input_path}")
        if 'Content-Type' not in self.headers: self.headers['Content-Type'] = 'application/json'

        print(f"Reading file '{input_path}' to send to HTTP endpoint {self.url}...")
        with open(input_path, 'r', encoding='utf-8') as f:
            payloads = [line.strip() for line in f if line.strip()]

        if not payloads:
            print("No data in file to load."); return None

        print(f"Sending {len(payloads)} HTTP {self.method} requests...")
        try:
            asyncio.run(self._main(payloads))
            print("All HTTP requests processed successfully.")
        except Exception as e:
            print(f"An error occurred during HTTP loading: {e}")
            raise
        return None