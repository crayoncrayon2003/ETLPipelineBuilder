import requests
from pathlib import Path
from typing import Dict, Any, Optional
import pluggy
from urllib.parse import urlparse

from core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class HttpExtractor:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "from_http"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "url": {"type": "string", "title": "Source URL"},
                "output_path": {"type": "string", "title": "Output File/Directory Path"}
            },
            "required": ["url", "output_path"]
        }

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        url = params.get("url")
        output_path = params.get("output_path") # This is now a Path object
        method = params.get("method", "GET").upper()
        request_params = params.get("request_params")
        headers = params.get("headers")

        if not url or not output_path:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'url' and 'output_path'.")

        final_output_path = output_path
        if output_path.is_dir():
            parsed_url = urlparse(url)
            filename = Path(parsed_url.path).name
            if not filename:
                raise ValueError("Could not infer filename from URL. Please provide a full file path for output_path.")
            final_output_path = output_path / filename

        print(f"Downloading from '{url}' to '{final_output_path}'...")
        try:
            with requests.request(method=method, url=url, params=request_params, headers=headers, stream=True, timeout=60) as response:
                response.raise_for_status()
                final_output_path.parent.mkdir(parents=True, exist_ok=True)
                with open(final_output_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
            print("File downloaded successfully.")
        except requests.RequestException as e:
            print(f"HTTP request failed: {e}")
            raise

        container = DataContainer()
        container.add_file_path(final_output_path)
        container.metadata['source_url'] = url
        return container