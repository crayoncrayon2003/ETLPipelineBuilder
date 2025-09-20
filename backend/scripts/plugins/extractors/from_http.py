import os
import requests
from typing import Dict, Any
from urllib.parse import urlparse
import pluggy

from core.infrastructure import storage_adapter
from core.data_container.container import DataContainer
from core.plugin_manager.base_plugin import BasePlugin
from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

hookimpl = pluggy.HookimplMarker("etl_framework")

class HttpExtractor(BasePlugin):

    @hookimpl
    def get_plugin_name(self) -> str:
        return "from_http"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "url": {"type": "string"},
                "output_path": {"type": "string"}
            },
            "required": ["url", "output_path"]
        }

    def run(self, input_data: DataContainer, container: DataContainer) -> DataContainer:
        url = self.params.get("url")
        output_path_str = str(self.params.get("output_path"))

        if not url or not output_path_str:
            raise ValueError("Missing required parameters: 'url' and 'output_path'.")

        final_output_path = output_path_str
        if final_output_path.endswith('/'):
            parsed_url = urlparse(url)
            filename = os.path.basename(parsed_url.path)
            if not filename:
                raise ValueError("Could not infer filename from URL.")
            final_output_path = os.path.join(final_output_path, filename)

        logger.info(f"[{self.get_plugin_name()}] Downloading from '{url}' to '{final_output_path}'...")
        try:
            response = requests.get(url=url, timeout=60)
            response.raise_for_status()
            storage_adapter.write_bytes(response.content, final_output_path)
            logger.info(f"[{self.get_plugin_name()}] File downloaded and saved successfully.")
        except requests.RequestException as e:
            raise RuntimeError(f"HTTP request failed: {e}")

        return self.finalize_container(
            container,
            output_path=final_output_path,
            metadata={"source_url": url}
        )
