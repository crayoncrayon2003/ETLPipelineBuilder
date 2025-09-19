import os
import requests
from typing import Dict, Any, Optional
import pluggy
from urllib.parse import urlparse
import pluggy

from core.infrastructure import storage_adapter
from core.data_container.container import DataContainer, DataContainerStatus
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

    @hookimpl
    def execute(self, input_data: DataContainer) -> DataContainer:
        url = self.params.get("url")
        output_path_str = str(self.params.get("output_path"))
        container = DataContainer()

        if not url or not output_path_str:
            container.set_status(DataContainerStatus.ERROR)
            container.add_error("Missing required parameters: 'url' and 'output_path'.")
            return container

        final_output_path = output_path_str
        if final_output_path.endswith('/'):
            parsed_url = urlparse(url)
            filename = os.path.basename(parsed_url.path)
            if not filename:
                container.set_status(DataContainerStatus.ERROR)
                container.add_error("Could not infer filename from URL.")
                return container
            final_output_path = os.path.join(final_output_path, filename)

        logger.info(f"Downloading from '{url}' to '{final_output_path}'...")
        try:
            response = requests.get(url=url, timeout=60)
            response.raise_for_status()
            storage_adapter.write_bytes(response.content, final_output_path)
            logger.info("File downloaded and saved successfully.")
        except requests.RequestException as e:
            logger.error(f"HTTP request failed: {e}")
            container.set_status(DataContainerStatus.ERROR)
            container.add_error(str(e))
            return container

        container.set_status(DataContainerStatus.SUCCESS)
        container.add_file_path(final_output_path)
        container.metadata['source_url'] = url
        container.add_history(self.get_plugin_name())
        return container
