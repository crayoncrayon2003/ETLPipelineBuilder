import os
import requests
import json
from urllib.parse import urlparse
from typing import Dict, Any
import pluggy

from core.infrastructure import storage_adapter
from core.infrastructure.storage_path_utils import normalize_path
from core.data_container.container import DataContainer, DataContainerStatus
from core.plugin_manager.base_plugin import BasePlugin

from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

hookimpl = pluggy.HookimplMarker("etl_framework")

class HttpBasicAuthExtractor(BasePlugin):
    """
    (Storage Aware) Downloads a file from an HTTP(S) source that requires
    Basic Authentication, saving to local filesystem or S3.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "from_http_with_basic_auth"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "url": {"type": "string", "title": "Source URL"},
                "output_path": {"type": "string", "title": "Output Path (local or s3://)"},
                "username": {"type": "string", "title": "Username"},
                "password": {"type": "string", "title": "Password", "format": "password"}
            },
            "required": ["url", "output_path", "username", "password"]
        }

    @hookimpl
    def execute(self, input_data: DataContainer) -> DataContainer:
        logger.info(f"Received params: {json.dumps(self.params, indent=2)}")

        url = self.params.get("url")
        output_path_str = str(self.params.get("output_path"))
        username = self.params.get("username")
        password = self.params.get("password")

        container = DataContainer()

        if not all([url, output_path_str, username, password]):
            container.set_status(DataContainerStatus.ERROR)
            container.add_error("Missing required parameters: 'url', 'output_path', 'username', 'password'.")
            return container

        final_output_path = normalize_path(output_path_str, os.getcwd())

        if final_output_path.endswith('/'):
            parsed_url = urlparse(url)
            filename = os.path.basename(parsed_url.path)
            if not filename:
                container.set_status(DataContainerStatus.ERROR)
                container.add_error("Could not infer filename from URL.")
                return container
            final_output_path = os.path.join(final_output_path, filename)

        logger.info(f"Downloading from '{url}' to '{final_output_path}' using Basic Auth...")

        try:
            response = requests.get(url, auth=(username, password), timeout=60)
            response.raise_for_status()
            storage_adapter.write_bytes(response.content, final_output_path)
            logger.info("File downloaded and saved successfully.")
        except requests.RequestException as e:
            logger.error(f"HTTP request with Basic Auth failed: {e}")
            container.set_status(DataContainerStatus.ERROR)
            container.add_error(str(e))
            return container

        container.set_status(DataContainerStatus.SUCCESS)
        container.add_file_path(final_output_path)
        container.metadata['source_url'] = url
        container.add_history(self.get_plugin_name())
        return container
