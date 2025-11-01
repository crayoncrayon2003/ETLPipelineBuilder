import os
import requests
import json
from urllib.parse import urlparse
from typing import Dict, Any
import pluggy

from core.infrastructure import storage_adapter
from core.infrastructure.storage_path_utils import normalize_path
from core.data_container.container import DataContainer
from core.plugin_manager.base_plugin import BasePlugin
from core.infrastructure import secret
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

    def run(self, input_data: DataContainer, container: DataContainer) -> DataContainer:
        logger.info(f"[{self.get_plugin_name()}] Received params: {json.dumps(self.params, indent=2)}")

        url = self.params.get("url")
        output_path_str = str(self.params.get("output_path"))
        username = self.params.get("username")
        password = self.params.get("password")

        if username:
            username = secret.read_secret(username)
        if password:
            password = secret.read_secret(password)

        if not all([url, output_path_str, username, password]):
            raise ValueError("Missing required parameters: 'url', 'output_path', 'username', 'password'.")

        final_output_path = normalize_path(output_path_str, os.getcwd())

        if final_output_path.endswith('/'):
            parsed_url = urlparse(url)
            filename = os.path.basename(parsed_url.path)
            if not filename:
                raise ValueError("Could not infer filename from URL.")
            final_output_path = os.path.join(final_output_path, filename)

        logger.info(f"[{self.get_plugin_name()}] Downloading from '{url}' to '{final_output_path}' using Basic Auth...")

        try:
            response = requests.get(url, auth=(username, password), timeout=60)
            response.raise_for_status()
            storage_adapter.write_bytes(response.content, final_output_path)
            logger.info(f"[{self.get_plugin_name()}] File downloaded and saved successfully.")
        except requests.RequestException as e:
            raise RuntimeError(f"HTTP request with Basic Auth failed: {e}")

        return self.finalize_container(
            container,
            output_path=final_output_path,
            metadata={"source_url": url}
        )
