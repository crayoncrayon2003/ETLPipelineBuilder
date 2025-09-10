import os
import requests
from pathlib import Path
from typing import Dict, Any, Optional
import pluggy
from urllib.parse import urlparse

from core.infrastructure import storage_adapter
from core.data_container.container import DataContainer

from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

hookimpl = pluggy.HookimplMarker("etl_framework")

class HttpBasicAuthExtractor:
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
                "url": {"type": "string", "title": "Source URL" },
                "output_path": {"type": "string", "title": "Output Path (local or s3://)"},
                "username": {"type": "string", "title": "Username"},
                "password": {"type": "string", "title": "Password", "format": "password"}
            },
            "required": ["url", "output_path", "username", "password"]
        }

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        url = params.get("url")
        output_path_str = str(params.get("output_path"))
        username = params.get("username")
        password = params.get("password")

        if not all([url, output_path_str, username, password]):
            raise ValueError("Plugin requires 'url', 'output_path', 'username', and 'password'.")

        final_output_path = output_path_str
        if final_output_path.endswith('/'):
            parsed_url = urlparse(url)
            filename = Path(parsed_url.path).name
            if not filename: raise ValueError("Could not infer filename from URL.")
            final_output_path = final_output_path + filename

        logger.info(f"Downloading from '{url}' to '{final_output_path}' using Basic Auth...")
        try:
            with requests.get(url, auth=(username, password), timeout=60) as response:
                response.raise_for_status()
                storage_adapter.write_bytes(response.content, final_output_path)

            logger.info("File downloaded and saved successfully.")
        except requests.RequestException as e:
            logger.error(f"HTTP request with Basic Auth failed: {e}")
            raise

        container = DataContainer()
        container.add_file_path(final_output_path)
        container.metadata['source_url'] = url
        return container