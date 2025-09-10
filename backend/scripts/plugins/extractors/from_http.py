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

class HttpExtractor:
    """
    (Storage Aware) Downloads a file from an HTTP(S) source and saves it
    to a specified destination (local filesystem or S3).
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "from_http"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "title": "Source URL",
                    "description": "The URL to download the data file from."
                },
                "output_path": {
                    "type": "string",
                    "title": "Output File/Directory Path (local or s3://)",
                    "description": "If a directory is provided, the filename is inferred from the URL."
                }
            },
            "required": ["url", "output_path"]
        }

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        url = params.get("url")
        output_path_str = str(params.get("output_path"))

        if not url or not output_path_str:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'url' and 'output_path'.")

        final_output_path = output_path_str
        if final_output_path.endswith('/'):
            parsed_url = urlparse(url)
            filename = Path(parsed_url.path).name
            if not filename:
                raise ValueError("Could not infer filename from URL.")
            final_output_path = final_output_path + filename

        logger.info(f"Downloading from '{url}' to '{final_output_path}'...")
        try:
            with requests.get(url=url, timeout=60) as response:
                response.raise_for_status()
                # Use the storage adapter to write the downloaded bytes.
                # It will automatically handle local vs. S3 paths.
                storage_adapter.write_bytes(response.content, final_output_path)

            logger.info("File downloaded and saved successfully.")
        except requests.RequestException as e:
            logger.error(f"HTTP request failed: {e}")
            raise

        container = DataContainer()
        container.add_file_path(final_output_path)
        container.metadata['source_url'] = url
        return container