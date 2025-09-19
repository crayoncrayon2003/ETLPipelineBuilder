import os
import json
import csv
import tarfile
import zipfile
import xml.etree.ElementTree as ET
import tempfile
from typing import Dict, Any
import pluggy

from core.data_container.container import DataContainer, DataContainerStatus
from core.data_container.formats import SupportedFormats
from core.infrastructure import storage_adapter
from core.plugin_manager.base_plugin import BasePlugin

from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

hookimpl = pluggy.HookimplMarker("etl_framework")

class FormatDetector(BasePlugin):
    """
    (Storage Aware) Detects the format of a file (local or S3) and passes it through.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "format_detector"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {"type": "string", "title": "Input File Path (local/s3)"},
                "output_path": {"type": "string", "title": "Output File Path (local/s3)"},
                "read_chunk_size": {
                    "type": "integer",
                    "title": "Read Chunk Size for Detection",
                    "default": 4096,
                    "description": "Number of bytes to read from the file start for format detection."
                }
            },
            "required": ["input_path", "output_path"]
        }

    def _detect_format(self, file_path: str, read_chunk_size: int) -> SupportedFormats:
        try:
            if os.path.getsize(file_path) == 0:
                return SupportedFormats.UNKNOWN

            with open(file_path, 'rb') as f:
                chunk = f.read(read_chunk_size)

            try:
                text_chunk = chunk.decode('utf-8').lstrip()
            except UnicodeDecodeError:
                return SupportedFormats.BINARY

            if text_chunk.startswith(('{', '[')):
                try:
                    json.loads(text_chunk.split('\n')[0].strip() if '\n' in text_chunk else text_chunk)
                    return SupportedFormats.JSON
                except json.JSONDecodeError:
                    pass

            if text_chunk.startswith('<'):
                try:
                    ET.fromstring(text_chunk)
                    return SupportedFormats.XML
                except ET.ParseError:
                    pass

            if len(text_chunk) > 100:
                try:
                    csv.Sniffer().sniff(text_chunk, delimiters=',;\t|')
                    return SupportedFormats.CSV
                except csv.Error:
                    pass

            return SupportedFormats.TEXT
        except Exception as e:
            logger.error(f"Error during format detection for {file_path}: {e}")
            return SupportedFormats.UNKNOWN

    @hookimpl
    def execute(self, input_data: DataContainer) -> DataContainer:
        input_path = str(self.params.get("input_path"))
        output_path = str(self.params.get("output_path"))
        read_chunk_size = self.params.get("read_chunk_size", 4096)

        container = DataContainer()

        if not input_path or not output_path:
            container.set_status(DataContainerStatus.ERROR)
            container.add_error("Missing required parameters: 'input_path' and 'output_path'.")
            return container

        try:
            file_content_bytes = storage_adapter.read_bytes(input_path)

            with tempfile.TemporaryDirectory() as temp_dir:
                temp_input_path = os.path.join(temp_dir, "input_file_to_detect")
                with open(temp_input_path, "wb") as f:
                    f.write(file_content_bytes)

                detected_format = self._detect_format(temp_input_path, read_chunk_size)
                logger.info(f"Detected format: {detected_format.value}")
        except Exception as e:
            container.set_status(DataContainerStatus.ERROR)
            container.add_error(f"Format detection failed: {str(e)}")
            return container

        try:
            storage_adapter.copy_file(input_path, output_path)
        except Exception as e:
            container.set_status(DataContainerStatus.ERROR)
            container.add_error(f"Failed to copy file: {str(e)}")
            return container

        container.set_status(DataContainerStatus.SUCCESS)
        container.add_file_path(output_path)
        container.metadata['detected_format'] = detected_format.value
        container.add_history(self.get_plugin_name())
        return container
