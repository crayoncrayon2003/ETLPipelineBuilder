import os
import tempfile
import charset_normalizer
from typing import Dict, Any
import pluggy

from core.data_container.container import DataContainer, DataContainerStatus
from core.infrastructure import storage_adapter
from core.plugin_manager.base_plugin import BasePlugin

from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

hookimpl = pluggy.HookimplMarker("etl_framework")

class EncodingConverter(BasePlugin):
    """
    (Storage Aware) Converts the character encoding of a text file from local or S3.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "encoding_converter"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {"type": "string", "title": "Input File Path (local/s3)"},
                "output_path": {"type": "string", "title": "Output File Path (local/s3)"},
                "target_encoding": {"type": "string", "title": "Target Encoding", "default": "utf-8"},
                "source_encoding": {
                    "type": "string",
                    "title": "Source Encoding (Optional)",
                    "description": "Specify the source encoding if known. If not provided, it will be auto-detected."
                },
                "encoding_detection_sample_size": {
                    "type": "integer",
                    "title": "Encoding Detection Sample Size",
                    "default": 10000,
                    "description": "Number of bytes to read for auto-detecting the source encoding."
                }
            },
            "required": ["input_path", "output_path"]
        }

    def _detect_encoding(self, file_path: str, sample_size: int) -> str:
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(sample_size)

            result = charset_normalizer.from_bytes(raw_data).best()
            file_name = os.path.basename(file_path)

            if result and result.encoding:
                logger.info(f"Detected encoding for '{file_name}': {result.encoding} (confidence: {result.confidence:.2f})")
                return result.encoding
            logger.info(f"Could not confidently detect encoding for '{file_name}'. Defaulting to 'latin-1'.")
            return 'latin-1'
        except Exception as e:
            logger.error(f"Error during encoding detection for '{file_path}': {e}. Defaulting to 'latin-1'.")
            return 'latin-1'

    @hookimpl
    def execute(self, input_data: DataContainer) -> DataContainer:
        input_path = str(self.params.get("input_path"))
        output_path = str(self.params.get("output_path"))
        target_encoding = self.params.get("target_encoding", "utf-8")
        source_encoding = self.params.get("source_encoding")
        sample_size = self.params.get("encoding_detection_sample_size", 10000)

        container = DataContainer()

        if not input_path or not output_path:
            container.set_status(DataContainerStatus.ERROR)
            container.add_error("Missing required parameters: 'input_path' and 'output_path'.")
            return container

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_input_path = os.path.join(temp_dir, "input_file_for_conversion")

            try:
                file_content_bytes = storage_adapter.read_bytes(input_path)
                with open(temp_input_path, 'wb') as f:
                    f.write(file_content_bytes)
            except Exception as e:
                container.set_status(DataContainerStatus.ERROR)
                container.add_error(f"Failed to read input file: {str(e)}")
                return container

            source_enc = source_encoding or self._detect_encoding(temp_input_path, sample_size)

            logger.info(f"Converting from '{source_enc}' to '{target_encoding}'...")

            try:
                with open(temp_input_path, 'r', encoding=source_enc, errors='replace') as f:
                    content = f.read()
                storage_adapter.write_text(content, output_path)
            except Exception as e:
                container.set_status(DataContainerStatus.ERROR)
                container.add_error(f"Encoding conversion failed: {str(e)}")
                return container

        container.set_status(DataContainerStatus.SUCCESS)
        container.add_file_path(output_path)
        container.metadata['original_encoding'] = source_enc
        container.metadata['converted_to_encoding'] = target_encoding
        container.add_history(self.get_plugin_name())
        return container
