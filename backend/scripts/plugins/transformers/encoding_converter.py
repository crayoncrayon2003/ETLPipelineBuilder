import os
from typing import Dict, Any, Optional
import pluggy
from pathlib import Path

from core.data_container.container import DataContainer
from core.infrastructure.storage_adapter import storage_adapter

from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

hookimpl = pluggy.HookimplMarker("etl_framework")

class EncodingConverter:
    """
    (Storage Aware) Reads a text file from S3 with a specified input encoding,
    converts its content to a specified output encoding (default: UTF-8),
    and saves the converted content back to S3.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "encoding_converter"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {
                    "type": "string",
                    "title": "Input File Path (S3)",
                    "description": "The S3 path to the text file to be converted."
                },
                "input_encoding": {
                    "type": "string",
                    "title": "Input File Encoding",
                    "description": "The character encoding of the input file (e.g., 'shift_jis', 'cp932', 'euc_jp').",
                    "default": "cp932"
                },
                "output_path": {
                    "type": "string",
                    "title": "Output File Path (S3)",
                    "description": "The S3 path to save the converted file. Must be different from input_path."
                },
                "output_encoding": {
                    "type": "string",
                    "title": "Output File Encoding",
                    "description": "The character encoding for the output file.",
                    "default": "utf-8"
                }
            },
            "required": ["input_path", "input_encoding", "output_path"]
        }

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = str(params.get("input_path"))
        input_encoding = str(params.get("input_encoding", "cp932"))
        output_path = str(params.get("output_path"))
        output_encoding = str(params.get("output_encoding", "utf-8"))

        if input_path == output_path:
            raise ValueError("Input path and output path cannot be the same for encoding_converter.")
        if not input_path.startswith("s3://") or not output_path.startswith("s3://"):
             raise ValueError("Both input_path and output_path must be S3 paths for encoding_converter.")

        logger.info(f"Starting encoding conversion from '{input_path}' ({input_encoding}) to '{output_path}' ({output_encoding}).")

        try:
            logger.debug(f"Reading bytes from '{input_path}' with assumed encoding '{input_encoding}'.")
            raw_bytes = storage_adapter.read_bytes(input_path)

            logger.debug(f"Decoding {len(raw_bytes)} bytes using '{input_encoding}'.")
            decoded_text = raw_bytes.decode(input_encoding)

            logger.debug(f"Encoding text using '{output_encoding}'.")
            encoded_bytes = decoded_text.encode(output_encoding)

            logger.debug(f"Writing {len(encoded_bytes)} bytes to '{output_path}'.")
            storage_adapter.write_bytes(encoded_bytes, output_path)

            logger.info(f"Successfully converted and saved file from '{input_path}' to '{output_path}'.")

        except UnicodeDecodeError as e:
            logger.error(f"UnicodeDecodeError occurred when decoding '{input_path}' with '{input_encoding}': {e}", exc_info=True)
            logger.error(f"Please check the 'input_encoding' parameter. Common alternatives for Japanese: 'cp932', 'shift_jis', 'euc_jp'.")
            raise
        except Exception as e:
            logger.error(f"An error occurred during encoding conversion from '{input_path}': {e}", exc_info=True)
            raise

        output_container = DataContainer()
        output_container.add_file_path(output_path)
        output_container.metadata['original_input_path'] = input_path
        output_container.metadata['converted_encoding'] = output_encoding
        return output_container