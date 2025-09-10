from pathlib import Path
from typing import Dict, Any, Optional
import pluggy
import charset_normalizer
import tempfile

from core.data_container.container import DataContainer
from core.infrastructure import storage_adapter

hookimpl = pluggy.HookimplMarker("etl_framework")

class EncodingConverter:
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

    def _detect_encoding(self, file_path: Path, sample_size: int) -> str:
        """
        Detects the encoding of a local file using charset_normalizer.
        """
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(sample_size)

            result = charset_normalizer.from_bytes(raw_data).best()
            if result and result.encoding:
                print(f"Detected encoding for '{file_path.name}': {result.encoding} (confidence: {result.confidence:.2f})")
                return result.encoding
            print(f"Warning: Could not confidently detect encoding for '{file_path.name}'. Defaulting to 'latin-1'.")
            return 'latin-1'
        except Exception as e:
            print(f"Error during encoding detection for '{file_path.name}': {e}. Defaulting to 'latin-1'.")
            return 'latin-1'

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = str(params.get("input_path"))
        output_path = str(params.get("output_path"))
        target_encoding = params.get("target_encoding", "utf-8")
        source_encoding = params.get("source_encoding")
        encoding_detection_sample_size = params.get("encoding_detection_sample_size", 10000)

        if not input_path or not output_path:
            raise ValueError("Plugin requires 'input_path', 'output_path', and 'strategy'.")

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_input_path = Path(temp_dir) / "input_file_for_conversion"

            # 1. Read file content (local or S3) as bytes using StorageAdapter
            print(f"Reading '{input_path}' content for encoding conversion using StorageAdapter...")
            try:
                file_content_bytes = storage_adapter.read_bytes(input_path)
                temp_input_path.write_bytes(file_content_bytes)
            except Exception as e:
                raise IOError(f"Failed to read input file '{input_path}' using StorageAdapter: {e}") from e

            # 2. Determine source encoding
            source_enc = source_encoding
            if not source_enc:
                source_enc = self._detect_encoding(temp_input_path, encoding_detection_sample_size)

            print(f"Processing file from '{input_path}' (detected/provided encoding: {source_enc}) "
                  f"to '{output_path}' (target encoding: {target_encoding}).")

            # 3. Perform encoding conversion
            try:
                content = temp_input_path.read_text(encoding=source_enc, errors='replace')

                # 4. Use StorageAdapter to write the converted content to the final destination
                storage_adapter.write_text(content, output_path)
            except Exception as e:
                print(f"ERROR during encoding conversion or writing: {e}")
                raise

        output_container = DataContainer()
        output_container.add_file_path(output_path)
        output_container.metadata['original_encoding'] = source_enc
        output_container.metadata['converted_to_encoding'] = target_encoding
        return output_container