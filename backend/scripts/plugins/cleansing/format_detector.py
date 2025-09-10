import json, csv, tarfile, zipfile, xml.etree.ElementTree as ET, shutil, tempfile
from pathlib import Path
from typing import Dict, Any, Optional
import pluggy

from core.data_container.container import DataContainer
from core.data_container.formats import SupportedFormats
from core.infrastructure import storage_adapter

hookimpl = pluggy.HookimplMarker("etl_framework")

class FormatDetector:
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
                "read_chunk_size": { # read_chunk_sizeもスキーマに追加
                    "type": "integer",
                    "title": "Read Chunk Size for Detection",
                    "default": 4096,
                    "description": "Number of bytes to read from the file start for format detection."
                }
            },
            "required": ["input_path", "output_path"]
        }

    def _detect_format(self, file_path: Path, read_chunk_size: int) -> SupportedFormats:
        # This helper works on a local file path
        try:
            if file_path.stat().st_size == 0:
                return SupportedFormats.UNKNOWN

            with open(file_path, 'rb') as f:
                chunk = f.read(read_chunk_size)

            # detect BINARY
            try:
                text_chunk = chunk.decode('utf-8').lstrip()
            except UnicodeDecodeError:
                return SupportedFormats.BINARY

            # detect JSON
            if text_chunk.startswith(('{', '[')):
                try:
                    if '\n' in text_chunk:
                        json.loads(text_chunk.split('\n')[0].strip())
                    else:
                        json.loads(text_chunk)
                    return SupportedFormats.JSON
                except json.JSONDecodeError:
                    pass

            # detect XML
            if text_chunk.startswith('<'):
                try:
                    ET.fromstring(text_chunk); return SupportedFormats.XML
                except ET.ParseError:
                    pass

            # detect CSV
            if len(text_chunk) > 100:
                try:
                    csv.Sniffer().sniff(text_chunk, delimiters=',;\t|')
                    return SupportedFormats.CSV
                except csv.Error:
                    pass

            return SupportedFormats.TEXT
        except Exception as e:
            print(f"Error during format detection for {file_path}: {e}")
            return SupportedFormats.UNKNOWN

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = str(params.get("input_path"))
        output_path = str(params.get("output_path"))
        read_chunk_size = params.get("read_chunk_size", 4096)

        if not input_path or not output_path:
            raise ValueError("Plugin requires 'input_path' and 'output_path'.")

        detected_format = SupportedFormats.UNKNOWN

        #  Download file content (or read local) into memory, then write to temporary local file for inspection
        print(f"Reading '{input_path}' content for format detection...")
        try:
            file_content_bytes = storage_adapter.read_bytes(input_path)

            with tempfile.TemporaryDirectory() as temp_dir:
                temp_input_path = Path(temp_dir) / "input_file_to_detect"
                temp_input_path.write_bytes(file_content_bytes)

                # Detect format on the local temporary file
                detected_format = self._detect_format(temp_input_path, read_chunk_size)
                print(f" -> Detected format: {detected_format.value}")

        except Exception as e:
            print(f"Failed to process file '{input_path}' for format detection: {e}")
            raise

        # This plugin is non-destructive, so copy the original file to the output path
        # storage_adapter.copy_file は input_path が S3でもlocalでも対応する
        print(f"Format detection complete. Copying original file to '{output_path}'.")
        storage_adapter.copy_file(input_path, output_path)

        output_container = DataContainer()
        output_container.add_file_path(output_path)
        output_container.metadata['detected_format'] = detected_format.value
        return output_container