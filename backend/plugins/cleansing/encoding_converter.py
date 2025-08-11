# backend/plugins/cleansing/encoding_converter.py

from pathlib import Path
from typing import Dict, Any, Optional
import pluggy
import charset_normalizer

from backend.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class EncodingConverter:
    """
    (File-based) Converts the character encoding of a text file.
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
                    "title": "Input File Path",
                    "description": "The text file whose encoding needs to be converted."
                },
                "output_path": {
                    "type": "string",
                    "title": "Output File Path",
                    "description": "The path to save the newly encoded file."
                },
                "target_encoding": {
                    "type": "string",
                    "title": "Target Encoding",
                    "description": "The encoding to convert the file to.",
                    "default": "utf-8"
                },
                "source_encoding": {
                    "type": "string",
                    "title": "Source Encoding (Optional)",
                    "description": "The original encoding. If not provided, it will be auto-detected."
                }
            },
            "required": ["input_path", "output_path"]
        }

    def _detect_encoding(self, file_path: Path) -> str:
        with open(file_path, 'rb') as f:
            raw_data = f.read(10000)
        try:
            result = charset_normalizer.from_bytes(raw_data).best()
            if result:
                detected_encoding = result.encoding
                print(f"Detected encoding for '{file_path.name}': {detected_encoding}")
                return detected_encoding
        except Exception as e:
            print(f"Could not detect encoding for '{file_path.name}': {e}")

        print("Warning: Encoding detection failed. Falling back to 'latin-1'.")
        return 'latin-1'

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = Path(params.get("input_path"))
        output_path = Path(params.get("output_path"))
        target_encoding = params.get("target_encoding", "utf-8")
        source_encoding = params.get("source_encoding")

        if not input_path or not output_path:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'input_path' and 'output_path' parameters.")
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found at: {input_path}")

        print(f"Converting encoding for '{input_path}' to '{target_encoding}'")

        source_enc = source_encoding or self._detect_encoding(input_path)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(input_path, 'r', encoding=source_enc, errors='replace') as infile:
                content = infile.read()

            with open(output_path, 'w', encoding=target_encoding) as outfile:
                outfile.write(content)

            print(f"Successfully converted and saved to '{output_path}'.")
        except Exception as e:
            print(f"ERROR converting file: {e}")
            raise

        output_container = DataContainer()
        output_container.add_file_path(output_path)
        output_container.metadata['original_encoding'] = source_enc
        output_container.metadata['converted_to_encoding'] = target_encoding
        return output_container