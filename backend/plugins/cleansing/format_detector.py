# backend/plugins/cleansing/format_detector.py

import json, csv, tarfile, zipfile, xml.etree.ElementTree as ET, shutil
from pathlib import Path
from typing import Dict, Any, Optional

import pluggy

from backend.core.data_container.container import DataContainer
from backend.core.data_container.formats import SupportedFormats

hookimpl = pluggy.HookimplMarker("etl_framework")

class FormatDetector:
    """
    (File-based) Detects the format of a file and passes the path through.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "format_detector"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {
                    "type": "string",
                    "title": "Input File Path",
                    "description": "The file whose format will be detected."
                },
                "output_path": {
                    "type": "string",
                    "title": "Output File Path",
                    "description": "Path to copy the file to after detection. The file content is not changed."
                }
            },
            "required": ["input_path", "output_path"]
        }

    def _detect_format(self, file_path: Path, read_chunk_size: int) -> SupportedFormats:
        try:
            if zipfile.is_zipfile(file_path): return SupportedFormats.ZIP
            if tarfile.is_tarfile(file_path): return SupportedFormats.TAR

            with open(file_path, 'rb') as f:
                chunk = f.read(read_chunk_size)

            try:
                text_chunk = chunk.decode('utf-8').lstrip()
            except UnicodeDecodeError:
                return SupportedFormats.BINARY

            if text_chunk.startswith(('{', '[')):
                try: json.loads(text_chunk); return SupportedFormats.JSON
                except json.JSONDecodeError: pass

            if text_chunk.startswith('<'):
                try: ET.fromstring(text_chunk); return SupportedFormats.XML
                except ET.ParseError: pass

            try:
                csv.Sniffer().sniff(text_chunk, delimiters=',;\t|'); return SupportedFormats.CSV
            except csv.Error: pass

            return SupportedFormats.TEXT
        except Exception as e:
            print(f"Could not access or read file {file_path} for format detection: {e}")
            return SupportedFormats.UNKNOWN

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = Path(params.get("input_path"))
        output_path = Path(params.get("output_path"))
        read_chunk_size = params.get("read_chunk_size", 4096)

        if not input_path or not output_path:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'input_path' and 'output_path' parameters.")
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found at: {input_path}")

        print(f"Detecting format for: {input_path.name}")
        detected_format = self._detect_format(input_path, read_chunk_size)
        print(f" -> Detected format: {detected_format.value}")

        # This plugin is non-destructive, so copy the source file to the output path.
        output_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(input_path, output_path)

        output_container = DataContainer()
        output_container.add_file_path(output_path)
        output_container.metadata['detected_format'] = detected_format.value

        return output_container