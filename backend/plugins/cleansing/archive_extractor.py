# backend/plugins/cleansing/archive_extractor.py

import zipfile
import tarfile
from pathlib import Path
from typing import Dict, Any, Optional
import pluggy

from backend.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class ArchiveExtractor:
    """
    (File-based) Extracts files from an archive into a destination directory.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "archive_extractor"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {
                    "type": "string",
                    "title": "Input Archive Path",
                    "description": "Path to the archive file (.zip, .tar.gz) to be extracted."
                },
                "output_path": {
                    "type": "string",
                    "title": "Output Directory",
                    "description": "The directory where the contents of the archive will be extracted."
                }
            },
            "required": ["input_path", "output_path"]
        }

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = Path(params.get("input_path"))
        # For this plugin, output_path is treated as a directory.
        output_dir = Path(params.get("output_path"))

        if not input_path or not output_dir:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'input_path' and 'output_path' parameters.")

        if not input_path.exists():
            raise FileNotFoundError(f"Input archive file not found at: {input_path}")

        # Create the destination directory
        output_dir.mkdir(parents=True, exist_ok=True)

        extracted_files = []

        print(f"Extracting archive '{input_path}' to directory '{output_dir}'")

        if zipfile.is_zipfile(input_path):
            with zipfile.ZipFile(input_path, 'r') as zip_ref:
                zip_ref.extractall(output_dir)
                for name in zip_ref.namelist():
                    if (output_dir / name).is_file():
                        extracted_files.append(output_dir / name)
            print(f"Extracted {len(extracted_files)} files from ZIP archive.")

        elif tarfile.is_tarfile(input_path):
            with tarfile.open(input_path, 'r:*') as tar_ref:
                tar_ref.extractall(output_dir)
                for member in tar_ref.getmembers():
                    if member.isfile():
                        extracted_files.append(output_dir / member.name)
            print(f"Extracted {len(extracted_files)} files from Tar archive.")
        else:
            raise ValueError(f"'{input_path}' is not a recognized ZIP or Tar archive. Cannot extract.")

        # Return a container with paths to ALL extracted files
        output_container = DataContainer()
        for f_path in extracted_files:
            output_container.add_file_path(f_path)

        output_container.metadata['extracted_from'] = str(input_path)
        return output_container