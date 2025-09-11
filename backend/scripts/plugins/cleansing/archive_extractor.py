import os
import zipfile
import tarfile
import tempfile
import pluggy
from typing import Dict, Any, Optional

from core.data_container.container import DataContainer
from core.infrastructure import storage_adapter
from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

hookimpl = pluggy.HookimplMarker("etl_framework")


class ArchiveExtractor:
    """
    (Storage Aware) Extracts files from an archive (local or S3) to a
    destination directory (local or S3).
    """

    @hookimpl
    def get_plugin_name(self) -> str:
        return "archive_extractor"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {"type": "string", "title": "Input Archive Path (local or s3://)"},
                "output_path": {"type": "string", "title": "Output Directory (local or s3://)"},
                "strip_components": {
                    "type": "integer",
                    "title": "Strip Path Components",
                    "default": 0,
                    "description": "Number of leading path components to strip from extracted files (e.g., 1 to remove top-level directory)."
                }
            },
            "required": ["input_path", "output_path"]
        }

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = str(params.get("input_path"))
        output_path = str(params.get("output_path"))
        strip_components = params.get("strip_components", 0)

        if not input_path or not output_path:
            raise ValueError("Plugin requires 'input_path' and 'output_path'.")

        with tempfile.TemporaryDirectory() as temp_dir:
            archive_filename = os.path.basename(input_path)
            local_archive_path = os.path.join(temp_dir, archive_filename)
            local_extraction_dir = os.path.join(temp_dir, "extracted_content")

            os.makedirs(local_extraction_dir, exist_ok=True)

            # --- Step 1: Read the archive content as bytes ---
            logger.info(f"Reading archive '{input_path}' using StorageAdapter...")
            try:
                archive_content_bytes = storage_adapter.read_bytes(input_path)
                with open(local_archive_path, 'wb') as f:
                    f.write(archive_content_bytes)
            except Exception as e:
                raise IOError(f"Failed to read archive '{input_path}' using StorageAdapter: {e}") from e

            # --- Step 2: Extract the local archive ---
            extracted_files_rel_paths = []
            logger.info(f"Extracting local archive '{local_archive_path}' to '{local_extraction_dir}'...")

            if zipfile.is_zipfile(local_archive_path):
                with zipfile.ZipFile(local_archive_path, 'r') as zip_ref:
                    zip_ref.extractall(local_extraction_dir)
                    for name in zip_ref.namelist():
                        if not name.endswith('/'):
                            extracted_files_rel_paths.append(name)
            elif tarfile.is_tarfile(local_archive_path):
                with tarfile.open(local_archive_path, 'r:*') as tar_ref:
                    tar_ref.extractall(local_extraction_dir)
                    for member in tar_ref.getmembers():
                        if member.isfile():
                            extracted_files_rel_paths.append(member.name)
            else:
                raise ValueError(f"'{input_path}' is not a recognized archive type (zip or tar).")

            # --- Step 3: Upload extracted files to the final destination ---
            output_container = DataContainer()

            if not extracted_files_rel_paths:
                logger.info("No files extracted from the archive.")
                return None

            logger.info(f"Uploading {len(extracted_files_rel_paths)} extracted files to '{output_path}'...")

            for rel_path in extracted_files_rel_paths:
                final_rel_path = rel_path
                parts = rel_path.split(os.sep)

                if strip_components > 0:
                    if len(parts) > strip_components:
                        final_rel_path = os.path.join(*parts[strip_components:])
                    else:
                        logger.info(f"Warning: Cannot strip {strip_components} components from '{rel_path}'. Skipping stripping.")
                        final_rel_path = os.path.basename(rel_path)

                local_full_path = os.path.join(local_extraction_dir, rel_path)
                final_dest_path = os.path.join(output_path.rstrip('/'), final_rel_path).replace('\\', '/')

                logger.info(f"  Uploading '{local_full_path}' to '{final_dest_path}'...")
                storage_adapter.upload_local_file(local_full_path, final_dest_path)
                output_container.add_file_path(final_dest_path)

        output_container.metadata['extracted_from'] = input_path
        return output_container
