import os
import zipfile
import tarfile
import tempfile
from typing import Dict, Any
import pluggy

from core.data_container.container import DataContainer
from core.infrastructure import storage_adapter
from core.plugin_manager.base_plugin import BasePlugin

from utils.logger import setup_logger

logger = setup_logger(__name__)

hookimpl = pluggy.HookimplMarker("etl_framework")

class ArchiveExtractor(BasePlugin):
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
                    "description": "Number of leading path components to strip from extracted files."
                }
            },
            "required": ["input_path", "output_path"]
        }

    def run(self, input_data: DataContainer, container: DataContainer) -> DataContainer:
        input_path = str(self.params.get("input_path"))
        output_path = str(self.params.get("output_path"))
        strip_components = self.params.get("strip_components", 0)

        if not input_path or not output_path:
            raise ValueError("Missing required parameters: 'input_path' and 'output_path'.")

        with tempfile.TemporaryDirectory() as temp_dir:
            archive_filename = os.path.basename(input_path)
            local_archive_path = os.path.join(temp_dir, archive_filename)
            local_extraction_dir = os.path.join(temp_dir, "extracted_content")
            os.makedirs(local_extraction_dir, exist_ok=True)

            logger.info(f"[{self.get_plugin_name()}] Reading archive '{input_path}' using StorageAdapter...")
            try:
                archive_content_bytes = storage_adapter.read_bytes(input_path)
                with open(local_archive_path, 'wb') as f:
                    f.write(archive_content_bytes)
            except Exception as e:
                raise RuntimeError(f"Failed to read archive: {str(e)}")

            extracted_files_rel_paths = []
            logger.info(f"[{self.get_plugin_name()}] Extracting archive '{local_archive_path}'...")

            try:
                if zipfile.is_zipfile(local_archive_path):
                    with zipfile.ZipFile(local_archive_path, 'r') as zip_ref:
                        zip_ref.extractall(local_extraction_dir)
                        extracted_files_rel_paths = [name for name in zip_ref.namelist() if not name.endswith('/')]
                elif tarfile.is_tarfile(local_archive_path):
                    with tarfile.open(local_archive_path, 'r:*') as tar_ref:
                        tar_ref.extractall(local_extraction_dir)
                        extracted_files_rel_paths = [m.name for m in tar_ref.getmembers() if m.isfile()]
                else:
                    raise ValueError("Unsupported archive format (not zip or tar).")
            except Exception as e:
                raise RuntimeError(f"Extraction failed: {str(e)}")

            if not extracted_files_rel_paths:
                raise RuntimeError("No files extracted from archive.")

            logger.info(f"[{self.get_plugin_name()}] Uploading {len(extracted_files_rel_paths)} files to '{output_path}'...")
            for rel_path in extracted_files_rel_paths:
                parts = rel_path.split(os.sep)
                final_rel_path = os.path.join(*parts[strip_components:]) if len(parts) > strip_components else os.path.basename(rel_path)
                local_full_path = os.path.join(local_extraction_dir, rel_path)
                final_dest_path = os.path.join(output_path.rstrip('/'), final_rel_path).replace('\\', '/')

                try:
                    storage_adapter.upload_local_file(local_full_path, final_dest_path)
                    container.add_file_path(final_dest_path)
                except Exception as e:
                    logger.warning(f"[{self.get_plugin_name()}] Failed to upload '{local_full_path}': {e}")
                    container.add_error(f"Upload failed for '{rel_path}': {str(e)}")

        return self.finalize_container(
            container,
            metadata={"extracted_from": input_path}
        )
