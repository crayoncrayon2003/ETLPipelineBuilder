import os
from typing import Dict, Any, Union, List
import pluggy

from core.infrastructure import storage_adapter
from core.data_container.container import DataContainer, DataContainerStatus
from core.plugin_manager.base_plugin import BasePlugin

from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

hookimpl = pluggy.HookimplMarker("etl_framework")

class DuplicateRemover(BasePlugin):
    """
    (Storage Aware) Removes duplicate rows from a tabular file (local or S3),
    preserving the original file format.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "duplicate_remover"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {
                    "type": "string",
                    "title": "Input File Path (local or s3://)",
                    "description": "The source file to process for duplicate removal."
                },
                "output_path": {
                    "type": "string",
                    "title": "Output File Path (local or s3://)",
                    "description": "Path to save the deduplicated data, in the same format as the input."
                },
                "subset": {
                    "type": "array",
                    "title": "Subset of Columns (Optional)",
                    "description": "List of column names to consider for identifying duplicates.",
                    "items": { "type": "string" }
                },
                "keep": {
                    "type": "string",
                    "title": "Which Duplicate to Keep",
                    "enum": ["first", "last", False],
                    "default": "first"
                }
            },
            "required": ["input_path", "output_path"]
        }

    @hookimpl
    def execute(self, input_data: DataContainer) -> DataContainer:
        input_path = str(self.params.get("input_path"))
        output_path = str(self.params.get("output_path"))
        subset: Union[List[str], None] = self.params.get("subset")
        keep: Union[str, bool] = self.params.get("keep", "first")

        container = DataContainer()

        if not input_path or not output_path:
            container.set_status(DataContainerStatus.ERROR)
            container.add_error("Missing required parameters: 'input_path' and 'output_path'.")
            return container

        try:
            df = storage_adapter.read_df(input_path)
        except Exception as e:
            container.set_status(DataContainerStatus.ERROR)
            container.add_error(f"Failed to read input file: {str(e)}")
            return container

        initial_row_count = len(df)
        logger.info(f"Initial rows: {initial_row_count}")

        try:
            deduplicated_df = df.drop_duplicates(subset=subset, keep=keep, inplace=False)
            rows_removed = initial_row_count - len(deduplicated_df)
            logger.info(f"Removed {rows_removed} duplicate rows. Saving to '{output_path}'.")

            storage_adapter.write_df(deduplicated_df, output_path)
        except Exception as e:
            container.set_status(DataContainerStatus.ERROR)
            container.add_error(f"Deduplication or saving failed: {str(e)}")
            return container

        container.set_status(DataContainerStatus.SUCCESS)
        container.add_file_path(output_path)
        container.metadata.update({
            "input_path": input_path,
            "rows_removed": rows_removed,
            "deduplicated": True
        })
        container.add_history(self.get_plugin_name())
        return container
