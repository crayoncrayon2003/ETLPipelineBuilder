import os
from typing import Dict, Any, Union, List, Optional
import pluggy

from core.infrastructure import storage_adapter
from core.data_container.container import DataContainer

from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

hookimpl = pluggy.HookimplMarker("etl_framework")

class DuplicateRemover:
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
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = str(params.get("input_path"))
        output_path = str(params.get("output_path"))
        subset: Union[List[str], None] = params.get("subset")
        keep: Union[str, bool] = params.get("keep", "first")

        if not input_path or not output_path:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'input_path' and 'output_path'.")

        # Use the storage adapter to read from local or S3
        df = storage_adapter.read_df(input_path)

        initial_row_count = len(df)
        logger.info(f"Initial rows: {initial_row_count}")

        deduplicated_df = df.drop_duplicates(subset=subset, keep=keep, inplace=False)
        rows_removed = initial_row_count - len(deduplicated_df)
        logger.info(f"Removed {rows_removed} duplicate rows. Saving to '{output_path}'.")

        # Use the storage adapter to write to local or S3
        storage_adapter.write_df(deduplicated_df, output_path)

        output_container = DataContainer()
        output_container.add_file_path(output_path)
        return output_container