import os
import pluggy
from typing import Dict, Any, Optional

from core.data_container.container import DataContainer
from core.infrastructure import storage_adapter

from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

hookimpl = pluggy.HookimplMarker("etl_framework")

class NullHandler:
    """
    (Storage Aware) Handles missing values in a tabular file (local or S3),
    preserving the original format.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "null_handler"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {"type": "string", "title": "Input File Path (local or s3://)"},
                "output_path": {"type": "string", "title": "Output File Path (local or s3://)"},
                "strategy": {"type": "string", "title": "Strategy", "enum": ["drop_row", "fill"]},
                "subset": {"type": "array", "title": "Subset of Columns (for drop_row)", "items": {"type": "string"}},
                "value": {"title": "Fill Value (for fill)"}
            },
            "required": ["input_path", "output_path", "strategy"]
        }

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = str(params.get("input_path"))
        output_path = str(params.get("output_path"))
        strategy = params.get("strategy")

        if not all([input_path, output_path, strategy]):
            raise ValueError("Plugin requires 'input_path', 'output_path', and 'strategy'.")

        df = storage_adapter.read_df(input_path)

        initial_null_counts = df.isnull().sum().sum()
        logger.info(f"Initial total nulls: {initial_null_counts}")

        processed_df = df.copy()
        if strategy == 'drop_row':
            processed_df.dropna(axis=0, subset=params.get("subset"), inplace=True)
        elif strategy == 'fill':
            processed_df.fillna(value=params.get("value"), method=params.get("method"), inplace=True)
        else:
            raise ValueError(f"Unsupported strategy: '{strategy}'.")

        final_null_counts = processed_df.isnull().sum().sum()
        logger.info(f"Null handling complete. Final nulls: {final_null_counts}. Saving to '{output_path}'.")

        storage_adapter.write_df(processed_df, output_path)

        output_container = DataContainer()
        output_container.add_file_path(output_path)
        return output_container