import os
from typing import Dict, Any, Optional, List
import pluggy

from core.data_container.container import DataContainer
from core.infrastructure import storage_adapter
from core.plugin_manager.base_plugin import BasePlugin

from utils.logger import setup_logger

logger = setup_logger(__name__)

hookimpl = pluggy.HookimplMarker("etl_framework")

class NullHandler(BasePlugin):
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
                "value": {"title": "Fill Value (for fill)"},
                "method": {"type": "string", "title": "Fill Method (Optional)"}
            },
            "required": ["input_path", "output_path", "strategy"]
        }

    def run(self, input_data: DataContainer, container: DataContainer) -> DataContainer:
        input_path = str(self.params.get("input_path"))
        output_path = str(self.params.get("output_path"))
        strategy = self.params.get("strategy")
        subset: Optional[List[str]] = self.params.get("subset")
        fill_value = self.params.get("value")
        fill_method = self.params.get("method")

        if not all([input_path, output_path, strategy]):
            raise ValueError("Missing required parameters: 'input_path', 'output_path', and 'strategy'.")

        try:
            df = storage_adapter.read_df(input_path)
        except Exception as e:
            raise RuntimeError(f"Failed to read input file: {str(e)}")

        initial_nulls = df.isnull().sum().sum()
        logger.info(f"[{self.get_plugin_name()}] Initial total nulls: {initial_nulls}")

        processed_df = df.copy()
        try:
            if strategy == 'drop_row':
                processed_df.dropna(axis=0, subset=subset, inplace=True)
            elif strategy == 'fill':
                processed_df.fillna(value=fill_value, method=fill_method, inplace=True)
            else:
                raise ValueError(f"Unsupported strategy: '{strategy}'")
        except Exception as e:
            raise RuntimeError(f"Null handling failed: {str(e)}")

        final_nulls = processed_df.isnull().sum().sum()
        logger.info(f"[{self.get_plugin_name()}] Final total nulls: {final_nulls}. Saving to '{output_path}'.")

        try:
            storage_adapter.write_df(processed_df, output_path)
            logger.info(f"[{self.get_plugin_name()}] File successfully saved to '{output_path}'.")
        except Exception as e:
            raise RuntimeError(f"Failed to write output file: {str(e)}")

        return self.finalize_container(
            container,
            output_path=output_path,
            metadata={
                "input_path": input_path,
                "strategy": strategy,
                "initial_nulls": int(initial_nulls),
                "final_nulls": int(final_nulls)
            }
        )
