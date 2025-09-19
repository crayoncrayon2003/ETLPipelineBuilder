import os
from typing import Dict, Any, Optional, List
import pluggy

from core.data_container.container import DataContainer, DataContainerStatus
from core.infrastructure import storage_adapter
from core.plugin_manager.base_plugin import BasePlugin

from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

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

    @hookimpl
    def execute(self, input_data: DataContainer) -> DataContainer:
        input_path = str(self.params.get("input_path"))
        output_path = str(self.params.get("output_path"))
        strategy = self.params.get("strategy")
        subset: Optional[List[str]] = self.params.get("subset")
        fill_value = self.params.get("value")
        fill_method = self.params.get("method")

        container = DataContainer()

        if not all([input_path, output_path, strategy]):
            container.set_status(DataContainerStatus.ERROR)
            container.add_error("Missing required parameters: 'input_path', 'output_path', and 'strategy'.")
            return container

        try:
            df = storage_adapter.read_df(input_path)
        except Exception as e:
            container.set_status(DataContainerStatus.ERROR)
            container.add_error(f"Failed to read input file: {str(e)}")
            return container

        initial_nulls = df.isnull().sum().sum()
        logger.info(f"Initial total nulls: {initial_nulls}")

        processed_df = df.copy()
        try:
            if strategy == 'drop_row':
                processed_df.dropna(axis=0, subset=subset, inplace=True)
            elif strategy == 'fill':
                processed_df.fillna(value=fill_value, method=fill_method, inplace=True)
            else:
                container.set_status(DataContainerStatus.ERROR)
                container.add_error(f"Unsupported strategy: '{strategy}'")
                return container
        except Exception as e:
            container.set_status(DataContainerStatus.ERROR)
            container.add_error(f"Null handling failed: {str(e)}")
            return container

        final_nulls = processed_df.isnull().sum().sum()
        logger.info(f"Final total nulls: {final_nulls}. Saving to '{output_path}'.")

        try:
            storage_adapter.write_df(processed_df, output_path)
        except Exception as e:
            container.set_status(DataContainerStatus.ERROR)
            container.add_error(f"Failed to write output file: {str(e)}")
            return container

        container.set_status(DataContainerStatus.SUCCESS)
        container.add_file_path(output_path)
        container.metadata.update({
            "input_path": input_path,
            "strategy": strategy,
            "initial_nulls": int(initial_nulls),
            "final_nulls": int(final_nulls)
        })
        container.add_history(self.get_plugin_name())
        return container
