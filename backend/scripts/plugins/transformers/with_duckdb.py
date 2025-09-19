import os
import duckdb
import pandas as pd
from typing import Dict, Any
import pluggy

from core.data_container.container import DataContainer, DataContainerStatus
from core.infrastructure.storage_adapter import storage_adapter
from core.plugin_manager.base_plugin import BasePlugin

from utils.logger import setup_logger

os.environ["HOME"] = "/tmp"
os.environ["DUCKDB_TMPDIR"] = "/tmp/duckdb_cache"

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

hookimpl = pluggy.HookimplMarker("etl_framework")

class DuckDBTransformer(BasePlugin):
    """
    (Storage Aware) Transforms data using a SQL query powered by DuckDB.
    Reads input data into a pandas DataFrame using StorageAdapter,
    then registers it with DuckDB for transformation.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "with_duckdb"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {
                    "type": "string",
                    "title": "Input File Path (local/s3)",
                    "description": "The file to be read by StorageAdapter and registered as a table in DuckDB."
                },
                "input_encoding": {
                    "type": "string",
                    "title": "Input File Encoding",
                    "default": "utf-8"
                },
                "output_path": {
                    "type": "string",
                    "title": "Output File Path (local/s3)"
                },
                "query_file": {
                    "type": "string",
                    "title": "Query File Path (local/s3)"
                },
                "table_name": {
                    "type": "string",
                    "title": "Table Name for Input",
                    "default": "source_data"
                }
            },
            "required": ["input_path", "output_path", "query_file"]
        }

    def _get_query(self, path: str) -> str:
        return storage_adapter.read_text(path)

    @hookimpl
    def execute(self, input_data: DataContainer) -> DataContainer:
        input_path = str(self.params.get("input_path"))
        input_encoding = str(self.params.get("input_encoding", "utf-8"))
        output_path = str(self.params.get("output_path"))
        query_path = str(self.params.get("query_file"))
        table_name = str(self.params.get("table_name", "source_data"))

        container = DataContainer()

        try:
            sql_query = self._get_query(query_path)
            logger.info(f"Connecting to DuckDB in-memory database.")
            con = duckdb.connect(database=':memory:')

            logger.info(f"Reading input file '{input_path}' with encoding '{input_encoding}'.")
            input_df = storage_adapter.read_df(input_path, read_options={"encoding": input_encoding})

            con.register(table_name, input_df)
            logger.info(f"Executing SQL query:\n{sql_query}")
            result_df = con.execute(sql_query).fetch_df()
            con.close()
            logger.info(f"Query executed. Result has {len(result_df)} rows.")
        except Exception as e:
            logger.error(f"DuckDB transformation failed: {e}", exc_info=True)
            container.set_status(DataContainerStatus.ERROR)
            container.add_error(str(e))
            return container

        try:
            storage_adapter.write_df(result_df, output_path)
            logger.info(f"Result saved to '{output_path}'.")
        except Exception as e:
            logger.error(f"Failed to write output file: {e}", exc_info=True)
            container.set_status(DataContainerStatus.ERROR)
            container.add_error(str(e))
            return container

        container.set_status(DataContainerStatus.SUCCESS)
        container.add_file_path(output_path)
        container.metadata.update({
            "input_path": input_path,
            "query_file": query_path,
            "table_name": table_name,
            "rows_output": len(result_df)
        })
        container.add_history(self.get_plugin_name())
        return container
