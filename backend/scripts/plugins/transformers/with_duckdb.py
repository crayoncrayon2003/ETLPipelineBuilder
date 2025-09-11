import os
import duckdb
from typing import Dict, Any, Optional
import pluggy
import pandas as pd

from core.data_container.container import DataContainer
from core.infrastructure.storage_adapter import storage_adapter

from utils.logger import setup_logger

os.environ["HOME"] = "/tmp"
os.environ["DUCKDB_TMPDIR"] = "/tmp/duckdb_cache"

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

hookimpl = pluggy.HookimplMarker("etl_framework")

class DuckDBTransformer:
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
                    "description": "The character encoding of the input file (e.g., 'utf-8', 'shift_jis', 'cp932', 'latin1').",
                    "default": "utf-8"
                },
                "output_path": {
                    "type": "string",
                    "title": "Output File Path (local/s3)",
                    "description": "Path to save the result of the SQL query."
                },
                "query_file": {
                    "type": "string",
                    "title": "Query File Path (local/s3)",
                    "description": "Path to a file containing the SQL query. Can be local or S3."
                },
                "table_name": {
                    "type": "string",
                    "title": "Table Name for Input",
                    "description": "The name to use for the input table in the SQL query.",
                    "default": "source_data"
                }
            },
            "required": ["input_path", "output_path", "query_file"]
        }

    def _get_query(self, path: str) -> str:
        """Delegates reading query file to StorageAdapter (S3 or local)."""
        return storage_adapter.read_text(path)

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = str(params.get("input_path"))
        input_encoding = str(params.get("input_encoding", "utf-8"))
        output_path = str(params.get("output_path"))
        query_path = str(params.get("query_file"))
        table_name = str(params.get("table_name", "source_data"))

        sql_query = self._get_query(query_path)
        logger.info(f"input_path : {input_path}")
        logger.info(f"output_path: {output_path}")

        con = None
        try:
            logger.info(f"Connecting to in-memory DuckDB database.")
            con = duckdb.connect(database=':memory:')

            logger.info(f"Loading input file '{os.path.basename(input_path)}' into pandas DataFrame using StorageAdapter.")
            input_df: pd.DataFrame = storage_adapter.read_df(input_path, read_options={"encoding": input_encoding})

            # Register as a pandas DataFrame in DuckDB
            con.register(table_name, input_df)
            logger.info(f"Registered '{os.path.basename(input_path)}' as table '{table_name}' from in-memory pandas DataFrame ({len(input_df)} rows).")

            logger.info(f"Executing SQL query:\n{sql_query}")
            result_df = con.execute(sql_query).fetch_df()
            logger.info(f"SQL query executed. Result has {len(result_df)} rows.")

        except Exception as e:
            logger.error(f"ERROR during DuckDB transformation: {e}", exc_info=True)
            raise
        finally:
            if con:
                con.close()
                logger.info("DuckDB connection closed.")

        # Output the result using the StorageAdapter
        storage_adapter.write_df(result_df, output_path)
        logger.info(f"Transformation complete. Result saved to '{output_path}'.")

        output_container = DataContainer()
        output_container.add_file_path(output_path)
        return output_container
