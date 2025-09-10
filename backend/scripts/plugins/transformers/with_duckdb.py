import os
import duckdb
from typing import Dict, Any, Optional
import pluggy
from pathlib import Path

from core.data_container.container import DataContainer
from core.infrastructure import storage_adapter

from utils.logger import setup_logger

os.environ["HOME"] = "/tmp"
os.environ["DUCKDB_TMPDIR"] = "/tmp/duckdb_cache"

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

hookimpl = pluggy.HookimplMarker("etl_framework")

class DuckDBTransformer:
    """
    (Storage Aware) Transforms data using a SQL query powered by DuckDB.
    Can read from and write to various formats on local or S3 storage.
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
                    "title": "Input File Path (Optional, local/s3)",
                    "description": "The file to be registered as a table. Not needed if the query reads files directly."
                },
                "output_path": {
                    "type": "string",
                    "title": "Output File Path (local/s3)",
                    "description": "Path to save the result of the SQL query."
                },
                # "query": {
                #     "type": "string",
                #     "title": "SQL Query",
                #     "description": "The SQL query to execute. Use this or 'Query File'.",
                #     "format": "textarea"
                # },
                "query_file": {
                    "type": "string",
                    "title": "Query File Path (local)",
                    "description": "Local path to a file containing the SQL query."
                },
                "table_name": {
                    "type": "string",
                    "title": "Table Name for Input",
                    "description": "The name to use for the input table in the SQL query.",
                    "default": "source_data"
                }
            },
            "required": ["output_path"]
        }

    def _get_query(self, path: str) -> str:
        """Delegates reading query file to StorageAdapter (S3 or local)."""
        return storage_adapter.read_text(path)

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = str(params.get("input_path"))
        output_path = str(params.get("output_path"))
        query_path = str(params.get("query_file"))
        table_name = str(params.get("table_name", "source_data"))

        sql_query = self._get_query(query_path)

        try:
            con = duckdb.connect(database=':memory:')
            con.execute("INSTALL httpfs;")
            con.execute("LOAD httpfs;")
            con.execute("CREATE SECRET s3_access_secret (TYPE S3, PROVIDER CREDENTIAL_CHAIN, CHAIN 'config', REGION 'ap-northeast-1');")

            if input_path:
                read_expr = f"read_csv_auto('{input_path}')" if input_path.endswith('.csv') \
                    else f"read_parquet('{input_path}')" if input_path.endswith('.parquet') \
                    else f"read_json_auto('{input_path}')" if input_path.endswith('.json') or input_path.endswith('.jsonl') \
                    else None

                if not read_expr:
                    raise ValueError(f"Unsupported file type for input: {input_path}")

                con.execute(f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM {read_expr};")
                logger.info(f"Registered '{Path(input_path).name}' as table '{table_name}'.")

            result_df = con.execute(sql_query).fetch_df()

        except Exception as e:
            logger.error(f"ERROR during DuckDB transformation: {e}")
            raise
        finally:
            if 'con' in locals() and con: con.close()

        # Use the StorageAdapter to write the result, which handles both local and S3.
        storage_adapter.write_df(result_df, output_path)
        logger.info(f"Transformation complete. Result with {len(result_df)} rows saved.")

        output_container = DataContainer()
        output_container.add_file_path(output_path)
        return output_container