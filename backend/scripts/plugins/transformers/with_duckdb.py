import duckdb
from typing import Dict, Any, Optional
import pluggy
from pathlib import Path

from core.data_container.container import DataContainer
from core.infrastructure import storage_adapter

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

    def _get_query(self, params: Dict[str, Any]) -> str:
        """Loads the SQL query from a string or a local file."""
        if params.get("query"):
            return params.get("query")

        query_file_path = params.get("query_file")
        if not query_file_path:
            raise ValueError("DuckDBTransformer requires either 'query' or 'query_file' parameter.")

        try:
            # Query files are always read from the local filesystem where the code runs.
            with open(query_file_path, 'r', encoding='utf-8') as f:
                return f.read()
        except FileNotFoundError:
            raise FileNotFoundError(f"Query file not found at: {query_file_path}")

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        output_path = str(params.get("output_path"))
        sql_query = self._get_query(params)
        input_path = str(params.get("input_path")) if params.get("input_path") else None

        if not output_path:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'output_path'.")

        try:
            con = duckdb.connect(database=':memory:')

            if input_path:
                input_path = Path(input_path)
                if not input_path.exists(): raise FileNotFoundError(f"Input file not found: {input_path}")
                table_name = params.get("table_name", "source_data")

                suffix = input_path.suffix.lower()
                read_function = ""
                if suffix == '.csv':
                    read_function = f"read_csv_auto('{str(input_path)}')"
                elif suffix == '.parquet':
                    read_function = f"read_parquet('{str(input_path)}')"
                elif suffix == '.json' or suffix == '.jsonl':
                    read_function = f"read_json_auto('{str(input_path)}')"
                else:
                    raise ValueError(f"Unsupported input file type for DuckDB plugin: {suffix}")

                con.execute(f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM {read_function};")
                print(f"Registered '{input_path.name}' as table '{table_name}'.")

            result_df = con.execute(sql_query).fetch_df()

        except Exception as e:
            print(f"ERROR during DuckDB transformation: {e}")
            raise
        finally:
            if 'con' in locals() and con: con.close()

        # Use the StorageAdapter to write the result, which handles both local and S3.
        storage_adapter.write_df(result_df, output_path)
        print(f"Transformation complete. Result with {len(result_df)} rows saved.")

        output_container = DataContainer()
        output_container.add_file_path(output_path)
        return output_container