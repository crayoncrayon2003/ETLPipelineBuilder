# backend/plugins/transformers/with_duckdb.py

import duckdb
from typing import Dict, Any, Optional
import pluggy
from pathlib import Path

from core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class DuckDBTransformer:
    """
    (File-based) Transforms data in a file using a SQL query powered by DuckDB.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "with_duckdb"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {"type": "string", "title": "Input File Path (Optional)"},
                "output_path": {"type": "string", "title": "Output File Path"},
                # "query": {"type": "string", "title": "SQL Query", "format": "textarea"},
                "query_file": {"type": "string", "title": "Query File"},
                "table_name": {"type": "string", "title": "Table Name for Input", "default": "source_data"}
            },
            "required": ["output_path"]
        }

    def _get_query(self, params: Dict[str, Any]) -> str:
        if params.get("query"): return params.get("query")
        query_file_path = params.get("query_file")
        if not query_file_path: raise ValueError("Requires 'query' or 'query_file'.")
        try:
            with open(query_file_path, 'r', encoding='utf-8') as f: return f.read()
        except FileNotFoundError: raise FileNotFoundError(f"Query file not found: {query_file_path}")

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        output_path = Path(params.get("output_path"))
        sql_query = self._get_query(params)
        input_path_str = params.get("input_path")

        if not output_path:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'output_path'.")

        print(f"Executing DuckDB transformation. Output: '{output_path}'.")

        try:
            con = duckdb.connect(database=':memory:')

            if input_path_str:
                input_path = Path(input_path_str)
                if not input_path.exists(): raise FileNotFoundError(f"Input file not found: {input_path}")
                table_name = params.get("table_name", "source_data")

                # --- Start of Modification: Use correct read function based on suffix ---
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
                # --- End of Modification ---


            result_df = con.execute(sql_query).fetch_df()

        except Exception as e:
            print(f"ERROR during DuckDB transformation: {e}")
            raise
        finally:
            if 'con' in locals() and con: con.close()

        print(f"Transformation complete. Result has {len(result_df)} rows. Saving to '{output_path}'.")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        # Output format is determined by the output_path suffix
        output_suffix = output_path.suffix.lower()
        if output_suffix == '.parquet':
            result_df.to_parquet(output_path, index=False)
        elif output_suffix == '.csv':
            result_df.to_csv(output_path, index=False)
        else:
            raise ValueError(f"Unsupported output file type for DuckDB plugin: {output_suffix}")

        output_container = DataContainer()
        output_container.add_file_path(output_path)
        return output_container