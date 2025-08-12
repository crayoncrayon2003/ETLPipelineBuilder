from typing import Dict, Any, Union, List, Optional
import pluggy
import pandas as pd
from pathlib import Path

from core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class DuplicateRemover:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "duplicate_remover"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {"type": "string", "title": "Input File Path"},
                "output_path": {"type": "string", "title": "Output File Path"},
                "subset": {"type": "array", "title": "Subset of Columns (Optional)", "items": { "type": "string" }},
                "keep": {"type": "string", "title": "Which Duplicate to Keep", "enum": ["first", "last", False], "default": "first"}
            },
            "required": ["input_path", "output_path"]
        }

    def _read_file(self, path: Path) -> pd.DataFrame:
        suffix = path.suffix.lower()
        if suffix == '.csv': return pd.read_csv(path)
        elif suffix == '.parquet': return pd.read_parquet(path)
        elif suffix in ['.xls', '.xlsx']: return pd.read_excel(path)
        else: raise ValueError(f"Unsupported file type: {suffix}")

    def _write_file(self, df: pd.DataFrame, path: Path):
        suffix = path.suffix.lower()
        if suffix == '.csv': df.to_csv(path, index=False)
        elif suffix == '.parquet': df.to_parquet(path, index=False)
        elif suffix in ['.xls', '.xlsx']: df.to_excel(path, index=False)
        else: raise ValueError(f"Unsupported output file type: {suffix}")

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = Path(params.get("input_path"))
        output_path = Path(params.get("output_path"))
        subset: Union[List[str], None] = params.get("subset")
        keep: Union[str, bool] = params.get("keep", "first")

        if not input_path or not output_path:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'input_path' and 'output_path'.")
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found at: {input_path}")

        print(f"Reading file '{input_path}' to remove duplicates...")
        df = self._read_file(input_path)

        initial_row_count = len(df)
        print(f"Initial rows: {initial_row_count}")

        deduplicated_df = df.drop_duplicates(subset=subset, keep=keep, inplace=False)
        rows_removed = initial_row_count - len(deduplicated_df)
        print(f"Removed {rows_removed} rows. Saving to '{output_path}'.")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        self._write_file(deduplicated_df, output_path)

        output_container = DataContainer()
        output_container.add_file_path(output_path)
        return output_container