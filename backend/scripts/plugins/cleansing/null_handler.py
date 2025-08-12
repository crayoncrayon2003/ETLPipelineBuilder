from typing import Dict, Any, Optional
import pluggy
import pandas as pd
from pathlib import Path

from core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class NullHandler:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "null_handler"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {"type": "string", "title": "Input File Path"},
                "output_path": {"type": "string", "title": "Output File Path"},
                "strategy": {"type": "string", "title": "Strategy", "enum": ["drop_row", "fill"]},
                "subset": {"type": "array", "title": "Subset of Columns (for drop_row)", "items": {"type": "string"}},
                "value": {"title": "Fill Value (for fill)"}
            },
            "required": ["input_path", "output_path", "strategy"]
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
        strategy = params.get("strategy")

        if not all([input_path, output_path, strategy]):
            raise ValueError("Plugin requires 'input_path', 'output_path', and 'strategy'.")
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found at: {input_path}")

        print(f"Reading '{input_path}' to handle nulls with strategy '{strategy}'...")
        df = self._read_file(input_path)

        processed_df = df.copy()
        if strategy == 'drop_row':
            processed_df.dropna(axis=0, subset=params.get("subset"), inplace=True)
        elif strategy == 'fill':
            processed_df.fillna(value=params.get("value"), method=params.get("method"), inplace=True)
        else:
            raise ValueError(f"Unsupported strategy: '{strategy}'.")

        print(f"Null handling complete. Saving to '{output_path}'.")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        self._write_file(processed_df, output_path)

        output_container = DataContainer()
        output_container.add_file_path(output_path)
        return output_container