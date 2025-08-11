# backend/plugins/transformers/with_jinja2.py

import json
from pathlib import Path
from typing import Dict, Any, Optional
from jinja2 import Environment, FileSystemLoader
import pandas as pd
import pluggy

from core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class Jinja2Transformer:
    """
    (Generic Transformer) Transforms rows from a tabular file into a
    structured text file using a Jinja2 template.
    The template itself holds all knowledge of the output format.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "with_jinja2"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {
                    "type": "string",
                    "title": "Input Table File Path (CSV/Parquet)",
                    "description": "The source file containing rows to be transformed."
                },
                "output_path": {
                    "type": "string",
                    "title": "Output Text File Path",
                    "description": "Path to save the rendered text output (e.g., a .jsonl file)."
                },
                # "template": {"type": "string", "title": "Jinja2 template", "format": "textarea"},
                "template_path": {
                    "type": "string",
                    "title": "Jinja2 Template Path",
                    "description": "The path to the .j2 template file."
                }
            },
            "required": ["input_path", "output_path", "template_path"]
        }

    def _read_file(self, path: Path) -> pd.DataFrame:
        suffix = path.suffix.lower()
        if suffix == '.csv': return pd.read_csv(path)
        elif suffix == '.parquet': return pd.read_parquet(path)
        else: raise ValueError(f"Unsupported input file type for with_jinja2: {suffix}")

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = Path(params.get("input_path"))
        output_path = Path(params.get("output_path"))
        template_path = Path(params.get("template_path"))

        if not all([input_path, output_path, template_path]):
            raise ValueError("Plugin requires 'input_path', 'output_path', and 'template_path'.")
        if not input_path.exists(): raise FileNotFoundError(f"Input file not found: {input_path}")
        if not template_path.exists(): raise FileNotFoundError(f"Template file not found: {template_path}")

        df = self._read_file(input_path)

        env = Environment(loader=FileSystemLoader(str(template_path.parent)), trim_blocks=True, lstrip_blocks=True)
        template = env.get_template(template_path.name)

        records = df.to_dict(orient='records')

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            for record in records:
                # The context is simply the row from the DataFrame.
                # The plugin has no knowledge of 'entity_type' or 'id'.
                try:
                    rendered_string = template.render(record)
                    json_object = json.loads(rendered_string)
                    f.write(json.dumps(json_object) + '\n')
                except Exception as e:
                    print(f"ERROR rendering template for record: {record}. Error: {e}")
                    f.write(json.dumps({"error": str(e), "source_record": record}) + '\n')

        print(f"Transformation of {len(records)} records complete. Output: '{output_path}'.")
        output_container = DataContainer()
        output_container.add_file_path(output_path)
        return output_container