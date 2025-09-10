import json
import os
from typing import Dict, Any, Optional
from jinja2 import Environment, FileSystemLoader
import pandas as pd
import pluggy

from core.data_container.container import DataContainer
from core.infrastructure import storage_adapter

hookimpl = pluggy.HookimplMarker("etl_framework")

class Jinja2Transformer:
    """
    (Storage Aware) Transforms rows from a tabular file (local or S3) into a
    structured text file (local or S3) using a Jinja2 template.
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
                    "title": "Input File Path (local/s3)",
                    "description": "The source file containing rows to be transformed."
                },
                "output_path": {
                    "type": "string",
                    "title": "Output Text File Path (local/s3)",
                    "description": "Path to save the rendered text output (e.g., a .jsonl file)."
                },
                "template_path": {
                    "type": "string",
                    "title": "Jinja2 Template Path (local)",
                    "description": "The local path to the .j2 template file."
                }
            },
            "required": ["input_path", "output_path", "template_path"]
        }

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = str(params.get("input_path"))
        output_path = str(params.get("output_path"))
        template_path = str(params.get("template_path")) # Template is always local

        if not all([input_path, output_path, template_path]):
            raise ValueError("Plugin requires 'input_path', 'output_path', and 'template_path'.")
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input file not found: {input_path}")
        if not os.path.exists(template_path):
            raise FileNotFoundError(f"Template file not found: {template_path}")

        # Use StorageAdapter to read the input from local or S3
        df = storage_adapter.read_df(input_path)

        template_dir = os.path.dirname(template_path)
        template_file = os.path.basename(template_path)
        env = Environment(loader=FileSystemLoader(template_dir), trim_blocks=True, lstrip_blocks=True)
        template = env.get_template(template_file)

        records = df.to_dict(orient='records')

        # Build the full text content in memory first
        output_lines = []
        for record in records:
            try:
                rendered_string = template.render(record)
                # Ensure each line is a valid JSON object for JSONL format
                json_object = json.loads(rendered_string)
                output_lines.append(json.dumps(json_object))
            except Exception as e:
                print(f"ERROR rendering template for record: {record}. Error: {e}")
                output_lines.append(json.dumps({"error": str(e), "source_record": record}))

        full_output_text = "\n".join(output_lines)

        # Use StorageAdapter to write the final text file to local or S3
        storage_adapter.write_text(full_output_text, output_path)

        print(f"Transformation of {len(records)} records complete. Output: '{output_path}'.")
        output_container = DataContainer()
        output_container.add_file_path(output_path)
        return output_container