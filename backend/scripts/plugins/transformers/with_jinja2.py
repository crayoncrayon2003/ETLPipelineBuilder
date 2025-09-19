import os
import json
import pandas as pd
from typing import Dict, Any
import pluggy

from jinja2 import Environment, FileSystemLoader

from core.data_container.container import DataContainer, DataContainerStatus
from core.infrastructure import storage_adapter
from core.plugin_manager.base_plugin import BasePlugin

from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

hookimpl = pluggy.HookimplMarker("etl_framework")

class Jinja2Transformer(BasePlugin):
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
                    "title": "Input File Path (local/s3)"
                },
                "output_path": {
                    "type": "string",
                    "title": "Output Text File Path (local/s3)"
                },
                "template_path": {
                    "type": "string",
                    "title": "Jinja2 Template Path (local)"
                }
            },
            "required": ["input_path", "output_path", "template_path"]
        }

    @hookimpl
    def execute(self, input_data: DataContainer) -> DataContainer:
        input_path = str(self.params.get("input_path"))
        output_path = str(self.params.get("output_path"))
        template_path = str(self.params.get("template_path"))

        container = DataContainer()

        if not all([input_path, output_path, template_path]):
            container.set_status(DataContainerStatus.ERROR)
            container.add_error("Missing required parameters: 'input_path', 'output_path', 'template_path'.")
            return container

        if not os.path.exists(template_path):
            container.set_status(DataContainerStatus.ERROR)
            container.add_error(f"Template file not found: {template_path}")
            return container

        try:
            df = storage_adapter.read_df(input_path)
        except Exception as e:
            container.set_status(DataContainerStatus.ERROR)
            container.add_error(f"Failed to read input file: {str(e)}")
            return container

        template_dir = os.path.dirname(template_path)
        template_file = os.path.basename(template_path)
        env = Environment(loader=FileSystemLoader(template_dir), trim_blocks=True, lstrip_blocks=True)

        try:
            template = env.get_template(template_file)
        except Exception as e:
            container.set_status(DataContainerStatus.ERROR)
            container.add_error(f"Failed to load Jinja2 template: {str(e)}")
            return container

        records = df.to_dict(orient='records')
        output_lines = []

        for record in records:
            try:
                rendered_string = template.render(record)
                json_object = json.loads(rendered_string)
                output_lines.append(json.dumps(json_object))
            except Exception as e:
                logger.error(f"Template rendering error for record: {record}. Error: {e}")
                output_lines.append(json.dumps({"error": str(e), "source_record": record}))

        full_output_text = "\n".join(output_lines)

        try:
            storage_adapter.write_text(full_output_text, output_path)
        except Exception as e:
            container.set_status(DataContainerStatus.ERROR)
            container.add_error(f"Failed to write output file: {str(e)}")
            return container

        container.set_status(DataContainerStatus.SUCCESS)
        container.add_file_path(output_path)
        container.metadata.update({
            "input_path": input_path,
            "template_path": template_path,
            "records_processed": len(records)
        })
        container.add_history(self.get_plugin_name())
        return container
