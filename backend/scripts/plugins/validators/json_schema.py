import os
import json
import pandas as pd
from typing import Dict, Any, List
import pluggy

import jsonschema

from core.data_container.container import DataContainer, DataContainerStatus
from core.infrastructure import storage_adapter
from core.plugin_manager.base_plugin import BasePlugin

from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

hookimpl = pluggy.HookimplMarker("etl_framework")

class JsonSchemaValidator(BasePlugin):
    """
    (Storage Aware) Validates a column in a file (local or S3) against a JSON Schema.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "json_schema"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {"type": "string", "title": "Input File Path (local/s3)"},
                "output_path": {"type": "string", "title": "Output File Path (local/s3)"},
                "schema_path": {"type": "string", "title": "JSON Schema Path (local)"},
                "target_column": {"type": "string", "title": "Target Column"}
            },
            "required": ["input_path", "output_path", "schema_path", "target_column"]
        }

    @hookimpl
    def execute(self, input_data: DataContainer) -> DataContainer:
        input_path = str(self.params.get("input_path"))
        output_path = str(self.params.get("output_path"))
        schema_path = self.params.get("schema_path")
        target_column = self.params.get("target_column")

        container = DataContainer()

        if not all([input_path, output_path, schema_path, target_column]):
            container.set_status(DataContainerStatus.ERROR)
            container.add_error("Missing required parameters.")
            return container

        if not os.path.isfile(schema_path):
            container.set_status(DataContainerStatus.ERROR)
            container.add_error(f"JSON Schema file not found: {schema_path}")
            return container

        try:
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)
            jsonschema.Draft7Validator.check_schema(schema)
            validator = jsonschema.Draft7Validator(schema)
        except Exception as e:
            container.set_status(DataContainerStatus.ERROR)
            container.add_error(f"Invalid JSON Schema: {str(e)}")
            return container

        try:
            df = storage_adapter.read_df(input_path)
        except Exception as e:
            container.set_status(DataContainerStatus.ERROR)
            container.add_error(f"Failed to read input file: {str(e)}")
            return container

        if target_column not in df.columns:
            container.set_status(DataContainerStatus.ERROR)
            container.add_error(f"Target column '{target_column}' not found in input.")
            return container

        errors: List[str] = []
        for index, record in df[target_column].items():
            instance = record
            if isinstance(record, str):
                try:
                    instance = json.loads(record)
                except json.JSONDecodeError:
                    errors.append(f"Row {index}: Invalid JSON string.")
                    continue

            validation_errors = sorted(validator.iter_errors(instance), key=lambda e: e.path)
            if validation_errors:
                details = ", ".join([f"{'/'.join(map(str, e.path))}: {e.message}" for e in validation_errors])
                errors.append(f"Row {index}: Validation failed. Details: {details}")

        if errors:
            container.set_status(DataContainerStatus.ERROR)
            container.add_error(f"JSON Schema validation failed for {len(errors)} records:\n- " + "\n- ".join(errors))
            return container

        try:
            storage_adapter.copy_file(input_path, output_path)
        except Exception as e:
            container.set_status(DataContainerStatus.ERROR)
            container.add_error(f"Failed to copy file: {str(e)}")
            return container

        container.set_status(DataContainerStatus.SUCCESS)
        container.add_file_path(output_path)
        container.metadata.update({
            "input_path": input_path,
            "schema_path": schema_path,
            "target_column": target_column,
            "records_validated": len(df),
            "validation_passed": True
        })
        container.add_history(self.get_plugin_name())
        return container
