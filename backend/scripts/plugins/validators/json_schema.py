import os
import json
from typing import Dict, Any, List, Optional
import jsonschema
import pandas as pd
import pluggy

from core.data_container.container import DataContainer
from core.infrastructure import storage_adapter

from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

hookimpl = pluggy.HookimplMarker("etl_framework")

class JsonSchemaValidator:
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
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = str(params.get("input_path"))
        output_path = str(params.get("output_path"))
        schema_path = params.get("schema_path")
        target_column = params.get("target_column")

        if not all([input_path, output_path, schema_path, target_column]):
            raise ValueError("Plugin requires 'input_path', 'output_path', 'schema_path', 'target_column'.")

        if not os.path.isfile(schema_path):
            raise FileNotFoundError(f"JSON Schema not found: {schema_path}")

        try:
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)
            jsonschema.Draft7Validator.check_schema(schema)
            validator = jsonschema.Draft7Validator(schema)
        except Exception as e:
            raise ValueError(f"Invalid JSON Schema file '{schema_path}': {e}")

        df = storage_adapter.read_df(input_path)
        if target_column not in df.columns:
            raise KeyError(f"Target column '{target_column}' not found.")

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
                error_details = ", ".join([f"{'/'.join(map(str, e.path))}: {e.message}" for e in validation_errors])
                errors.append(f"Row {index}: Validation failed. Details: {error_details}")

        if errors:
            raise ValueError(f"JSON Schema validation failed for {len(errors)} records:\n- " + "\n- ".join(errors))

        logger.info("JSON Schema validation successful. Copying file to output path.")
        storage_adapter.copy_file(input_path, output_path)
        output_container = DataContainer()
        output_container.add_file_path(output_path)
        return output_container
