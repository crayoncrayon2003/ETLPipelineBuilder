# backend/plugins/validators/json_schema.py

import json
from pathlib import Path
from typing import Dict, Any, List, Optional
import jsonschema
from jsonschema import Draft7Validator
import pandas as pd
import pluggy
import shutil

from core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class JsonSchemaValidator:
    """
    (File-based) Validates a column of JSON objects in a file against a JSON Schema.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "json_schema"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {"type": "string", "title": "Input File Path"},
                "output_path": {"type": "string", "title": "Output File Path"},
                "schema_path": {"type": "string", "title": "JSON Schema Path"},
                "target_column": {"type": "string", "title": "Target Column"}
            },
            "required": ["input_path", "output_path", "schema_path", "target_column"]
        }

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = Path(params.get("input_path"))
        output_path = Path(params.get("output_path"))
        schema_path = Path(params.get("schema_path"))
        target_column = params.get("target_column")
        stop_on_first_error = params.get("stop_on_first_error", True)

        if not all([input_path, output_path, schema_path, target_column]):
            raise ValueError("Plugin requires 'input_path', 'output_path', 'schema_path', and 'target_column'.")
        if not input_path.exists(): raise FileNotFoundError(f"Input file not found: {input_path}")
        if not schema_path.exists(): raise FileNotFoundError(f"JSON Schema not found: {schema_path}")

        print(f"Reading file '{input_path}' to validate column '{target_column}'...")
        df = pd.read_parquet(input_path)

        try:
            with schema_path.open('r', encoding='utf-8') as f: schema = json.load(f)
            Draft7Validator.check_schema(schema)
            validator = Draft7Validator(schema)
        except (json.JSONDecodeError, jsonschema.SchemaError) as e:
            raise ValueError(f"Invalid JSON Schema file '{schema_path}': {e}")

        if target_column not in df.columns: raise KeyError(f"Target column '{target_column}' not found.")
        print(f"Validating column '{target_column}' against schema '{schema_path.name}'.")
        errors: List[str] = []

        for index, record in df[target_column].items():
            instance = record
            if isinstance(record, str):
                try: instance = json.loads(record)
                except json.JSONDecodeError:
                    error_msg = f"Row {index}: Invalid JSON string."
                    if stop_on_first_error: raise ValueError(error_msg)
                    errors.append(error_msg); continue

            validation_errors = sorted(validator.iter_errors(instance), key=lambda e: e.path)
            if validation_errors:
                error_msg = f"Row {index}: Validation failed.\n"
                for error in validation_errors: error_msg += f"  - Path: '{'/'.join(map(str, error.path))}', Rule: '{error.validator}', Msg: {error.message}\n"
                if stop_on_first_error: raise jsonschema.ValidationError(error_msg)
                errors.append(error_msg)

        if errors:
            raise ValueError(f"JSON Schema validation failed for {len(errors)} records:\n" + "\n".join(errors))

        print("JSON Schema validation successful. Copying file to output path.")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(input_path, output_path)

        output_container = DataContainer()
        output_container.add_file_path(output_path)
        return output_container