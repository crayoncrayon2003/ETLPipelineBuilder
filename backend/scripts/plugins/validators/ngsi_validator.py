import json
from typing import Dict, Any, List, Optional
import pluggy
from pathlib import Path

from core.infrastructure import storage_adapter
from core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class NgsiValidator:
    """
    (Storage Aware) Validates NGSI entities in a JSON Lines file from local or S3.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "ngsi_validator"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {
                    "type": "string",
                    "title": "Input JSONL File Path (local/s3)",
                    "description": "The JSON Lines file containing NGSI entities to validate."
                },
                "output_path": {
                    "type": "string",
                    "title": "Output JSONL File Path (local/s3)",
                    "description": "Path to copy the file to if validation succeeds."
                },
                "ngsi_version": {
                    "type": "string",
                    "title": "NGSI Version",
                    "description": "The NGSI specification version to validate against.",
                    "enum": ["v2", "ld"],
                    "default": "ld"
                }
            },
            "required": ["input_path", "output_path"]
        }

    def _validate_entity(self, entity: Dict[str, Any], index: int, ngsi_version: str) -> List[str]:
        """Validates a single NGSI entity dictionary."""
        errors = []
        if 'id' not in entity:
            errors.append(f"Row {index}: Entity is missing mandatory 'id' property.")
        if 'type' not in entity:
            errors.append(f"Row {index}: Entity is missing mandatory 'type' property.")

        reserved_keys = {'id', 'type'}
        if ngsi_version == 'ld':
            reserved_keys.add('@context')

        attributes = {k: v for k, v in entity.items() if k not in reserved_keys}
        for attr_name, attr_value in attributes.items():
            if not isinstance(attr_value, dict):
                errors.append(f"Row {index}: Attribute '{attr_name}' must be a dictionary object.")
                continue

            if ngsi_version == 'v2':
                if 'type' not in attr_value:
                    errors.append(f"Row {index}: NGSI-v2 attribute '{attr_name}' is missing 'type'.")
                if 'value' not in attr_value:
                    errors.append(f"Row {index}: NGSI-v2 attribute '{attr_name}' is missing 'value'.")

            elif ngsi_version == 'ld':
                if attr_value.get('type') not in ['Property', 'Relationship', 'GeoProperty']:
                    errors.append(f"Row {index}: NGSI-LD attribute '{attr_name}' has an invalid or missing 'type'. Must be one of 'Property', 'Relationship', 'GeoProperty'.")

                key_to_check = 'object' if attr_value.get('type') == 'Relationship' else 'value'
                if key_to_check not in attr_value:
                     errors.append(f"Row {index}: NGSI-LD attribute '{attr_name}' of type '{attr_value.get('type')}' is missing the required '{key_to_check}' key.")

        return errors

    def _read_text_content(self, path: str) -> str:
        """Reads the entire content of a text file from local or S3."""
        print(f"Reading text content from: {path}")
        if path.startswith("s3://"):
            try:
                import s3fs
                s3 = s3fs.S3FileSystem()
                with s3.open(path, 'r', encoding='utf-8') as f:
                    return f.read()
            except ImportError:
                raise ImportError("s3fs is required for reading from S3. Please install it.")
        else:
            p = Path(path)
            if not p.exists():
                raise FileNotFoundError(f"Input file not found at local path: {p}")
            return p.read_text(encoding='utf-8')

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = str(params.get("input_path"))
        output_path = str(params.get("output_path"))
        ngsi_version = params.get("ngsi_version", "ld").lower()
        stop_on_first_error = params.get("stop_on_first_error", True)

        if not input_path or not output_path:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'input_path' and 'output_path'.")

        print(f"Validating NGSI-{ngsi_version} entities in file '{input_path}'.")
        all_errors: List[str] = []

        content = self._read_text_content(input_path)
        lines = content.splitlines()

        for i, line in enumerate(lines):
            if not line.strip(): continue # Skip empty lines
            try:
                instance = json.loads(line)
            except json.JSONDecodeError:
                error_msg = f"Row {i+1}: Invalid JSON format."
                if stop_on_first_error: raise ValueError(error_msg)
                all_errors.append(error_msg); continue

            entity_errors = self._validate_entity(instance, i + 1, ngsi_version)
            if entity_errors:
                if stop_on_first_error: raise ValueError("\n".join(entity_errors))
                all_errors.extend(entity_errors)

        if all_errors:
            raise ValueError(f"NGSI validation failed for {len(all_errors)} issues:\n- " + "\n- ".join(all_errors))

        print("NGSI validation successful. Copying file to output path.")
        # We use write_text instead of copy_file to ensure consistent content handling
        storage_adapter.write_text(content, output_path)

        output_container = DataContainer()
        output_container.add_file_path(output_path)
        return output_container