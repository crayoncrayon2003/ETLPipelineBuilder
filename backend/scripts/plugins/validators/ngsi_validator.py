import os
import json
from typing import Dict, Any, List
import pluggy

from core.infrastructure import storage_adapter
from core.data_container.container import DataContainer
from core.plugin_manager.base_plugin import BasePlugin

from utils.logger import setup_logger

logger = setup_logger(__name__)

hookimpl = pluggy.HookimplMarker("etl_framework")

class NgsiValidator(BasePlugin):
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
                    "title": "Input JSONL File Path (local/s3)"
                },
                "output_path": {
                    "type": "string",
                    "title": "Output JSONL File Path (local/s3)"
                },
                "ngsi_version": {
                    "type": "string",
                    "title": "NGSI Version",
                    "enum": ["v2", "ld"],
                    "default": "ld"
                },
                "stop_on_first_error": {
                    "type": "boolean",
                    "title": "Stop on First Error",
                    "default": True
                }
            },
            "required": ["input_path", "output_path"]
        }

    def _validate_entity(self, entity: Dict[str, Any], index: int, ngsi_version: str) -> List[str]:
        errors = []
        if 'id' not in entity:
            errors.append(f"Row {index}: Missing 'id'.")
        if 'type' not in entity:
            errors.append(f"Row {index}: Missing 'type'.")

        reserved_keys = {'id', 'type'}
        if ngsi_version == 'ld':
            reserved_keys.add('@context')

        attributes = {k: v for k, v in entity.items() if k not in reserved_keys}
        for attr_name, attr_value in attributes.items():
            if not isinstance(attr_value, dict):
                errors.append(f"Row {index}: Attribute '{attr_name}' must be a dictionary.")
                continue

            if ngsi_version == 'v2':
                if 'type' not in attr_value:
                    errors.append(f"Row {index}: NGSI-v2 attribute '{attr_name}' missing 'type'.")
                if 'value' not in attr_value:
                    errors.append(f"Row {index}: NGSI-v2 attribute '{attr_name}' missing 'value'.")
            elif ngsi_version == 'ld':
                attr_type = attr_value.get('type')
                if attr_type not in ['Property', 'Relationship', 'GeoProperty']:
                    errors.append(f"Row {index}: NGSI-LD attribute '{attr_name}' has invalid 'type'.")
                key_to_check = 'object' if attr_type == 'Relationship' else 'value'
                if key_to_check not in attr_value:
                    errors.append(f"Row {index}: NGSI-LD attribute '{attr_name}' missing '{key_to_check}'.")

        return errors

    def run(self, input_data: DataContainer, container: DataContainer) -> DataContainer:
        input_path = str(self.params.get("input_path"))
        output_path = str(self.params.get("output_path"))
        ngsi_version = self.params.get("ngsi_version", "ld").lower()
        stop_on_first_error = self.params.get("stop_on_first_error", True)

        if not input_path or not output_path:
            raise ValueError("Missing 'input_path' or 'output_path'.")

        try:
            content = storage_adapter.read_text(input_path)
        except Exception as e:
            raise RuntimeError(f"Failed to read input file: {str(e)}")

        lines = content.splitlines()
        all_errors: List[str] = []

        for i, line in enumerate(lines):
            if not line.strip():
                continue
            try:
                instance = json.loads(line)
            except json.JSONDecodeError:
                msg = f"Row {i+1}: Invalid JSON format."
                if stop_on_first_error:
                    raise RuntimeError(msg)
                all_errors.append(msg)
                continue

            entity_errors = self._validate_entity(instance, i + 1, ngsi_version)
            if entity_errors:
                if stop_on_first_error:
                    raise RuntimeError("\n".join(entity_errors))
                all_errors.extend(entity_errors)

        if all_errors:
            raise RuntimeError(f"NGSI validation failed:\n- " + "\n- ".join(all_errors))

        try:
            storage_adapter.write_text(content, output_path)
        except Exception as e:
            raise RuntimeError(f"Failed to write output file: {str(e)}")

        return self.finalize_container(
            container,
            output_path=output_path,
            metadata={
                "input_path": input_path,
                "ngsi_version": ngsi_version,
                "entities_validated": len(lines),
                "validation_passed": True
            }
        )
