import json
from typing import Dict, Any, List, Optional
import pluggy
from pathlib import Path
import shutil

from core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class NgsiValidator:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "ngsi_validator"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {"type": "string", "title": "Input JSONL File Path"},
                "output_path": {"type": "string", "title": "Output JSONL File Path"},
                "ngsi_version": {"type": "string", "title": "NGSI Version", "enum": ["v2", "ld"], "default": "ld"}
            },
            "required": ["input_path", "output_path"]
        }

    def _validate_entity(self, entity: Dict[str, Any], index: int, ngsi_version: str) -> List[str]:
        errors = []
        if 'id' not in entity: errors.append(f"Row {index}: Missing 'id'.")
        if 'type' not in entity: errors.append(f"Row {index}: Missing 'type'.")
        reserved_keys = {'id', 'type', '@context'}
        attributes = {k: v for k, v in entity.items() if k not in reserved_keys}
        for attr_name, attr_value in attributes.items():
            if not isinstance(attr_value, dict): errors.append(f"Row {index}: Attr '{attr_name}' must be dict."); continue
            if ngsi_version == 'v2':
                if 'type' not in attr_value: errors.append(f"Row {index}: Attr '{attr_name}' missing 'type'.")
                if 'value' not in attr_value: errors.append(f"Row {index}: Attr '{attr_name}' missing 'value'.")
            elif ngsi_version == 'ld':
                if attr_value.get('type') not in ['Property', 'Relationship', 'GeoProperty']: errors.append(f"Row {index}: Attr '{attr_name}' invalid 'type'.")
                key = 'object' if attr_value.get('type') == 'Relationship' else 'value'
                if key not in attr_value: errors.append(f"Row {index}: Attr '{attr_name}' missing '{key}'.")
        return errors

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = Path(params.get("input_path"))
        output_path = Path(params.get("output_path"))
        ngsi_version = params.get("ngsi_version", "ld").lower()
        stop_on_first_error = params.get("stop_on_first_error", True)

        if not input_path or not output_path:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'input_path' and 'output_path'.")
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found at: {input_path}")

        print(f"Validating NGSI-{ngsi_version} entities in file '{input_path.name}'.")
        all_errors: List[str] = []

        with open(input_path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                try:
                    instance = json.loads(line)
                except json.JSONDecodeError:
                    error_msg = f"Row {i+1}: Invalid JSON."
                    if stop_on_first_error: raise ValueError(error_msg)
                    all_errors.append(error_msg); continue

                entity_errors = self._validate_entity(instance, i + 1, ngsi_version)
                if entity_errors:
                    if stop_on_first_error: raise ValueError("\n".join(entity_errors))
                    all_errors.extend(entity_errors)

        if all_errors:
            raise ValueError(f"NGSI validation failed:\n- " + "\n- ".join(all_errors))

        print("NGSI validation successful. Copying file to output path.")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(input_path, output_path)

        output_container = DataContainer()
        output_container.add_file_path(output_path)
        return output_container