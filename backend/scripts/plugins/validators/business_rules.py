import os
import pandas as pd
from typing import Dict, Any, List
import pluggy

from core.data_container.container import DataContainer
from core.infrastructure import storage_adapter
from core.plugin_manager.base_plugin import BasePlugin

from utils.logger import setup_logger

logger = setup_logger(__name__)

hookimpl = pluggy.HookimplMarker("etl_framework")

class BusinessRulesValidator(BasePlugin):
    """
    (Storage Aware) Validates a file (local or S3) against custom business rules.
    """

    @hookimpl
    def get_plugin_name(self) -> str:
        return "business_rules"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {"type": "string", "title": "Input File Path (local/s3)"},
                "output_path": {"type": "string", "title": "Output File Path (local/s3)"},
                "rules": {
                    "type": "array",
                    "title": "Business Rules",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string", "description": "A description of the rule."},
                            "expression": {"type": "string", "description": "A pandas query string that identifies INVALID rows."}
                        },
                        "required": ["name", "expression"]
                    }
                }
            },
            "required": ["input_path", "output_path", "rules"]
        }

    def run(self, input_data: DataContainer, container: DataContainer) -> DataContainer:
        input_path = str(self.params.get("input_path"))
        output_path = str(self.params.get("output_path"))
        rules = self.params.get("rules", [])

        if not input_path or not output_path:
            raise ValueError("Missing required parameters: 'input_path' and 'output_path'.")

        try:
            df = storage_adapter.read_df(input_path)
        except Exception as e:
            raise RuntimeError(f"Failed to read input file: {str(e)}")

        all_errors: List[str] = []
        for rule in rules:
            rule_name = rule.get('name')
            expression = rule.get('expression')
            if not rule_name or not expression:
                raise ValueError(f"Rule missing 'name' or 'expression': {rule}")
            try:
                invalid_rows_df = df.query(expression)
                if not invalid_rows_df.empty:
                    error_msg = (f"Rule '{rule_name}' failed for {len(invalid_rows_df)} rows. "
                                 f"(Expression: '{expression}').")
                    all_errors.append(error_msg)
            except Exception as e:
                raise RuntimeError(f"Error executing rule '{rule_name}': {str(e)}")

        if all_errors:
            raise RuntimeError("Business rule validation failed:\n- " + "\n- ".join(all_errors))

        try:
            storage_adapter.copy_file(input_path, output_path)
        except Exception as e:
            raise RuntimeError(f"Failed to copy file: {str(e)}")

        return self.finalize_container(
            container,
            output_path=output_path,
            metadata={
                "input_path": input_path,
                "rules_checked": len(rules),
                "validation_passed": True
            }
        )
