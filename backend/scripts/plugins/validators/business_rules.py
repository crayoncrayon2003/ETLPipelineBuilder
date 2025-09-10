import os
from typing import Dict, Any, List, Optional
import pluggy
import pandas as pd

from core.data_container.container import DataContainer
from core.infrastructure import storage_adapter

from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

hookimpl = pluggy.HookimplMarker("etl_framework")

class BusinessRulesValidator:
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
                    "type": "array", "title": "Business Rules", "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string", "description": "A description of the rule."},
                            "expression": {"type": "string", "description": "A pandas query string that identifies INVALID rows."}
                        }, "required": ["name", "expression"]
                    }
                }
            },
            "required": ["input_path", "output_path", "rules"]
        }

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = str(params.get("input_path"))
        output_path = str(params.get("output_path"))
        rules = params.get("rules", [])

        if not input_path or not output_path:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'input_path' and 'output_path'.")

        df = storage_adapter.read_df(input_path)

        all_errors: List[str] = []
        for rule in rules:
            rule_name, expression = rule.get('name'), rule.get('expression')
            if not rule_name or not expression:
                raise ValueError(f"Rule is missing 'name' or 'expression': {rule}")
            try:
                invalid_rows_df = df.query(expression)
                if not invalid_rows_df.empty:
                    error_msg = (f"Rule '{rule_name}' failed for {len(invalid_rows_df)} rows. "
                                 f"(Expression: '{expression}').")
                    all_errors.append(error_msg)
            except Exception as e:
                raise ValueError(f"Error executing business rule '{rule_name}': {e}")

        if all_errors:
            raise ValueError(f"Business rule validation failed:\n- " + "\n- ".join(all_errors))

        logger.info("All business rules passed. Copying file to output path.")
        storage_adapter.copy_file(input_path, output_path)

        output_container = DataContainer()
        output_container.add_file_path(output_path)
        return output_container