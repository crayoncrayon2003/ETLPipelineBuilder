from typing import Dict, Any, List, Optional
import pluggy
import pandas as pd
from pathlib import Path
import shutil

from core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class BusinessRulesValidator:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "business_rules"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {"type": "string", "title": "Input File Path"},
                "output_path": {"type": "string", "title": "Output File Path"},
                "rules": {
                    "type": "array", "title": "Business Rules", "items": {
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

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = Path(params.get("input_path"))
        output_path = Path(params.get("output_path"))
        rules = params.get("rules", [])

        if not input_path or not output_path:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'input_path' and 'output_path'.")
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found at: {input_path}")
        if not isinstance(rules, list):
            raise ValueError("'rules' parameter must be a list.")

        print(f"Reading file '{input_path}' to validate business rules...")
        df = pd.read_parquet(input_path)

        all_errors: List[str] = []
        for rule in rules:
            rule_name = rule.get('name')
            expression = rule.get('expression')
            if not rule_name or not expression:
                raise ValueError(f"Rule is missing 'name' or 'expression': {rule}")

            try:
                invalid_rows_df = df.query(expression)
                if not invalid_rows_df.empty:
                    error_msg = (f"Rule '{rule_name}' failed for {len(invalid_rows_df)} rows. "
                                 f"(Expression: '{expression}'). "
                                 f"Indices: {invalid_rows_df.index.tolist()[:5]}")
                    all_errors.append(error_msg)
            except Exception as e:
                raise ValueError(f"Error executing business rule '{rule_name}': {e}")

        if all_errors:
            raise ValueError(f"Business rule validation failed:\n- " + "\n- ".join(all_errors))

        print("All business rules passed. Copying file to output path.")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(input_path, output_path)

        output_container = DataContainer()
        output_container.add_file_path(output_path)
        return output_container