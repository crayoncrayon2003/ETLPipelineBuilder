# backend/plugins/validators/data_quality.py

import pandas as pd
from typing import Dict, Any, List, Optional
import pluggy
from pathlib import Path
import shutil

from backend.core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class DataQualityValidator:
    """
    (File-based) Performs data quality checks on a file based on a set of rules.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "data_quality"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {"type": "string", "title": "Input File Path"},
                "output_path": {"type": "string", "title": "Output File Path"},
                "rules": {
                    "type": "array", "title": "Validation Rules", "items": {
                        "type": "object",
                        "properties": { "column": {"type": "string"}, "type": {"type": "string", "enum": ["not_null", "is_unique", "in_range", "matches_regex", "in_set"]},},
                        "required": ["column", "type"]
                    }
                }
            },
            "required": ["input_path", "output_path", "rules"]
        }

    def _validate_rule(self, df: pd.DataFrame, rule: Dict[str, Any]) -> List[str]:
        errors = []
        col_name, rule_type = rule.get("column"), rule.get("type")
        if not col_name or not rule_type: raise ValueError(f"Rule missing 'column' or 'type': {rule}")
        if col_name not in df.columns: raise KeyError(f"Column '{col_name}' in rule not in DataFrame.")
        series = df[col_name]
        if rule_type == "not_null":
            if series.isnull().any(): errors.append(f"Column '{col_name}' has null values.")
        elif rule_type == "is_unique":
            if not series.is_unique: errors.append(f"Column '{col_name}' has duplicate values.")
        elif rule_type == "in_range":
            min_val, max_val = rule.get('min'), rule.get('max')
            if not series[(series < min_val) | (series > max_val)].empty: errors.append(f"Column '{col_name}' has values out of range [{min_val}, {max_val}].")
        elif rule_type == "matches_regex":
            pattern = rule.get('pattern')
            if not pattern: raise ValueError("Rule 'matches_regex' needs 'pattern'.")
            if not series[~series.astype(str).str.match(pattern, na=False)].empty: errors.append(f"Column '{col_name}' has values not matching regex.")
        elif rule_type == "in_set":
            value_set = set(rule.get('values', []))
            if not value_set: raise ValueError("Rule 'in_set' needs 'values' list.")
            if not series[~series.isin(value_set)].empty: errors.append(f"Column '{col_name}' has values not in allowed set.")
        else: errors.append(f"Unknown rule type '{rule_type}'.")
        return errors

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = Path(params.get("input_path"))
        output_path = Path(params.get("output_path"))
        rules = params.get("rules", [])

        if not input_path or not output_path:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'input_path' and 'output_path'.")
        if not input_path.exists(): raise FileNotFoundError(f"Input file not found: {input_path}")
        if not isinstance(rules, list): raise ValueError("'rules' parameter must be a list.")

        print(f"Reading file '{input_path}' to perform data quality checks...")
        df = pd.read_parquet(input_path)

        all_errors: List[str] = []
        for rule in rules:
            try:
                rule_errors = self._validate_rule(df, rule)
                if rule_errors: all_errors.extend(rule_errors)
            except (KeyError, ValueError) as e: raise e

        if all_errors:
            raise ValueError(f"Data quality validation failed:\n- " + "\n- ".join(all_errors))

        print("Data quality checks passed. Copying file to output path.")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(input_path, output_path)

        output_container = DataContainer()
        output_container.add_file_path(output_path)
        return output_container