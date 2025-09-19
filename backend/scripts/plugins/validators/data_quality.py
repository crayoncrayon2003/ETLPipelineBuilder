import os
import pandas as pd
from typing import Dict, Any, List
import pluggy

from core.infrastructure import storage_adapter
from core.data_container.container import DataContainer, DataContainerStatus
from core.plugin_manager.base_plugin import BasePlugin

from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

hookimpl = pluggy.HookimplMarker("etl_framework")

class DataQualityValidator(BasePlugin):
    """
    (Storage Aware) Performs data quality checks on a tabular file from local or S3.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "data_quality"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {"type": "string", "title": "Input File Path (local/s3)"},
                "output_path": {"type": "string", "title": "Output File Path (local/s3)"},
                "rules": {
                    "type": "array",
                    "title": "Validation Rules",
                    "items": {
                        "type": "object",
                        "properties": {
                            "column": {"type": "string"},
                            "type": {
                                "type": "string",
                                "enum": ["not_null", "is_unique", "in_range", "matches_regex", "in_set"]
                            },
                            "min": {"type": "number"},
                            "max": {"type": "number"},
                            "pattern": {"type": "string"},
                            "values": {"type": "array", "items": {"type": "string"}}
                        },
                        "required": ["column", "type"]
                    }
                }
            },
            "required": ["input_path", "output_path", "rules"]
        }

    def _validate_rule(self, df: pd.DataFrame, rule: Dict[str, Any]) -> List[str]:
        errors = []
        col_name = rule.get("column")
        rule_type = rule.get("type")

        if not col_name or not rule_type:
            raise ValueError(f"Rule missing 'column' or 'type': {rule}")
        if col_name not in df.columns:
            raise KeyError(f"Column '{col_name}' in rule not found in DataFrame.")

        series = df[col_name]

        if rule_type == "not_null":
            if series.isnull().any():
                errors.append(f"Column '{col_name}' has nulls.")
        elif rule_type == "is_unique":
            if not series.is_unique:
                errors.append(f"Column '{col_name}' has duplicates.")
        elif rule_type == "in_range":
            min_val = rule.get("min")
            max_val = rule.get("max")
            out_of_range = series.dropna()
            if not out_of_range[(out_of_range < min_val) | (out_of_range > max_val)].empty:
                errors.append(f"Column '{col_name}' has values out of range [{min_val}, {max_val}].")
        elif rule_type == "matches_regex":
            pattern = rule.get("pattern")
            if not pattern:
                raise ValueError("Rule 'matches_regex' requires 'pattern'.")
            non_matching = series.dropna().astype(str)
            if not non_matching[non_matching.str.match(pattern)].equals(non_matching):
                errors.append(f"Column '{col_name}' has values not matching regex.")
        elif rule_type == "in_set":
            value_set = set(rule.get("values", []))
            if not value_set:
                raise ValueError("Rule 'in_set' requires 'values' list.")
            out_of_set = series.dropna()
            if not out_of_set[out_of_set.isin(value_set)].equals(out_of_set):
                errors.append(f"Column '{col_name}' has values not in allowed set.")
        else:
            errors.append(f"Unknown rule type '{rule_type}'.")

        return errors

    @hookimpl
    def execute(self, input_data: DataContainer) -> DataContainer:
        input_path = str(self.params.get("input_path"))
        output_path = str(self.params.get("output_path"))
        rules = self.params.get("rules", [])

        container = DataContainer()

        if not input_path or not output_path:
            container.set_status(DataContainerStatus.ERROR)
            container.add_error("Missing required parameters: 'input_path' and 'output_path'.")
            return container

        try:
            df = storage_adapter.read_df(input_path)
        except Exception as e:
            container.set_status(DataContainerStatus.ERROR)
            container.add_error(f"Failed to read input file: {str(e)}")
            return container

        all_errors: List[str] = []
        for rule in rules:
            try:
                rule_errors = self._validate_rule(df, rule)
                all_errors.extend(rule_errors)
            except (KeyError, ValueError) as e:
                container.set_status(DataContainerStatus.ERROR)
                container.add_error(str(e))
                return container

        if all_errors:
            container.set_status(DataContainerStatus.ERROR)
            container.add_error("Data quality validation failed:\n- " + "\n- ".join(all_errors))
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
            "rules_checked": len(rules),
            "validation_passed": True
        })
        container.add_history(self.get_plugin_name())
        return container
