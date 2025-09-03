from typing import Dict, Any, Optional
import re

from core.secrets.secret_resolver import secret_resolver

from core.plugin_manager.manager import framework_manager
from core.data_container.container import DataContainer

class StepExecutor:
    """
    Resolves inputs and secrets, then executes a single plugin step.
    """

    def _resolve_secrets_in_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Recursively finds and replaces secret references (e.g., "${secrets.MY_KEY}")
        in the parameters dictionary.
        """
        resolved_params = {}
        # Regex to find patterns like "${secrets.SOME_NAME}"
        secret_pattern = re.compile(r"\${secrets\.([^}]+)}")

        for key, value in params.items():
            if isinstance(value, str):
                # Check if the string matches the secret pattern
                match = secret_pattern.fullmatch(value)
                if match:
                    secret_name = match.group(1)
                    print(f"  Resolving secret: '{secret_name}'...")
                    resolved_value = secret_resolver.resolve(secret_name)
                    if resolved_value is None:
                        raise ValueError(f"Secret '{secret_name}' could not be resolved.")
                    resolved_params[key] = resolved_value
                else:
                    # Not a secret, keep the original value
                    resolved_params[key] = value
            elif isinstance(value, dict):
                # Recurse into nested dictionaries
                resolved_params[key] = self._resolve_secrets_in_params(value)
            elif isinstance(value, list):
                 # Recurse into lists
                resolved_params[key] = [
                    self._resolve_secrets_in_params(item) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
                # Not a string, dict, or list, keep the original value
                resolved_params[key] = value

        return resolved_params

    def execute_step(
        self,
        step_config: Dict[str, Any],
        inputs: Optional[Dict[str, DataContainer]] = None
    ) -> Optional[DataContainer]:
        """
        Executes a single pipeline step.
        """
        plugin_name = step_config.get('plugin')
        params = step_config.get('params', {})
        step_name = step_config.get('name', plugin_name)

        print(f"  Executing step: '{step_name}' using plugin: '{plugin_name}'")

        try:
            params_with_secrets = self._resolve_secrets_in_params(params)

            resolved_params = params_with_secrets.copy()
            if inputs:
                for input_name, container in inputs.items():
                    if container:
                        param_key = "input_path" if input_name == "input_data" else f"{input_name}_path"
                        file_paths = container.get_file_paths()
                        if len(file_paths) == 1:
                            resolved_params[param_key] = file_paths[0]
                        elif len(file_paths) > 1:
                            resolved_params[param_key] = file_paths

            output_container = framework_manager.call_plugin_execute(
                plugin_name=plugin_name,
                params=resolved_params,
                inputs=inputs or {}
            )

            print(f"  Step '{step_name}' completed.")
            return output_container

        except Exception as e:
            print(f"  ERROR during step '{step_name}': {e}")
            raise