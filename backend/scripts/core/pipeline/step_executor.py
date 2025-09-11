import os
import re
from typing import Dict, Any, Optional

from core.plugin_manager.manager import framework_manager
from core.data_container.container import DataContainer

from core.infrastructure.secret_resolver import secret_resolver, SecretResolutionError

from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

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

        secret_pattern = re.compile(r"\${secrets\.([^}]+)}")

        for key, value in params.items():
            if isinstance(value, str):
                match = secret_pattern.fullmatch(value)
                if match:
                    secret_reference = match.group(1)
                    logger.info(f"  Resolving secret reference: '{secret_reference}'...")
                    try:
                        resolved_value = secret_resolver.resolve(secret_reference)
                        if resolved_value is None:
                            raise ValueError(f"Secret reference '{secret_reference}' could not be resolved (returned None). "
                                             "Please ensure the reference is correct and the secret exists.")
                        resolved_params[key] = resolved_value
                    except SecretResolutionError as e:
                        raise ValueError(f"Error resolving secret '{secret_reference}': {e}") from e
                else:
                    resolved_params[key] = value
            elif isinstance(value, dict):
                resolved_params[key] = self._resolve_secrets_in_params(value)
            elif isinstance(value, list):
                resolved_params[key] = [
                    self._resolve_secrets_in_params(item) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
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
        logger.debug(f"[StepExecutor] step_config (raw): {step_config}")

        plugin_name = step_config.get('plugin')
        params = step_config.get('params', {})
        step_name = step_config.get('name', plugin_name)

        logger.info(f"  Executing step: '{step_name}' using plugin: '{plugin_name}'")

        try:
            params_with_secrets = self._resolve_secrets_in_params(params)
            logger.debug(f"[StepExecutor] Params after secret resolution: {params_with_secrets}")

            resolved_params = params_with_secrets.copy()
            if inputs:
                for input_name, container in inputs.items():
                    if container:
                        param_key = "input_path" if input_name == "input_data" else f"{input_name}_path"
                        file_paths = container.get_file_paths()
                        logger.debug(f"[StepExecutor] Input '{input_name}' file paths: {file_paths}")
                        if len(file_paths) == 1:
                            resolved_params[param_key] = file_paths[0]
                        elif len(file_paths) > 1:
                            resolved_params[param_key] = file_paths

            output_container = framework_manager.call_plugin_execute(
                plugin_name=plugin_name,
                params=resolved_params,
                inputs=inputs or {}
            )

            logger.info(f"  Step '{step_name}' completed.")
            return output_container

        except Exception as e:
            logger.error(f"  ERROR during step '{step_name}': {e}")
            raise