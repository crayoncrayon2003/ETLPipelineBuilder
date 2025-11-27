import os
from typing import Dict, Any, Optional

from core.plugin_manager.manager import framework_manager
from core.data_container.container import DataContainer

from utils.logger import setup_logger

logger = setup_logger(__name__)

class StepExecutor:
    """
    Executes a single plugin step.
    Passes parameters and inputs directly to the plugin without resolving secrets.
    """

    def execute_step(
        self,
        step_config: Dict[str, Any],
        inputs: Optional[Dict[str, DataContainer]] = None
    ) -> Optional[DataContainer]:
        """
        Executes a single pipeline step.

        Parameters:
        - step_config: Dict containing 'plugin', 'params', and optionally 'name'
        - inputs: Optional dictionary of input DataContainer objects

        Returns:
        - DataContainer output from the plugin execution
        """
        logger.debug(f"[StepExecutor] step_config (raw): {step_config}")

        plugin_name = step_config.get('plugin')
        params = step_config.get('params', {})
        step_name = step_config.get('name', plugin_name)

        logger.info(f"  Executing step: '{step_name}' using plugin: '{plugin_name}' with params: {params}")

        try:
            resolved_params = params.copy()
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