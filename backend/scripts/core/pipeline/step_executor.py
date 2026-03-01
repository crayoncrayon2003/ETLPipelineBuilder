import copy
from typing import Dict, Any, Optional

from core.plugin_manager.manager import framework_manager
from core.data_container.container import DataContainer

from utils.logger import setup_logger

logger = setup_logger(__name__)


class StepExecutor:
    """
    inputs のキー名はそのまま plugin の params キー名として使われます。
    例: inputs={"input_path": container} → params["input_path"] = "/path/to/file"
    キー名は各 plugin の get_parameters_schema() で定義された名前に合わせてください。
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
        - inputs: Optional dictionary of input DataContainer objects.
                  Each key must match the parameter name defined in the plugin schema.
                  e.g. {"input_path": container}

        Returns:
        - DataContainer output from the plugin execution

        Raises:
        - ValueError: If 'plugin' key is missing or empty in step_config
        """
        logger.debug(f"[StepExecutor] step_config (raw): {step_config}")

        plugin_name = step_config.get('plugin')

        if not plugin_name:
            raise ValueError(
                f"'plugin' key is missing or empty in step_config: {step_config}"
            )

        params = step_config.get('params') or {}
        step_name = step_config.get('name', plugin_name)

        logger.info(
            f"  Executing step: '{step_name}' using plugin: '{plugin_name}' "
            f"with params: {params}"
        )

        try:
            resolved_params = copy.deepcopy(params)

            if inputs:
                for input_name, container in inputs.items():
                    if container:
                        file_paths = container.get_file_paths()
                        logger.debug(
                            f"[StepExecutor] Input '{input_name}' file paths: {file_paths}"
                        )

                        if len(file_paths) == 0:
                            logger.warning(
                                f"[StepExecutor] Input '{input_name}' has no file paths. "
                                f"'{input_name}' will not be set in params."
                            )
                        elif len(file_paths) == 1:
                            resolved_params[input_name] = file_paths[0]
                        else:
                            resolved_params[input_name] = file_paths

            safe_inputs = {
                k: copy.deepcopy(v) for k, v in (inputs or {}).items()
            }

            output_container = framework_manager.call_plugin_execute(
                plugin_name=plugin_name,
                params=resolved_params,
                inputs=safe_inputs
            )

            logger.info(f"  Step '{step_name}' completed.")
            return output_container

        except Exception as e:
            logger.error(f"  ERROR during step '{step_name}': {e}")
            raise