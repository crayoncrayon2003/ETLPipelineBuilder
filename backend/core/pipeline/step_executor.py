# backend/core/pipeline/step_executor.py

from typing import Dict, Any, Optional

from ..plugin_manager.manager import framework_manager
from ..data_container.container import DataContainer

class StepExecutor:
    """
    Resolves input data containers to file paths, injects them into
    parameters, and executes a single plugin step.
    """

    def execute_step(
        self,
        step_config: Dict[str, Any],
        inputs: Optional[Dict[str, DataContainer]] = None
    ) -> Optional[DataContainer]:
        """
        Executes a single pipeline step.

        Args:
            step_config (Dict[str, Any]): Config for the step ('name', 'plugin', 'params').
            inputs (Optional[Dict[str, DataContainer]]): Resolved DataContainers from upstream.
        """
        plugin_name = step_config.get('plugin')
        params = step_config.get('params', {})
        step_name = step_config.get('name', plugin_name)

        print(f"  Executing step: '{step_name}' using plugin: '{plugin_name}'")

        try:
            resolved_params = params.copy()
            if inputs:
                for input_name, container in inputs.items():
                    if container:
                        # Convention:
                        # inputs={'input_data': ...} -> params['input_path'] = ...
                        # inputs={'some_key': ...}   -> params['some_key_path'] = ...
                        param_key = "input_path" if input_name == "input_data" else f"{input_name}_path"

                        file_paths = container.get_file_paths()
                        # Handle single vs. multiple files
                        if len(file_paths) == 1:
                            resolved_params[param_key] = file_paths[0]
                        elif len(file_paths) > 1:
                            resolved_params[param_key] = file_paths

            # The plugin receives a params dict with all paths resolved.
            output_container = framework_manager.call_plugin_execute(
                plugin_name=plugin_name,
                params=resolved_params,
                inputs=inputs or {} # Pass original inputs for metadata access etc.
            )

            print(f"  Step '{step_name}' completed.")
            return output_container

        except Exception as e:
            print(f"  ERROR during step '{step_name}': {e}")
            raise