import copy
from typing import Dict, Any, Optional

from core.plugin_manager.manager import framework_manager
from core.data_container.container import DataContainer

from utils.logger import setup_logger

logger = setup_logger(__name__)


class StepExecutor:
    """
    Executes a single pipeline step by delegating to the appropriate plugin.

    inputs の DataContainer はパスに展開せず、そのまま plugin の
    run(input_data, container) に渡します。
    params のパス (input_path / output_path 等) は呼び出し元が
    コンフィグまたはコードで明示的に指定してください。

    設計方針:
        - inputs の DataContainer → plugin の run(input_data) 引数で受け取る
        - params のキー名/パス形式 → 呼び出し元が責任を持つ
        - StepExecutor はパスの補完・変換を一切行わない
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
                  DataContainer はそのまま plugin に渡される。パスの展開は行わない。

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

            # inputs の DataContainer をそのまま plugin に渡す。
            # パス展開 (file_paths[0] を resolved_params に設定する処理) は行わない。
            # input_path / output_path 等のパス指定は呼び出し元が params で明示すること。
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