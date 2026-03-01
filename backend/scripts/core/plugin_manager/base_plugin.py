import abc
import copy
from typing import Dict, Any

from core.data_container.container import DataContainer, DataContainerStatus

from utils.logger import setup_logger

logger = setup_logger(__name__)


class BasePlugin(abc.ABC):
    """
    Abstract base class for ETL plugins.
    Provides a unified execution lifecycle:
    prev_execute → run → post_execute

    引数の役割:
        input_data : 前ステップから渡された入力データ。読み取り専用として扱うこと。
        container  : execute() が新規作成した出力先。run() はここに結果を書き込む。
    """

    def __init__(self, params: Dict[str, Any]):
        self.params = params or {}

    def set_params(self, params: Dict[str, Any]):
        self.params = params or {}

    def execute(self, input_data: DataContainer) -> DataContainer:
        """
        Unified lifecycle entry point. Handles common execution flow.
        """
        container = DataContainer()
        try:
            self.prev_execute(input_data, container)
            result = self.run(input_data, container)
            self.post_execute(input_data, result)
            return result
        except Exception as e:
            logger.error(f"[{self.get_plugin_name()}] Execution failed: {e}", exc_info=True)
            error_container = DataContainer()
            error_container.set_status(DataContainerStatus.ERROR)
            error_container.add_error(str(e))
            return error_container

    def prev_execute(self, input_data: DataContainer, container: DataContainer) -> None:
        """
        Optional setup before processing.
        """
        logger.info(f"[{self.get_plugin_name()}] Starting plugin execution.")

    @abc.abstractmethod
    def run(self, input_data: DataContainer, container: DataContainer) -> DataContainer:
        """
        Plugin-specific logic to be implemented by subclasses.

        Args:
            input_data: 前ステップから渡された入力データ。
                        【読み取り専用】このオブジェクトを変更してはならない。
                        変更が必要な場合は copy.deepcopy() してから使うこと。
            container:  このプラグインの出力先。
                        finalize_container(container, ...) に渡して結果を書き込む。

        Returns:
            finalize_container() で完成させた container を返すこと。
            input_data をそのまま返してはならない。
        """
        pass

    def post_execute(self, input_data: DataContainer, container: DataContainer) -> None:
        """
        Optional cleanup or metadata enrichment after processing.
        """
        container.add_history(self.get_plugin_name())
        logger.info(f"[{self.get_plugin_name()}] Plugin execution completed.")

    @abc.abstractmethod
    def get_plugin_name(self) -> str:
        """
        Returns the unique name of the plugin.
        """
        pass

    @abc.abstractmethod
    def get_parameters_schema(self) -> Dict[str, Any]:
        """
        Returns the plugin's parameter schema (JSON Schema format).
        """
        pass

    def finalize_container(
        self,
        container: DataContainer,
        output_path: str = None,
        metadata: Dict[str, Any] = None
    ) -> DataContainer:
        """
        Helper to finalize container with success status, output path, and metadata.

        Args:
            container:   run() で受け取った出力先 DataContainer を渡すこと。
                         input_data を渡すと入力データが破壊されるため厳禁。
            output_path: 出力ファイルパス。
            metadata:    追加するメタデータ。
        """
        container.set_status(DataContainerStatus.SUCCESS)
        if output_path:
            container.add_file_path(output_path)
        if metadata:
            container.metadata.update(copy.copy(metadata))
        return container
