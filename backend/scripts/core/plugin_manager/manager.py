import pluggy
import pkgutil
import importlib
import inspect
from typing import Dict, Any, Optional, Type

from . import hooks
import plugins
from core.data_container.container import DataContainer
from core.plugin_manager.base_plugin import BasePlugin

from utils.logger import setup_logger

logger = setup_logger(__name__)


class FrameworkManager:
    """
    Manages the ETL framework's plugin system using pluggy.
    """

    def __init__(self):
        self._pm = pluggy.PluginManager("etl_framework")
        self._pm.add_hookspecs(hooks.EtlHookSpecs)

        # クラスをキャッシュし call_plugin_execute のたびに新しいインスタンスを
        # 生成することで各呼び出しが独立した状態を持つ。
        self._plugin_class_cache: Dict[str, Type[BasePlugin]] = {}

        self._discover_plugins(plugins)

    def _discover_plugins(self, package) -> None:
        """
        Recursively discovers and caches all plugin classes.
        Only subclasses of BasePlugin (excluding BasePlugin itself) are considered.
        """
        prefix = package.__name__ + "."
        for _importer, modname, _ispkg in pkgutil.walk_packages(package.__path__, prefix):
            try:
                module = importlib.import_module(modname)
                for name, obj in inspect.getmembers(module):
                    if (
                        inspect.isclass(obj)
                        and issubclass(obj, BasePlugin)
                        and obj is not BasePlugin
                    ):
                        try:
                            temp = obj(params={})
                            plugin_name = temp.get_plugin_name()
                            if plugin_name:
                                if plugin_name in self._plugin_class_cache:
                                    logger.warning(
                                        f"Duplicate plugin name '{plugin_name}' detected. "
                                        f"'{modname}.{name}' will overwrite the existing entry."
                                    )
                                self._plugin_class_cache[plugin_name] = obj
                                logger.info(f"Discovered plugin class: '{plugin_name}'")
                        except TypeError as e:
                            logger.error(
                                f"Plugin class '{name}' in module '{modname}' "
                                f"must define __init__(self, params). Error: {e}"
                            )
                        except Exception as e:
                            logger.error(
                                f"Failed to get plugin name from '{name}' "
                                f"in module '{modname}': {e}"
                            )
            except Exception as e:
                logger.error(f"Failed during discovery in module {modname}: {e}")

    def call_plugin_execute(
        self,
        plugin_name: str,
        params: Dict[str, Any],
        inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        """
        Finds a plugin class by name, instantiates it with the given params,
        and invokes its execute() method.
        """
        if not self._plugin_class_cache:
            raise RuntimeError("Plugin cache is empty. No plugins were discovered.")

        if plugin_name not in self._plugin_class_cache:
            raise ValueError(
                f"Plugin '{plugin_name}' not found. "
                f"Available: {list(self._plugin_class_cache.keys())}"
            )

        plugin_class = self._plugin_class_cache[plugin_name]
        instance = plugin_class(params=params)

        # 現在の設計では StepExecutor が inputs の file_paths を
        # resolved_params に文字列として展開済みのため、plugin は
        # params["input_path"] 等でファイルパスを取得する。
        # input_data は「前ステップの DataContainer オブジェクト」として渡す。
        #
        # - inputs が空       → 空の DataContainer を渡す (初回ステップ等)
        # - inputs に値あり   → 最初の DataContainer をプライマリ入力として渡す
        if inputs:
            input_data = next(iter(inputs.values()))
        else:
            input_data = DataContainer()

        return instance.execute(input_data=input_data)


# Create a singleton instance
framework_manager = FrameworkManager()