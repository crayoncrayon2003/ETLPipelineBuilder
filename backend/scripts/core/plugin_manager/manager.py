import os
import pluggy
import pkgutil
import importlib
import inspect
from typing import Dict, Any, Optional

from . import hooks
import plugins
from core.data_container.container import DataContainer

from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

class FrameworkManager:
    """
    Manages the ETL framework's plugin system using pluggy.
    """
    def __init__(self):
        # pluggyのPluginManagerはフック仕様の管理にのみ使用
        self._pm = pluggy.PluginManager("etl_framework")
        self._pm.add_hookspecs(hooks.EtlHookSpecs)

        # プラグイン名とインスタンスのマッピングを保持するキャッシュ
        self._plugin_name_cache: Dict[str, Any] = {}

        # プラグイン検出とインスタンス化
        self._discover_and_instantiate_plugins(plugins)

    def _discover_and_instantiate_plugins(self, package):
        """
        Recursively discovers, instantiates, and caches all plugin classes.
        """
        prefix = package.__name__ + "."
        for importer, modname, ispkg in pkgutil.walk_packages(package.__path__, prefix):
            try:
                module = importlib.import_module(modname)
                # モジュール内のすべてのメンバーを調べる
                for name, obj in inspect.getmembers(module):
                    # それがクラスであり、かつ get_plugin_name メソッドを持つか？
                    if inspect.isclass(obj) and hasattr(obj, 'get_plugin_name'):
                        # プラグインクラスを発見
                        instance = obj()
                        plugin_name = instance.get_plugin_name()
                        if plugin_name:
                            if plugin_name in self._plugin_name_cache:
                                logger.info(f"Warning: Duplicate plugin name '{plugin_name}'.")
                            self._plugin_name_cache[plugin_name] = instance
                            logger.info(f"Discovered and cached plugin: '{plugin_name}'")
            except Exception as e:
                logger.error(f"Failed during discovery in module {modname}: {e}")

    def call_plugin_execute(
        self,
        plugin_name: str,
        params: Dict[str, Any],
        inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        """
        Finds a plugin instance by name and invokes its `execute_plugin` method.
        """
        if not self._plugin_name_cache:
             raise RuntimeError("Plugin cache is empty. No plugins were discovered.")

        if plugin_name not in self._plugin_name_cache:
            raise ValueError(f"Plugin '{plugin_name}' not found. Available: {list(self._plugin_name_cache.keys())}")

        instance = self._plugin_name_cache[plugin_name]
        return instance.execute_plugin(params=params, inputs=inputs)

# シングルトンインスタンスを作成
framework_manager = FrameworkManager()