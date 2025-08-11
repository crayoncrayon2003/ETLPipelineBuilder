# backend/core/plugin_manager/hooks.py

from typing import Dict, Any, Optional
import pluggy

from ..data_container.container import DataContainer

# "etl_framework" という名前のフック仕様（hook specification）グループを作成
# この名前は、後でフック実装（hook implementation）と紐付けるために使う
hookspec = pluggy.HookspecMarker("etl_framework")

class EtlHookSpecs:
    """
    Defines the hook specifications for the ETL framework plugins.

    Each method defined here, decorated with @hookspec, represents a
    pluggable point in the framework. Plugins will provide implementations
    for these hooks.
    """

    @hookspec
    def get_plugin_name(self) -> str:
        """
        A simple hook for a plugin to declare its own name.
        This will replace the need for the central registry.
        """
        pass

    @hookspec
    def get_parameters_schema(self) -> Dict[str, Any]:
        """
        A hook for a plugin to declare the schema of the parameters it accepts.
        The schema should conform to a subset of the JSON Schema specification.
        """
        pass

    @hookspec
    def execute_plugin(
        self,
        params: Dict[str, Any],
        inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        """
        The primary hook that all functional plugins (Extractors, Transformers, etc.) will implement.

        Pluggy will call all plugins that implement this hook. The plugin itself
        is responsible for checking if it should run based on its name.
        """
        pass