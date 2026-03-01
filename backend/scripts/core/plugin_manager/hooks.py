from typing import Dict, Any
import pluggy

hookspec = pluggy.HookspecMarker("etl_framework")


class EtlHookSpecs:
    """
    Defines the hook specifications for the ETL framework plugins.

    Each method defined here, decorated with @hookspec, represents a
    pluggable point in the framework. Plugins will provide implementations
    for these hooks.
    """

    @hookspec(firstresult=True)
    def get_plugin_name(self) -> str:
        """
        A simple hook for a plugin to declare its own name.
        This will replace the need for the central registry.

        firstresult=True: 最初の non-None 値を返す。
        複数プラグインが登録された場合でもリストではなく str が返る。
        """
        pass

    @hookspec(firstresult=True)
    def get_parameters_schema(self) -> Dict[str, Any]:
        """
        A hook for a plugin to declare the schema of the parameters it accepts.
        The schema should conform to a subset of the JSON Schema specification.

        firstresult=True: 最初の non-None 値を返す。
        複数プラグインが登録された場合でもリストではなく Dict が返る。
        """
        pass