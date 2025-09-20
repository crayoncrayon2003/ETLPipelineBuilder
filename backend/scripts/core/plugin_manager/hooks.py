from typing import Dict, Any
import pluggy

from ..data_container.container import DataContainer

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
