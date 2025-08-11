# backend/api/schemas/__init__.py

# Import models from the pipeline schema module to expose them at the package level.
from .pipeline import PipelineDefinition, PipelineNode, PipelineEdge

# Import models from the plugin schema module.
from .plugin import PluginInfo, PluginType

# Define the public API of this package using __all__.
# This controls what is imported when a client uses `from backend.api.schemas import *`.
__all__ = [
    "PipelineDefinition",
    "PipelineNode",
    "PipelineEdge",
    "PluginInfo",
    "PluginType",
]