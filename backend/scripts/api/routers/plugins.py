from fastapi import APIRouter
from typing import List, Any
import os

from core.plugin_manager.manager import framework_manager
from api.schemas.plugin import PluginInfo

from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

router = APIRouter(
    prefix="/plugins",
    tags=["Plugins"],
)

@router.get("/", response_model=List[PluginInfo])
async def get_available_plugins():
    """
    Retrieves a list of all available plugins, including their parameter schemas.
    """
    available_plugins = []

    plugin_cache = framework_manager._plugin_name_cache

    for plugin_name, instance in plugin_cache.items():
        params_schema = {}
        if hasattr(instance, 'get_parameters_schema'):
            try:
                params_schema = instance.get_parameters_schema()
            except Exception as e:
                logger.error(f"ERROR getting schema for plugin '{plugin_name}': {e}")
                params_schema = {"type": "object", "properties": {"error": {"type": "string", "default": f"Could not load schema: {e}"}}}

        plugin_info = {
            "name": plugin_name,
            "type": _get_plugin_type(instance),
            "description": (instance.__doc__ or "No description provided.").strip(),
            "parameters_schema": params_schema,
        }
        available_plugins.append(plugin_info)

    return sorted(available_plugins, key=lambda p: p['name'])

def _get_plugin_type(plugin_instance: Any) -> str:
    """ Helper function to infer plugin type from its module path. """
    module_name = plugin_instance.__class__.__module__
    if ".extractors." in module_name: return "extractor"
    if ".cleansing." in module_name: return "cleanser"
    if ".transformers." in module_name: return "transformer"
    if ".validators." in module_name: return "validator"
    if ".loaders." in module_name: return "loader"
    return "unknown"