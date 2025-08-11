# backend/api/schemas/plugin.py

from pydantic import BaseModel, Field
from typing import Literal, Dict, Any

# Use Literal to define a specific set of allowed string values for plugin types.
# This provides strong typing and validation.
PluginType = Literal["extractor", "cleanser", "transformer", "validator", "loader", "unknown"]

class PluginInfo(BaseModel):
    """
    Defines the data structure for information about a single available plugin.
    This model is used as the response model for the GET /plugins endpoint,
    providing the frontend with the necessary data to render plugin nodes.
    """
    name: str = Field(
        ...,
        description="The unique name of the plugin. This name is used in pipeline definitions to identify which plugin to run.",
        examples=["from_local_file"]
    )
    type: PluginType = Field(
        ...,
        description="The category of the plugin. This is used by the frontend GUI to group, color-code, or otherwise organize the plugin nodes.",
        examples=["extractor"]
    )
    description: str = Field(
        ...,
        description="A brief description of what the plugin does, typically extracted from its docstring.",
        examples=["(File-based) Reads a source file (like CSV) and saves it as Parquet..."]
    )

    parameters_schema: Dict[str, Any] = Field(
        ...,
        description="The JSON Schema definition for the parameters this plugin accepts.",
        examples=[{"type": "object", "properties": {"path": {"type": "string"}}, "required": ["path"]}]
    )