# backend/api/schemas/pipeline.py

from pydantic import BaseModel, Field
from typing import List, Dict, Any

class PipelineNode(BaseModel):
    """
    Defines the structure for a single node (a plugin step) within a pipeline DAG.
    This model validates the structure of each node in the pipeline definition.
    """
    id: str = Field(
        ...,
        description="A unique identifier for the node within the pipeline, e.g., 'node-1-xyz'. This is used to define edges.",
        examples=["node-extractor-csv-1"]
    )
    plugin: str = Field(
        ...,
        description="The name of the plugin to be executed for this node, e.g., 'from_local_file'. This must match a name in the plugin registry.",
        examples=["from_local_file"]
    )
    params: Dict[str, Any] = Field(
        default_factory=dict,
        description="A dictionary of static parameters to be passed to the plugin.",
        examples=[{"source_path": "data/input/source.csv"}]
    )

class PipelineEdge(BaseModel):
    """
    Defines the structure for a directed edge (a connection) between two nodes in the DAG.
    This defines the data flow and dependencies between steps.
    """
    source_node_id: str = Field(
        ...,
        description="The 'id' of the source node from which the edge originates.",
        examples=["node-extractor-csv-1"]
    )
    target_node_id: str = Field(
        ...,
        description="The 'id' of the target node to which the edge connects.",
        examples=["node-validator-quality-1"]
    )
    target_input_name: str = Field(
        ...,
        description="The name of the input on the target node that will receive the source node's output, e.g., 'input_data'.",
        examples=["input_data"]
    )

class PipelineDefinition(BaseModel):
    """
    The complete, self-contained definition of a pipeline DAG.
    This is the main model for the request body of the /pipelines/run endpoint.
    """
    name: str = Field(
        ...,
        description="A descriptive name for the pipeline run, which will be used as the Prefect flow name.",
        examples=["My Daily Sales Pipeline"]
    )
    nodes: List[PipelineNode] = Field(
        ...,
        description="A list of all the nodes (steps) in the pipeline."
    )
    edges: List[PipelineEdge] = Field(
        ...,
        description="A list of all the edges (dependencies) connecting the nodes."
    )