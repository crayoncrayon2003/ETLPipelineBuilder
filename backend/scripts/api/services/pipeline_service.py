# backend/api/services/pipeline_service.py

from pathlib import Path
from typing import Dict, Any, Optional, List
import re

from prefect import flow, task

from api.schemas.pipeline import PipelineDefinition, PipelineNode, PipelineEdge
from core.data_container.container import DataContainer
from core.pipeline.step_executor import StepExecutor

_node_results_cache: Dict[str, Any] = {}

@task(name="API Triggered Step")
def execute_step_api_task(
    step_name: str, plugin_name: str, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]] = None
) -> Optional[DataContainer]:
    """ A reusable Prefect task that runs any plugin step. """
    inputs = inputs or {}
    step_executor = StepExecutor()
    step_config = {"name": step_name, "plugin": plugin_name, "params": params}
    return step_executor.execute_step(step_config, inputs)

def _normalize_path(path_str: str, project_root: Path) -> Path:
    """
    Normalizes a path string from various formats to a valid WSL/Linux Path object.
    """
    normalized_str = path_str.replace('\\', '/')
    wsl_match = re.match(r"^//wsl(\$|\.localhost)/[^/]+(/.*)", normalized_str)
    if wsl_match:
        return Path(wsl_match.group(2))
    win_match = re.match(r"^([a-zA-Z]):/", normalized_str)
    if win_match:
        drive = win_match.group(1).lower()
        path_remainder = normalized_str[len(win_match.group(0)):]
        return Path(f"/mnt/{drive}/{path_remainder}")
    path_obj = Path(normalized_str)
    if not path_obj.is_absolute():
        return project_root / path_obj
    return path_obj

def _submit_node_task(
    node_id: str, nodes_map: Dict[str, PipelineNode], edges: List[PipelineEdge], project_root: Path
):
    """
    Recursively submits a node's task to Prefect, resolving paths correctly.
    """
    if node_id in _node_results_cache: return _node_results_cache[node_id]
    node_def = nodes_map[node_id]

    upstream_inputs = {}
    for edge in edges:
        if edge.target_node_id == node_id:
            source_future = _submit_node_task(edge.source_node_id, nodes_map, edges, project_root)
            upstream_inputs[edge.target_input_name] = source_future

    params = node_def.params.copy()

    for key, value in params.items():
        if isinstance(value, str) and ("path" in key or "_file" in key):
            params[key] = _normalize_path(value, project_root)

    future = execute_step_api_task.submit(
        step_name=node_def.id, plugin_name=node_def.plugin,
        params=params, inputs=upstream_inputs
    )

    _node_results_cache[node_id] = future
    return future

def run_pipeline_from_definition(pipeline_def: PipelineDefinition, project_root: Path):
    """
    The main service entry point. Dynamically constructs and runs a Prefect flow.
    """
    @flow(name=pipeline_def.name)
    def dynamic_etl_flow():
        print(f"Starting dynamically generated flow: {pipeline_def.name}")
        _node_results_cache.clear()
        nodes_map = {node.id: node for node in pipeline_def.nodes}
        for node_id in nodes_map:
            _submit_node_task(node_id, nodes_map, pipeline_def.edges, project_root)
    
    dynamic_etl_flow()