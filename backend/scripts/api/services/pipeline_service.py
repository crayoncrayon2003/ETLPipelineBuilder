from typing import Dict, Any, Optional, List
import re
import os
import platform

from prefect import flow, task

from api.schemas.pipeline import PipelineDefinition, PipelineNode, PipelineEdge
from core.data_container.container import DataContainer
from core.pipeline.step_executor import StepExecutor

from utils.logger import setup_logger

logger = setup_logger(__name__)


@task(name="API Triggered Step")
def execute_step_api_task(
    step_name: str,
    plugin_name: str,
    params: Dict[str, Any],
    inputs: Optional[Dict[str, Optional[DataContainer]]] = None
) -> Optional[DataContainer]:
    """ A reusable Prefect task that runs any plugin step. """
    inputs = inputs or {}
    step_executor = StepExecutor()
    step_config = {"name": step_name, "plugin": plugin_name, "params": params}
    logger.info(f"execute_step_batch_task: '{step_name}' using plugin: '{plugin_name}' params: '{params}'")
    return step_executor.execute_step(step_config, inputs)


def _normalize_path(path_str: str, project_root: str) -> str:
    """
    Normalizes a path string from various formats to a valid WSL/Linux Path object.
    """
    normalized_str = path_str.replace('\\', '/')
    wsl_match = re.match(r"^//wsl(\$|\.localhost)/[^/]+(/.*)", normalized_str)
    if wsl_match:
        return wsl_match.group(2)
    win_match = re.match(r"^([a-zA-Z]):/", normalized_str)
    if win_match:
        if platform.system() == "Windows":
            return path_str
        drive = win_match.group(1).lower()
        path_remainder = normalized_str[len(win_match.group(0)):]
        return f"/mnt/{drive}/{path_remainder}"
    if not os.path.isabs(normalized_str):
        # return os.path.join(project_root, normalized_str)
        return os.path.normpath(os.path.join(project_root, normalized_str))
    # return normalized_str
    return os.path.normpath(normalized_str)


def _submit_node_task(
    node_id: str,
    nodes_map: Dict[str, PipelineNode],
    edges: List[PipelineEdge],
    project_root: str,
    node_results_cache: Dict[str, Any],
):
    """
    Recursively submits a node's task to Prefect, resolving paths correctly.
    """
    if node_id in node_results_cache:
        return node_results_cache[node_id]

    node_def = nodes_map[node_id]

    # プラグインは単一の input_data しか受け取れないため、上流エッジは高々1本までしか
    # 対応できない。2本以上あると片方が黙って上書きされて消えるため、ここで検知して止める。
    incoming_edges = [e for e in edges if e.target_node_id == node_id]
    if len(incoming_edges) > 1:
        raise ValueError(
            f"Node '{node_id}' has {len(incoming_edges)} incoming edges "
            f"(from {[e.source_node_id for e in incoming_edges]}), but a node can only "
            f"receive a single upstream input_data. Multiple incoming edges to one node "
            f"are not supported."
        )

    upstream_inputs: Dict[str, Any] = {}
    if incoming_edges:
        source_future = _submit_node_task(
            incoming_edges[0].source_node_id, nodes_map, edges, project_root, node_results_cache
        )
        upstream_inputs["input_data"] = source_future

    params = node_def.params.copy()
    for key, value in params.items():
        if isinstance(value, str) and ("path" in key or "_file" in key):
            params[key] = _normalize_path(value, project_root)

    future = execute_step_api_task.submit(
        step_name=node_def.id,
        plugin_name=node_def.plugin,
        params=params,
        inputs=upstream_inputs,
    )

    node_results_cache[node_id] = future
    return future


def run_pipeline_from_definition(pipeline_def: PipelineDefinition, project_root: str):
    """
    The main service entry point. Dynamically constructs and runs a Prefect flow.
    """
    @flow(name=pipeline_def.name)
    def dynamic_etl_flow():
        logger.info(f"Starting dynamically generated flow: {pipeline_def.name}")
        node_results_cache: Dict[str, Any] = {}
        nodes_map = {node.id: node for node in pipeline_def.nodes}

        # source_node_ids に出てこないノード = どのエッジの出発点にもなっていない
        # ノード = 終着(sink)ノード。エッジで繋がっていない独立ノードもここに含まれ、
        # その実行順序は pipeline_def.nodes の宣言順（= このループの反復順）に従う。
        source_node_ids = {edge.source_node_id for edge in pipeline_def.edges}
        sink_node_ids = [nid for nid in nodes_map if nid not in source_node_ids]

        if not sink_node_ids:
            raise ValueError(
                "No sink node found. Pipeline may have a circular dependency."
            )

        for node_id in sink_node_ids:
            _submit_node_task(
                node_id, nodes_map, pipeline_def.edges, project_root, node_results_cache
            )

    dynamic_etl_flow()