from pathlib import Path
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
        return os.path.join(project_root, normalized_str)
    return normalized_str


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

    upstream_inputs: Dict[str, Any] = {}
    for edge in edges:
        if edge.target_node_id == node_id:
            source_future = _submit_node_task(
                edge.source_node_id, nodes_map, edges, project_root, node_results_cache
            )
            # フューチャーをそのまま渡す。
            # Prefect が依存関係を検出し、上流タスク完了後に本タスクを実行する。
            # .result() で同期待機すると並列実行の恩恵が得られないため廃止。
            upstream_inputs[edge.target_input_name] = source_future

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

        # どのエッジのターゲットにもなっていないノード = シンクノード（末端）
        target_node_ids = {edge.target_node_id for edge in pipeline_def.edges}
        sink_node_ids = [nid for nid in nodes_map if nid not in target_node_ids]

        for node_id in sink_node_ids:
            _submit_node_task(
                node_id, nodes_map, pipeline_def.edges, project_root, node_results_cache
            )

    dynamic_etl_flow()