import os
import sys
import argparse
import json

scripts_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts")
if scripts_path not in sys.path:
    sys.path.append(scripts_path)

project_root = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

from prefect import flow, task
from typing import Dict, Any, Optional, List

from core.data_container.container import DataContainer
from core.pipeline.step_executor import StepExecutor
from api.schemas.pipeline import PipelineDefinition, PipelineNode, PipelineEdge

from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

_node_results_cache: Dict[str, Any] = {}

@task(name="Batch Step Runner")
def execute_step_batch_task(
    step_name: str, plugin_name: str, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]] = None
) -> Optional[DataContainer]:
    inputs = inputs or {}
    step_executor = StepExecutor()
    step_config = {"name": step_name, "plugin": plugin_name, "params": params}
    return step_executor.execute_step(step_config, inputs)

def _normalize_path_for_batch(path_str: str, project_root_dir: str) -> str:
    if not os.path.isabs(path_str):
        return os.path.join(project_root_dir, path_str)
    return path_str

def _submit_node_task_batch(
    node_id: str, nodes_map: Dict[str, PipelineNode], edges: List[PipelineEdge], project_root_dir: str
):
    if node_id in _node_results_cache:
        return _node_results_cache[node_id]
    node_def = nodes_map[node_id]
    upstream_inputs = {}
    for edge in edges:
        if edge.target_node_id == node_id:
            source_future = _submit_node_task_batch(edge.source_node_id, nodes_map, edges, project_root_dir)
            upstream_inputs[edge.target_input_name] = source_future
    params = node_def.params.copy()
    for key, value in params.items():
        if isinstance(value, str) and ("path" in key or "_file" in key):
            params[key] = _normalize_path_for_batch(value, project_root_dir)
    future = execute_step_batch_task.submit(
        step_name=node_def.id, plugin_name=node_def.plugin,
        params=params, inputs=upstream_inputs
    )
    _node_results_cache[node_id] = future
    return future


@flow
def run_pipeline_from_file(config_file_path: str):
    """
    The main entry point for running a pipeline from a saved JSON file.
    """
    path = config_file_path
    logger.info(f"Loading pipeline definition from absolute path: {path}")

    if not os.path.exists(path):
        raise FileNotFoundError(f"Pipeline definition file not found: {path}")

    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    pipeline_def = PipelineDefinition(**data)

    logger.info(f"Starting batch pipeline run for: {pipeline_def.name}")
    _node_results_cache.clear()
    nodes_map = {node.id: node for node in pipeline_def.nodes}
    for node_id in nodes_map:
        _submit_node_task_batch(node_id, nodes_map, pipeline_def.edges, project_root)
    logger.info(f"Pipeline '{pipeline_def.name}' submitted for execution.")


def main():
    """
    Parses command-line arguments, resolves the file path to an absolute path,
    and then triggers the Prefect flow.
    """

    def resolve_path(path_str: str) -> str:
        """ Argparse type for converting a string to a resolved absolute path. """
        if not os.path.isabs(path_str):
            path_str = os.path.join(os.getcwd(), path_str)
        return os.path.abspath(path_str)

    parser = argparse.ArgumentParser(description="ETL Framework Batch Runner.")
    parser.add_argument(
        "config_file",
        type=resolve_path,
        help="Path to the pipeline definition JSON file."
    )
    args = parser.parse_args()

    # Trigger the Prefect flow with the guaranteed absolute path
    run_pipeline_from_file(args.config_file)


if __name__ == "__main__":
    main()
