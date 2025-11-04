import os
import sys
import argparse
import json

scripts_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts")
if scripts_path not in sys.path:
    sys.path.append(scripts_path)

project_root = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

from typing import Dict, Any, Optional, List

from core.data_container.container import DataContainer, DataContainerStatus
from core.pipeline.step_executor import StepExecutor
from api.schemas.pipeline import PipelineDefinition, PipelineNode, PipelineEdge

from utils.logger import setup_logger
from core.infrastructure.storage_adapter import storage_adapter
from core.infrastructure.storage_path_utils import normalize_path, is_remote_path, is_local_path

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

_node_results_cache: Dict[str, Any] = {}

def execute_step_batch_task(
    step_name: str, plugin_name: str, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]] = None
) -> Optional[DataContainer]:
    inputs = inputs or {}
    step_executor = StepExecutor()
    step_config = {"name": step_name, "plugin": plugin_name, "params": params}
    logger.info(f"execute_step_batch_task: '{step_name}' using plugin: '{plugin_name}' params: '{params}'")
    return step_executor.execute_step(step_config, inputs)


def _submit_node_task_batch(
    node_id: str, nodes_map: Dict[str, PipelineNode], edges: List[PipelineEdge], project_root_dir: str
):
    if node_id in _node_results_cache:
        return _node_results_cache[node_id]
    node_def = nodes_map[node_id]
    upstream_inputs = {}
    for edge in edges:
        if edge.target_node_id == node_id:
            source_result = _submit_node_task_batch(edge.source_node_id, nodes_map, edges, project_root_dir)
            upstream_inputs[edge.target_input_name] = source_result
    params = node_def.params.copy()
    for key, value in params.items():
        if isinstance(value, str) and ("path" in key or "_file" in key):
            # ✅ storage_path_utils の normalize_path を使用
            params[key] = normalize_path(value, project_root_dir)
    result = execute_step_batch_task(
        step_name=node_def.id, plugin_name=node_def.plugin,
        params=params, inputs=upstream_inputs
    )
    _node_results_cache[node_id] = result
    return result

def run_pipeline_from_file(config_file_path: str, fail_stop: bool=True):
    """
    The main entry point for running a pipeline from a saved JSON file.
    Supports local and S3 paths.
    """
    logger.info(f"Loading pipeline definition from: {config_file_path}")

    try:
        file_content = storage_adapter.read_text(config_file_path)
        data = json.loads(file_content)
    except Exception as e:
        logger.error(f"Failed to load pipeline definition file: {e}", exc_info=True)
        raise
    logger.info(f"run_pipeline_from_file '{data}'")
    pipeline_def = PipelineDefinition(**data)

    logger.info(f"Starting batch pipeline run for: {pipeline_def.name}")
    _node_results_cache.clear()
    nodes_map = {node.id: node for node in pipeline_def.nodes}

    try:
        for node_id in nodes_map:
            ret = _submit_node_task_batch(node_id, nodes_map, pipeline_def.edges, project_root)
            if fail_stop :
                if ret.status in [DataContainerStatus.ERROR,DataContainerStatus.SKIPPED,DataContainerStatus.VALIDATION_FAILED,]:
                    raise RuntimeError(f"Node '{node_id}' execution failed.")
    except Exception as e:
        logger.error(f"Failed to load pipeline definition file: {e}", exc_info=True)
        return
    logger.info(f"Pipeline '{pipeline_def.name}' completed.")


def main_local():
    parser = argparse.ArgumentParser(description="ETL Framework Batch Runner (Local).")
    parser.add_argument(
        "config_file",
        type=str,
        help="Local path to the pipeline definition JSON file."
    )
    args, unknown = parser.parse_known_args()

    logger.info(f"[Local CLI Args] {json.dumps(vars(args), indent=2)}")
    if unknown:
        logger.warning(f"[Local CLI Args] Unknown arguments: {unknown}")

    config_file = args.config_file
    if not os.path.isabs(config_file):
        config_file = os.path.join(os.getcwd(), config_file)
    config_file = os.path.abspath(config_file)

    run_pipeline_from_file(config_file, fail_stop=False)


def main_aws():
    parser = argparse.ArgumentParser(description="ETL Framework Batch Runner (AWS).")
    parser.add_argument(
        "config_file",
        type=str,
        help="S3 path (or remote path) to the pipeline definition JSON file."
    )
    args, unknown = parser.parse_known_args()

    logger.info(f"[AWS CLI Args] {json.dumps(vars(args), indent=2)}")
    if unknown:
        logger.warning(f"[AWS CLI Args] Unknown arguments: {unknown}")

    run_pipeline_from_file(args.config_file, fail_stop=False)


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("config_file", type=str)
    args, _ = parser.parse_known_args()

    if is_remote_path(args.config_file):
        logger.info("Detected remote path → delegate to main_aws()")
        main_aws()
    else:
        logger.info("Detected local path → delegate to main_local()")
        main_local()


if __name__ == "__main__":
    main()