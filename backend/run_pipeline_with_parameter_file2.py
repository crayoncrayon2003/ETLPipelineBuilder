import os
import sys
import argparse
import json
from typing import Dict, Any, Optional, List

scripts_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts")
if scripts_path not in sys.path:
    sys.path.append(scripts_path)

project_root = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

from utils.logger import AppLogger, redact_sensitive_data, setup_logger

applogger = AppLogger(inputdataname="MainModule")
applogger.init_logger("INFO")
logger = setup_logger(__name__)

from core.data_container.container import DataContainer
from core.pipeline.step_executor import StepExecutor
from core.pipeline.result import ensure_successful_result
from core.infrastructure.storage_adapter import storage_adapter
from core.infrastructure.storage_path_utils import normalize_path, is_remote_path

from api.schemas.pipeline import PipelineDefinition, PipelineNode, PipelineEdge


_node_results_cache: Dict[str, Any] = {}


def execute_step_batch_task(
    step_name: str, plugin_name: str, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]] = None
) -> Optional[DataContainer]:
    inputs = inputs or {}
    step_executor = StepExecutor()
    step_config = {"name": step_name, "plugin": plugin_name, "params": params}
    logger.info(
        f"execute_step_batch_task: '{step_name}' using plugin: '{plugin_name}' "
        f"params: '{redact_sensitive_data(params)}'"
    )
    return step_executor.execute_step(step_config, inputs)


def _submit_node_task_batch(
    node_id: str,
    nodes_map: Dict[str, PipelineNode],
    edges: List[PipelineEdge],
    project_root_dir: str,
    fail_stop: bool = True,
):
    if node_id in _node_results_cache:
        return _node_results_cache[node_id]
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

    upstream_inputs = {}
    if incoming_edges:
        source_result = _submit_node_task_batch(
            incoming_edges[0].source_node_id,
            nodes_map,
            edges,
            project_root_dir,
            fail_stop,
        )
        upstream_inputs["input_data"] = source_result
    params = node_def.params.copy()
    for key, value in params.items():
        if isinstance(value, str) and ("path" in key or "_file" in key):
            params[key] = normalize_path(value, project_root_dir)
    result = execute_step_batch_task(
        step_name=node_def.id, plugin_name=node_def.plugin,
        params=params, inputs=upstream_inputs
    )
    if fail_stop:
        ensure_successful_result(result, node_def.id)
    _node_results_cache[node_id] = result
    return result


def run_pipeline_from_file(config_file_path: str, fail_stop: bool = True):
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
    logger.info(
        f"run_pipeline_from_file '{redact_sensitive_data(data)}'"
    )
    pipeline_def = PipelineDefinition(**data)

    logger.info(f"Starting batch pipeline run for: {pipeline_def.name}")
    _node_results_cache.clear()
    nodes_map = {node.id: node for node in pipeline_def.nodes}

    # source_node_ids に出てこないノード = どのエッジの出発点にもなっていない
    # ノード = 終着(sink)ノード。エッジで繋がっていない独立ノードもここに含まれ、
    # その実行順序は pipeline_def.nodes の宣言順（= このループの反復順）に従う。
    source_node_ids = {edge.source_node_id for edge in pipeline_def.edges}
    sink_node_ids = [nid for nid in nodes_map if nid not in source_node_ids]

    if not sink_node_ids:
        raise ValueError("No sink node found. Pipeline may have a circular dependency.")

    try:
        for node_id in sink_node_ids:
            _submit_node_task_batch(
                node_id,
                nodes_map,
                pipeline_def.edges,
                project_root,
                fail_stop,
            )
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        raise
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

    run_pipeline_from_file(config_file, fail_stop=True)


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

    run_pipeline_from_file(args.config_file, fail_stop=True)


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
