import os
import tempfile
import json
from typing import Dict, Any, Optional

from core.data_container.container import DataContainer
from core.pipeline.step_executor import StepExecutor
from core.pipeline.result import ensure_successful_result
from core.infrastructure import storage_adapter
from core.infrastructure.storage_path_utils import normalize_path
from api.schemas.pipeline import PipelineDefinition


def process_configured_request(
    body_bytes: bytes,
    config_path: str,
    headers: Dict[str, Any],
    project_root: Optional[str]
) -> Dict[str, Any]:
    config_text = storage_adapter.read_text(config_path)
    config_data = json.loads(config_text)
    if not config_data.get("nodes"):
        raise ValueError("pipeline_def.nodes is empty. At least one node is required.")
    pipeline_def = PipelineDefinition(**config_data)

    fd, temp_path = tempfile.mkstemp(suffix=".dat")
    os.close(fd)
    try:
        with open(temp_path, "wb") as f:
            f.write(body_bytes)

        initial_container = DataContainer()
        initial_container.add_file_path(temp_path)
        initial_container.metadata["headers"] = headers

        node_results_cache: Dict[str, Optional[DataContainer]] = {}
        nodes_map = {node.id: node for node in pipeline_def.nodes}

        step_executor = StepExecutor()

        def _submit_node(node_id: str) -> Optional[DataContainer]:
            if node_id in node_results_cache:
                return node_results_cache[node_id]
            node_def = nodes_map[node_id]

            # プラグインは単一の input_data しか受け取れないため、上流エッジは高々1本までしか
            # 対応できない。2本以上あると片方が黙って上書きされて消えるため、ここで検知して止める。
            incoming_edges = [e for e in pipeline_def.edges if e.target_node_id == node_id]
            if len(incoming_edges) > 1:
                raise ValueError(
                    f"Node '{node_id}' has {len(incoming_edges)} incoming edges "
                    f"(from {[e.source_node_id for e in incoming_edges]}), but a node can only "
                    f"receive a single upstream input_data. Multiple incoming edges to one node "
                    f"are not supported."
                )

            upstream_inputs = {}
            if incoming_edges:
                source_result = _submit_node(incoming_edges[0].source_node_id)
                upstream_inputs["input_data"] = source_result

            params = node_def.params.copy()
            for key, value in params.items():
                if isinstance(value, str) and ("path" in key or "_file" in key):
                    params[key] = normalize_path(value, project_root or os.getcwd())

            inputs = upstream_inputs or {"input_data": initial_container}

            result = step_executor.execute_step(
                {"name": node_def.id, "plugin": node_def.plugin, "params": params},
                inputs=inputs
            )
            ensure_successful_result(result, node_def.id)
            node_results_cache[node_id] = result
            return result

        # source_node_ids に出てこないノード = どのエッジの出発点にもなっていない
        # ノード = 終着(sink)ノード。エッジで繋がっていない独立ノードもここに含まれ、
        # その実行順序は pipeline_def.nodes の宣言順（= このループの反復順）に従う。
        source_node_ids = {edge.source_node_id for edge in pipeline_def.edges}
        sink_node_ids = [nid for nid in nodes_map if nid not in source_node_ids]

        if not sink_node_ids:
            raise ValueError("No sink node found. Pipeline may have a circular dependency.")

        final_container = None
        for sink_node_id in sink_node_ids:
            sink_result = _submit_node(sink_node_id)
            ensure_successful_result(sink_result, sink_node_id)
            final_container = sink_result

        if final_container is None:
            raise RuntimeError("Pipeline execution returned no result.")

        return {
            "status": "ok",
            "final_metadata": final_container.metadata,
            "primary_file": final_container.get_primary_file_path()
        }

    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)
