import os
import tempfile
import json
from typing import Dict, Any, Optional

from core.data_container.container import DataContainer, DataContainerStatus
from core.pipeline.step_executor import StepExecutor
from core.infrastructure import storage_adapter
from core.infrastructure.storage_path_utils import normalize_path
from api.schemas.pipeline import PipelineDefinition, PipelineNode, PipelineEdge

def process_configured_request(
    body_bytes: bytes,
    config_path: str,
    headers: Dict[str, Any],
    project_root: Optional[str]
) -> Dict[str, Any]:
    config_text = storage_adapter.read_text(config_path)
    config_data = json.loads(config_text)
    pipeline_def = PipelineDefinition(**config_data)

    if not pipeline_def.nodes:
        raise ValueError("pipeline_def.nodes is empty. At least one node is required.")

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
            upstream_inputs = {}
            for edge in pipeline_def.edges:
                if edge.target_node_id == node_id:
                    source_result = _submit_node(edge.source_node_id)
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
            node_results_cache[node_id] = result
            return result

        source_node_ids = {edge.source_node_id for edge in pipeline_def.edges}
        sink_node_ids = [nid for nid in nodes_map if nid not in source_node_ids]

        if not sink_node_ids:
            raise ValueError("No sink node found. Pipeline may have a circular dependency.")

        final_container = None
        for sink_node_id in sink_node_ids:
            final_container = _submit_node(sink_node_id)

        if final_container is None:
            raise RuntimeError("Pipeline execution returned no result.")
        if final_container.status == DataContainerStatus.ERROR:
            errors = ", ".join(final_container.errors) if final_container.errors else "unknown error"
            raise RuntimeError(f"Pipeline execution failed: {errors}")

        return {
            "status": "ok",
            "final_metadata": final_container.metadata,
            "primary_file": final_container.get_primary_file_path()
        }

    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)