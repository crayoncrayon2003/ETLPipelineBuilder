import os, tempfile, json
from typing import Dict, Any, Optional
from core.data_container.container import DataContainer
from core.pipeline.step_executor import StepExecutor
from core.infrastructure import storage_adapter
from core.infrastructure.storage_path_utils import normalize_path
from api.schemas.pipeline import PipelineDefinition, PipelineNode, PipelineEdge

_node_results_cache = {}

def process_configured_request(body_bytes: bytes, config_path: str, headers: Dict[str, Any], project_root: Optional[str]) -> Dict[str, Any]:
    config_text = storage_adapter.read_text(config_path)
    config_data = json.loads(config_text)
    pipeline_def = PipelineDefinition(**config_data)

    fd, temp_path = tempfile.mkstemp(suffix=".dat")
    os.close(fd)
    with open(temp_path, "wb") as f:
        f.write(body_bytes)

    initial_container = DataContainer()
    initial_container.add_file_path(temp_path)
    initial_container.metadata["headers"] = headers

    _node_results_cache.clear()
    nodes_map = {node.id: node for node in pipeline_def.nodes}

    def _submit_node(node_id: str) -> Optional[DataContainer]:
        if node_id in _node_results_cache:
            return _node_results_cache[node_id]
        node_def = nodes_map[node_id]
        upstream_inputs = {}
        for edge in pipeline_def.edges:
            if edge.target_node_id == node_id:
                source_result = _submit_node(edge.source_node_id)
                upstream_inputs[edge.target_input_name] = source_result

        params = node_def.params.copy()
        for key, value in params.items():
            if isinstance(value, str) and ("path" in key or "_file" in key):
                params[key] = normalize_path(value, project_root or os.getcwd())

        inputs = upstream_inputs or {"input_data": initial_container}
        result = StepExecutor().execute_step(
            {"name": node_def.id, "plugin": node_def.plugin, "params": params},
            inputs=inputs
        )
        _node_results_cache[node_id] = result
        return result

    final_node_id = pipeline_def.nodes[-1].id
    final_container = _submit_node(final_node_id)

    return {
        "status": "ok",
        "final_metadata": final_container.metadata,
        "primary_file": final_container.get_primary_file_path()
    }
