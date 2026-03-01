import os
import tempfile
from typing import Dict, Any, List

from core.data_container.container import DataContainer
from core.pipeline.step_executor import StepExecutor

# MIMEタイプ → 拡張子マッピング
MIME_SUFFIX_MAP = {
    "text/csv": ".csv",
    "application/json": ".json",
    "application/parquet": ".parquet",
    "application/octet-stream": ".bin",
    "text/plain": ".txt"
}

def get_suffix_from_headers(headers: Dict[str, Any]) -> str:
    content_type = headers.get("content-type", "").lower()
    return MIME_SUFFIX_MAP.get(content_type, ".bin")

def process_controlled_request(body_bytes: bytes, payload: Dict[str, Any], headers: Dict[str, Any]) -> Dict[str, Any]:
    steps = payload.get("steps", [])

    if not steps:
        raise ValueError("payload.steps is empty. At least one step is required.")

    suffix = get_suffix_from_headers(headers)

    fd, temp_path = tempfile.mkstemp(suffix=suffix)
    os.close(fd)
    try:
        with open(temp_path, "wb") as f:
            f.write(body_bytes)

        initial_container = DataContainer()
        initial_container.add_file_path(temp_path)
        initial_container.metadata["headers"] = headers

        executor = StepExecutor()
        container_stack: List[DataContainer] = [initial_container]

        import copy
        steps = copy.deepcopy(steps)

        if "params" in steps[0]:
            steps[0]["params"]["input_path"] = temp_path

        for idx, step in enumerate(steps):
            plugin_name = step.get("plugin")
            params = step.get("params", {})
            step_config = {
                "name": f"controlled_step_{idx}_{plugin_name}",
                "plugin": plugin_name,
                "params": params,
            }

            current_input = container_stack[-1]
            result = executor.execute_step(step_config, inputs={"input_data": current_input})

            if result is None or not result.file_paths:
                raise RuntimeError(f"Step '{plugin_name}' failed or returned no file paths.")

            container_stack.append(result)

        final_container = container_stack[-1]

        if not final_container.file_paths:
            raise RuntimeError("Final container has no file paths. Cannot return result.")

        return {
            "status": "ok",
            "final_metadata": final_container.metadata,
            "primary_file": final_container.get_primary_file_path()
        }

    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)