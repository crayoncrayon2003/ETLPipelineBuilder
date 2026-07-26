import importlib
import asyncio
import json

import pytest
from fastapi import BackgroundTasks
from unittest.mock import patch

from api.routers.pipelines import run_pipeline
from api.schemas.pipeline import PipelineDefinition
from core.data_container.container import DataContainer, DataContainerStatus
from core.pipeline.step_executor import StepExecutor


def test_library_dictionary_call_interface_is_unchanged():
    expected = DataContainer(status=DataContainerStatus.SUCCESS)
    step_config = {
        "name": "node1",
        "plugin": "user_plugin",
        "params": {"output_path": "memory://run/output.csv"},
    }

    with patch(
        "core.plugin_manager.manager.framework_manager.call_plugin_execute",
        return_value=expected,
    ) as mock_execute:
        result = StepExecutor().execute_step(step_config, inputs={})

    assert result is expected
    mock_execute.assert_called_once_with(
        plugin_name="user_plugin",
        params={"output_path": "memory://run/output.csv"},
        inputs={},
    )


def test_api_body_interface_is_unchanged():
    pipeline = PipelineDefinition(
        name="API pipeline",
        nodes=[{"id": "node1", "plugin": "user_plugin", "params": {}}],
        edges=[],
    )
    background_tasks = BackgroundTasks()

    response = asyncio.run(run_pipeline(pipeline, background_tasks))

    assert response == {
        "message": "Immediate pipeline execution started.",
        "pipeline_name": "API pipeline",
    }
    assert len(background_tasks.tasks) == 1


@pytest.mark.parametrize(
    "module_name",
    [
        "run_pipeline_with_parameter_file1",
        "run_pipeline_with_parameter_file2",
    ],
)
def test_file_runner_propagates_failed_step(module_name, tmp_path, monkeypatch):
    """Both public file runners keep their IF and propagate ETL failure."""
    module = importlib.import_module(module_name)
    config_path = tmp_path / "pipeline.json"
    config_path.write_text(
        json.dumps(
            {
                "name": "failing pipeline",
                "nodes": [
                    {
                        "id": "node1",
                        "plugin": "user_plugin",
                        "params": {},
                    }
                ],
                "edges": [],
            }
        ),
        encoding="utf-8",
    )

    failed = DataContainer(status=DataContainerStatus.ERROR)
    failed.add_error("user plugin failed")
    monkeypatch.setattr(
        module,
        "execute_step_batch_task",
        lambda *args, **kwargs: failed,
    )

    with pytest.raises(RuntimeError, match="user plugin failed"):
        module.run_pipeline_from_file(str(config_path), fail_stop=True)
