import pytest

from core.data_container.container import DataContainer, DataContainerStatus
from core.pipeline.result import ensure_successful_result


def test_pending_result_is_rejected():
    result = DataContainer(status=DataContainerStatus.PENDING)

    with pytest.raises(RuntimeError, match="status is pending"):
        ensure_successful_result(result, "pending_step")


def test_success_result_is_returned_unchanged():
    result = DataContainer(status=DataContainerStatus.SUCCESS)

    assert ensure_successful_result(result, "successful_step") is result


def test_error_result_remains_rejected():
    result = DataContainer(status=DataContainerStatus.ERROR)
    result.add_error("plugin failed")

    with pytest.raises(RuntimeError, match="plugin failed"):
        ensure_successful_result(result, "failed_step")
