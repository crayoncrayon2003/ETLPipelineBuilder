from typing import Optional

from core.data_container.container import DataContainer, DataContainerStatus


FAILURE_STATUSES = {
    DataContainerStatus.ERROR,
    DataContainerStatus.SKIPPED,
    DataContainerStatus.VALIDATION_FAILED,
}


def ensure_successful_result(
    result: Optional[DataContainer],
    step_name: str,
) -> DataContainer:
    """Raise when a framework step did not produce a successful result."""
    if result is None:
        raise RuntimeError(f"Step '{step_name}' returned no result.")
    if result.status == DataContainerStatus.PENDING:
        raise RuntimeError(
            f"Step '{step_name}' did not complete: status is pending."
        )
    if result.status in FAILURE_STATUSES:
        details = ", ".join(result.errors) if result.errors else "unknown error"
        raise RuntimeError(f"Step '{step_name}' failed: {details}")
    return result
