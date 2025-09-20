from __future__ import annotations
from typing import Any, Dict, List, Optional, Union
import pandas as pd
from pathlib import Path
from enum import Enum

class DataContainerStatus(Enum):
    PENDING = "pending"
    SUCCESS = "success"
    ERROR = "error"
    SKIPPED = "skipped"
    VALIDATION_FAILED = "validation_failed"
    TRANSFORMED = "transformed"
    LOADED = "loaded"

class DataContainer:
    def __init__(
        self,
        status: DataContainerStatus = DataContainerStatus.PENDING,
        data: Optional[pd.DataFrame] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        self.data: Optional[pd.DataFrame] = data
        self.metadata: Dict[str, Any] = metadata or {}
        self.file_paths: List[str] = []
        self.errors: List[str] = []
        self.status: DataContainerStatus = status
        self.history: List[str] = []
        self.schema: Optional[Dict[str, Any]] = None

    def __repr__(self) -> str:
        data_shape = self.data.shape if self.data is not None else "N/A (file-based)"
        num_files = len(self.file_paths)
        return f"<DataContainer | Data Shape: {data_shape} | File Paths: {num_files} | Status: {self.status.value}>"

    def add_file_path(self, path: Union[str, Path]):
        self.file_paths.append(str(path))

    def get_file_paths(self) -> List[str]:
        return self.file_paths

    def get_primary_file_path(self) -> str:
        if not self.file_paths:
            raise FileNotFoundError("DataContainer has no file paths.")
        return self.file_paths[0]

    def set_status(self, status: DataContainerStatus):
        self.status = status

    def get_status(self) -> DataContainerStatus:
        return self.status

    def add_history(self, plugin_name: str):
        self.history.append(plugin_name)

    def get_history(self) -> List[str]:
        return self.history

    def set_schema(self, schema: Dict[str, Any]):
        self.schema = schema

    def get_schema(self) -> Optional[Dict[str, Any]]:
        return self.schema

    def to_dict(self) -> Dict[str, Any]:
        return {
            "data": self.data.to_dict() if self.data is not None else None,
            "metadata": self.metadata,
            "file_paths": self.file_paths,
            "errors": self.errors,
            "status": self.status.value,
            "history": self.history,
            "schema": self.schema
        }

    def add_error(self, error: str):
        self.errors.append(error)
        self.metadata.setdefault("errors", []).append(error)