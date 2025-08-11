# backend/core/data_container/container.py

from __future__ import annotations
from typing import Any, Dict, List, Optional
import pandas as pd
from pathlib import Path

class DataContainer:
    """
    A standardized container for passing data pointers between ETL steps.
    In a file-based approach, this container primarily carries file paths.
    The `data` attribute is used for data that is small enough to be kept in memory.
    """

    def __init__(
        self,
        data: Optional[pd.DataFrame] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        self.data: Optional[pd.DataFrame] = data
        self.metadata: Dict[str, Any] = metadata or {}
        self.file_paths: List[Path] = []

    def __repr__(self) -> str:
        data_shape = self.data.shape if self.data is not None else "N/A (file-based)"
        num_files = len(self.file_paths)
        return (f"<DataContainer | Data Shape: {data_shape} | File Paths: {num_files}>")

    def add_file_path(self, path: str | Path):
        """Adds a file path associated with the data in this container."""
        self.file_paths.append(Path(path))

    def get_file_paths(self) -> List[Path]:
        """Returns the list of all file paths in the container."""
        return self.file_paths

    def get_primary_file_path(self) -> Path:
        """
        Returns the first file path in the list.
        Raises an error if the list is empty.
        """
        if not self.file_paths:
            raise FileNotFoundError("DataContainer has no file paths.")
        return self.file_paths[0]