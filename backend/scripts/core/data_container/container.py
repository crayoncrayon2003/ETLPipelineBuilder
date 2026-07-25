from __future__ import annotations
from typing import Any, Dict, List, Optional, Union
import pandas as pd
from pathlib import Path
from enum import Enum

from utils.logger import setup_logger

logger = setup_logger(__name__)


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
        # data は明示的に渡された場合のみ確定値として扱う。未指定(None)の場合は
        # 「まだ読み込んでいない」状態にし、file_paths が後から埋まった時点で
        # .data への初回アクセス時に遅延読み込みできるようにする。
        self._data: Optional[pd.DataFrame] = data
        self._data_loaded: bool = data is not None
        self.metadata: Dict[str, Any] = dict(metadata) if metadata is not None else {}
        self.file_paths: List[str] = []
        self.errors: List[str] = []
        self.status: DataContainerStatus = status
        self.history: List[str] = []
        self.schema: Optional[Dict[str, Any]] = None

    @property
    def data(self) -> Optional[pd.DataFrame]:
        """
        プラグイン間でメモリ経由でデータを受け渡すための領域。

        上流プラグインが明示的に `container.data = df` を設定していればそれを
        そのまま返す(ファイルI/O無し)。設定されていない場合は、file_paths[0] から
        `storage_adapter.read_df()` で1回だけ読み込んでキャッシュする
        (ファイルパス経由の受け渡しと共存させるためのフォールバック)。
        表形式以外のフォーマットや読み込み失敗時は None を返す。
        """
        if not self._data_loaded:
            self._data = self._load_data_from_file_paths()
            self._data_loaded = True
        return self._data

    @data.setter
    def data(self, value: Optional[pd.DataFrame]) -> None:
        self._data = value
        self._data_loaded = True

    def _load_data_from_file_paths(self) -> Optional[pd.DataFrame]:
        if not self.file_paths:
            return None
        from core.infrastructure.storage_adapter import storage_adapter
        try:
            return storage_adapter.read_df(self.file_paths[0])
        except Exception as e:
            logger.debug(f"DataContainer: lazy load of '{self.file_paths[0]}' into .data failed: {e}")
            return None

    def __repr__(self) -> str:
        # .data のプロパティ経由アクセスは遅延読み込みを誘発するため、
        # print()/ログ出力だけでファイルI/Oが走らないよう内部状態を直接見る。
        data_shape = self._data.shape if self._data is not None else "N/A (file-based)"
        num_files = len(self.file_paths)
        return (
            f"<DataContainer | Data Shape: {data_shape} | "
            f"File Paths: {num_files} | Status: {self.status.value}>"
        )

    def add_file_path(self, path: Union[str, Path]) -> None:
        self.file_paths.append(str(path))

    def get_file_paths(self) -> List[str]:
        return self.file_paths

    def get_primary_file_path(self) -> str:
        if not self.file_paths:
            raise ValueError("DataContainer has no file paths.")
        return self.file_paths[0]

    def set_status(self, status: DataContainerStatus) -> None:
        if not isinstance(status, DataContainerStatus):
            raise TypeError(
                f"status must be a DataContainerStatus instance, got {type(status).__name__!r}."
            )
        self.status = status

    def get_status(self) -> DataContainerStatus:
        return self.status

    def add_history(self, plugin_name: str) -> None:
        self.history.append(plugin_name)

    def get_history(self) -> List[str]:
        return self.history

    def set_schema(self, schema: Dict[str, Any]) -> None:
        self.schema = schema

    def get_schema(self) -> Optional[Dict[str, Any]]:
        return self.schema

    def to_dict(self) -> Dict[str, Any]:
        return {
            "data": self.data.to_dict(orient="records") if self.data is not None else None,
            "metadata": self.metadata,
            "file_paths": self.file_paths,
            "errors": self.errors,
            "status": self.status.value,
            "history": self.history,
            "schema": self.schema,
        }

    def add_error(self, error: str) -> None:
        self.errors.append(error)