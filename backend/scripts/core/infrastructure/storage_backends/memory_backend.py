from datetime import datetime, timezone
from typing import Any, Dict, List

from .base_backend import BaseStorageBackend
from utils.logger import setup_logger

logger = setup_logger(__name__)


class MemoryStorageBackend(BaseStorageBackend):
    """
    インメモリストレージバックエンド。
    プラグイン間の中間データ受け渡しやテスト用途に使用する。

    ライフタイム:
        storage_adapter はシングルトンのため、
        プロセスが生きている間データは保持される。
        パイプライン完了後は storage_adapter.clear_memory() で明示的にクリアすること。

    使用例:
        output_path: "memory://pipeline_run_1/step1_output.csv"
        input_path:  "memory://pipeline_run_1/step1_output.csv"
    """

    def __init__(self):
        # key: "memory://some/key.csv", value: bytes
        self._store: Dict[str, bytes] = {}

    def _check_exists(self, path: str) -> None:
        if path not in self._store:
            raise FileNotFoundError(f"Memory path not found: {path}")

    def read_bytes(self, path: str) -> bytes:
        self._check_exists(path)
        return self._store[path]

    def write_bytes(self, path: str, data: bytes) -> None:
        self._store[path] = data

    def read_text(self, path: str, encoding: str = 'utf-8') -> str:
        self._check_exists(path)
        return self._store[path].decode(encoding)

    def write_text(self, path: str, text_content: str, encoding: str = 'utf-8') -> None:
        self._store[path] = text_content.encode(encoding)

    def exists(self, path: str) -> bool:
        return path in self._store

    def delete(self, path: str) -> None:
        self._check_exists(path)
        del self._store[path]
        logger.info(f"Deleted memory path: {path}")

    def get_size(self, path: str) -> int:
        self._check_exists(path)
        return len(self._store[path])

    def list_files(self, path: str) -> List[str]:
        prefix = path if path.endswith("/") else path + "/"
        return [k for k in self._store if k.startswith(prefix)]

    def mkdir(self, path: str, exist_ok: bool = True) -> None:
        # メモリストレージはディレクトリの概念がないため何もしない
        logger.info(f"mkdir on memory path is a no-op: {path}")

    def is_dir(self, path: str) -> bool:
        # メモリストレージはファイル単位のため常にFalse
        return False

    def rename(self, old_path: str, new_path: str) -> None:
        self._check_exists(old_path)
        self._store[new_path] = self._store.pop(old_path)
        logger.info(f"Renamed memory path: {old_path} -> {new_path}")

    def stat(self, path: str) -> Dict[str, Any]:
        self._check_exists(path)
        return {
            "size": len(self._store[path]),
            "last_modified": datetime.now(tz=timezone.utc),
            "content_type": None,
            "etag": None,
            "storage_class": "MEMORY",
        }

    def clear(self, prefix: str = None) -> None:
        """
        メモリストアをクリアする。
        prefix を指定した場合はそのプレフィックスで始まるキーのみ削除する。
        """
        if prefix is None:
            self._store.clear()
            logger.info("Memory store cleared.")
        else:
            keys = [k for k in self._store if k.startswith(prefix)]
            for k in keys:
                del self._store[k]
            logger.info(f"Memory store cleared for prefix: {prefix} ({len(keys)} keys removed)")