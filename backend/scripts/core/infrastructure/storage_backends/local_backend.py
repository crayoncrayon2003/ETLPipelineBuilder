import os
import shutil
from datetime import datetime, timezone
from typing import Any, Dict, List

from .base_backend import BaseStorageBackend
from utils.logger import setup_logger

logger = setup_logger(__name__)


class LocalStorageBackend(BaseStorageBackend):
    """
    ローカルファイルシステム用バックエンド。
    """

    def read_bytes(self, path: str) -> bytes:
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Local file not found: {path}")
        with open(path, 'rb') as f:
            return f.read()

    def write_bytes(self, path: str, data: bytes) -> None:
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(path, 'wb') as f:
            f.write(data)

    def read_text(self, path: str, encoding: str = 'utf-8') -> str:
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Local file not found: {path}")
        with open(path, 'r', encoding=encoding) as f:
            return f.read()

    def write_text(self, path: str, text_content: str, encoding: str = 'utf-8') -> None:
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(path, 'w', encoding=encoding) as f:
            f.write(text_content)

    def exists(self, path: str) -> bool:
        return os.path.exists(path)

    def delete(self, path: str) -> None:
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Local file not found: {path}")
        os.remove(path)
        logger.info(f"Deleted local file: {path}")

    def get_size(self, path: str) -> int:
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Local file not found: {path}")
        return os.path.getsize(path)

    def list_files(self, path: str) -> List[str]:
        if os.path.isfile(path):
            return [path]
        elif os.path.isdir(path):
            result = []
            for root, _, files in os.walk(path):
                for f in files:
                    result.append(os.path.join(root, f))
            return result
        else:
            raise FileNotFoundError(f"Path not found: {path}")

    def mkdir(self, path: str, exist_ok: bool = True) -> None:
        os.makedirs(path, exist_ok=exist_ok)
        logger.info(f"Created local directory: {path}")

    def is_dir(self, path: str) -> bool:
        if os.path.isdir(path):
            return True
        _, ext = os.path.splitext(path)
        return ext == ""

    def rename(self, old_path: str, new_path: str) -> None:
        os.rename(old_path, new_path)
        logger.info(f"Renamed local file: {old_path} -> {new_path}")

    def stat(self, path: str) -> Dict[str, Any]:
        stat_result = os.stat(path)
        return {
            "size": stat_result.st_size,
            "last_modified": datetime.fromtimestamp(stat_result.st_mtime, tz=timezone.utc),
            "mode": stat_result.st_mode,
            "uid": stat_result.st_uid,
            "gid": stat_result.st_gid,
        }