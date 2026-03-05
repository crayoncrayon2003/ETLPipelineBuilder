import io
import os
import shutil
import pandas as pd
from typing import Any, Dict, List, Optional, Union

from core.data_container.formats import SupportedFormats
from .storage_path_utils import normalize_path, is_remote_path, is_local_path, is_memory_path
from .storage_backends import LocalStorageBackend, S3StorageBackend, MemoryStorageBackend
from utils.logger import setup_logger

logger = setup_logger(__name__)


class StorageAdapter:
    """
    ストレージ操作の統一インターフェース。
    パスのスキームに応じて適切なバックエンドに委譲する。

    対応スキーム:
        (スキームなし / file://)  → LocalStorageBackend
        s3://                     → S3StorageBackend
        memory://                 → MemoryStorageBackend
    """

    def __init__(self):
        self._local   = LocalStorageBackend()
        self._s3      = S3StorageBackend()
        self._memory  = MemoryStorageBackend()

    def _get_backend(self, path: str):
        if is_memory_path(path):
            return self._memory
        if is_remote_path(path):
            return self._s3
        return self._local

    def _normalize(self, path: str) -> str:
        """memory:// と s3:// はそのまま、ローカルパスのみ正規化する"""
        if is_memory_path(path) or is_remote_path(path):
            return path
        return normalize_path(path, os.getcwd())

    # ------------------------------------------------------------------
    # DataFrame read/write
    # ------------------------------------------------------------------

    def read_df(self, path: str, read_options: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        logger.info(f"Reading DataFrame from: {path}")
        read_opts = read_options.copy() if read_options else {}
        spark = read_opts.pop("spark", None)
        normalized = self._normalize(path)
        file_format = SupportedFormats.from_path(normalized)

        try:
            if is_memory_path(path):
                data = self._memory.read_bytes(normalized)
                return self._deserialize_df(data, file_format, read_opts)

            if spark is not None:
                return self._spark_read_df(spark, normalized, file_format, read_opts)

            options = {}
            if file_format == SupportedFormats.CSV:
                return pd.read_csv(normalized, storage_options=options, **read_opts)
            elif file_format == SupportedFormats.PARQUET:
                return pd.read_parquet(normalized, storage_options=options, **read_opts)
            elif file_format == SupportedFormats.EXCEL:
                return pd.read_excel(normalized, storage_options=options, **read_opts)
            elif file_format in [SupportedFormats.JSON, SupportedFormats.JSONL]:
                return pd.read_json(normalized, lines=True, storage_options=options, **read_opts)
            else:
                raise ValueError(f"Reading DataFrame from format '{file_format.value}' is not supported.")
        except Exception as e:
            logger.error(f"Failed to read file from '{path}': {e}")
            raise

    def write_df(self, df: pd.DataFrame, path: str, write_options: Optional[Dict[str, Any]] = None):
        logger.info(f"Writing {len(df)} rows to: {path}")
        write_opts = write_options.copy() if write_options else {}
        spark = write_opts.pop("spark", None)
        normalized = self._normalize(path)
        file_format = SupportedFormats.from_path(normalized)

        try:
            if is_memory_path(path):
                data = self._serialize_df(df, file_format, write_opts)
                self._memory.write_bytes(normalized, data)
                return

            if spark is not None:
                self._spark_write_df(spark, df, normalized, file_format, write_opts)
                return

            if not is_remote_path(path):
                parent = os.path.dirname(normalized)
                if parent:
                    os.makedirs(parent, exist_ok=True)

            options = {}
            if file_format == SupportedFormats.CSV:
                df.to_csv(normalized, index=False, storage_options=options, **write_opts)
            elif file_format == SupportedFormats.PARQUET:
                df.to_parquet(normalized, index=False, storage_options=options, **write_opts)
            elif file_format == SupportedFormats.EXCEL:
                df.to_excel(normalized, index=False, storage_options=options, **write_opts)
            elif file_format in [SupportedFormats.JSON, SupportedFormats.JSONL]:
                df.to_json(normalized, orient='records', lines=True, storage_options=options, **write_opts)
            else:
                raise ValueError(f"Writing DataFrame to format '{file_format.value}' is not supported.")
        except Exception as e:
            logger.error(f"Failed to write file to '{path}': {e}")
            raise

    def _deserialize_df(self, data: bytes, file_format: SupportedFormats, read_opts: dict) -> pd.DataFrame:
        buf = io.BytesIO(data)
        if file_format == SupportedFormats.CSV:
            return pd.read_csv(buf, **read_opts)
        elif file_format == SupportedFormats.PARQUET:
            return pd.read_parquet(buf, **read_opts)
        elif file_format in [SupportedFormats.JSON, SupportedFormats.JSONL]:
            return pd.read_json(buf, lines=True, **read_opts)
        else:
            raise ValueError(f"Reading DataFrame from memory format '{file_format.value}' is not supported.")

    def _serialize_df(self, df: pd.DataFrame, file_format: SupportedFormats, write_opts: dict) -> bytes:
        buf = io.BytesIO()
        if file_format == SupportedFormats.CSV:
            df.to_csv(buf, index=False, **write_opts)
        elif file_format == SupportedFormats.PARQUET:
            df.to_parquet(buf, index=False, **write_opts)
        elif file_format in [SupportedFormats.JSON, SupportedFormats.JSONL]:
            df.to_json(buf, orient='records', lines=True, **write_opts)
        else:
            raise ValueError(f"Writing DataFrame to memory format '{file_format.value}' is not supported.")
        return buf.getvalue()

    def _spark_read_df(self, spark, path: str, file_format: SupportedFormats, read_opts: dict):
        if file_format == SupportedFormats.CSV:
            return spark.read.options(**read_opts).csv(path)
        elif file_format == SupportedFormats.PARQUET:
            return spark.read.options(**read_opts).parquet(path)
        elif file_format in [SupportedFormats.JSON, SupportedFormats.JSONL]:
            return spark.read.options(**read_opts).json(path)
        else:
            raise ValueError(f"Spark read not supported for format '{file_format.value}'")

    def _spark_write_df(self, spark, df, path: str, file_format: SupportedFormats, write_opts: dict):
        if file_format == SupportedFormats.CSV:
            df.write.options(**write_opts).mode("overwrite").csv(path)
        elif file_format == SupportedFormats.PARQUET:
            df.write.options(**write_opts).mode("overwrite").parquet(path)
        elif file_format in [SupportedFormats.JSON, SupportedFormats.JSONL]:
            df.write.options(**write_opts).mode("overwrite").json(path)
        else:
            raise ValueError(f"Spark write not supported for format '{file_format.value}'")

    # ------------------------------------------------------------------
    # Text read/write
    # ------------------------------------------------------------------

    def read_text(self, path: str, encoding: str = 'utf-8') -> str:
        logger.info(f"Reading text from: {path}")
        try:
            normalized = self._normalize(path)
            return self._get_backend(path).read_text(normalized, encoding)
        except Exception as e:
            logger.error(f"Failed to read text from '{path}': {e}")
            raise

    def write_text(self, text_content: str, path: str, encoding: str = 'utf-8'):
        logger.info(f"Writing text content to: {path}")
        normalized = self._normalize(path)
        self._get_backend(path).write_text(normalized, text_content, encoding)

    # ------------------------------------------------------------------
    # Bytes read/write
    # ------------------------------------------------------------------

    def read_bytes(self, path: str) -> bytes:
        logger.info(f"Reading bytes from: {path}")
        try:
            normalized = self._normalize(path)
            return self._get_backend(path).read_bytes(normalized)
        except Exception as e:
            logger.error(f"Failed to read bytes from '{path}': {e}")
            raise

    def write_bytes(self, content: bytes, path: str):
        logger.info(f"Writing {len(content)} bytes to: {path}")
        normalized = self._normalize(path)
        self._get_backend(path).write_bytes(normalized, content)

    # ------------------------------------------------------------------
    # File operations
    # ------------------------------------------------------------------

    def download_remote_file(self, remote_path: str, local_path: Union[str, os.PathLike]):
        local_path = os.path.abspath(local_path)
        logger.info(f"Downloading remote file '{remote_path}' to '{local_path}'...")
        parent = os.path.dirname(local_path)
        if parent:
            os.makedirs(parent, exist_ok=True)

        if is_remote_path(remote_path):
            normalized = self._normalize(remote_path)
            self._s3.download_file(normalized, local_path)
        else:
            normalized = self._normalize(remote_path)
            if not os.path.isfile(normalized):
                raise FileNotFoundError(f"Remote file to download not found: {normalized}")
            shutil.copy(normalized, local_path)
            logger.info("Copied from local path complete.")

    def upload_local_file(self, local_path: Union[str, os.PathLike], remote_path: str):
        local_path = os.path.abspath(local_path)
        logger.info(f"Uploading local file '{local_path}' to '{remote_path}'...")
        if not os.path.isfile(local_path):
            raise FileNotFoundError(f"Local file to upload not found: {local_path}")

        if is_remote_path(remote_path):
            normalized = self._normalize(remote_path)
            if normalized.endswith("/"):
                normalized = normalized + os.path.basename(local_path)
            self._s3.upload_file(local_path, normalized)
        else:
            normalized = self._normalize(remote_path)
            parent = os.path.dirname(normalized)
            if parent:
                os.makedirs(parent, exist_ok=True)
            shutil.copy(local_path, normalized)
            logger.info("Copied to local path complete.")

    def list_files(self, path: str) -> List[str]:
        normalized = self._normalize(path)
        return self._get_backend(path).list_files(normalized)

    def exists(self, path: str) -> bool:
        normalized = self._normalize(path)
        return self._get_backend(path).exists(normalized)

    def delete(self, path: str):
        normalized = self._normalize(path)
        self._get_backend(path).delete(normalized)

    def get_size(self, path: str) -> int:
        normalized = self._normalize(path)
        return self._get_backend(path).get_size(normalized)

    def copy_file(self, source_path: str, dest_path: str):
        logger.info(f"Copying file from '{source_path}' to '{dest_path}'...")
        df = self.read_df(source_path)
        self.write_df(df, dest_path)
        logger.info("Copy complete.")

    def copy_file_raw(self, source: str, dest: str):
        logger.info(f"Copying raw file from '{source}' to '{dest}'...")
        content = self.read_bytes(source)
        self.write_bytes(content, dest)
        logger.info("Raw copy complete.")

    def move_file(self, source: str, dest: str):
        self.copy_file_raw(source, dest)
        self.delete(source)
        logger.info(f"Moved file from '{source}' to '{dest}'")

    def mkdir(self, path: str, exist_ok: bool = True):
        normalized = self._normalize(path)
        self._get_backend(path).mkdir(normalized, exist_ok)

    def is_dir(self, path: str) -> bool:
        normalized = self._normalize(path)
        return self._get_backend(path).is_dir(normalized)

    def rename(self, old_path: str, new_path: str):
        old_normalized = self._normalize(old_path)
        new_normalized = self._normalize(new_path)
        self._get_backend(old_path).rename(old_normalized, new_normalized)

    def stat(self, path: str) -> Dict[str, Any]:
        normalized = self._normalize(path)
        return self._get_backend(path).stat(normalized)

    # ------------------------------------------------------------------
    # memory:// 専用操作
    # ------------------------------------------------------------------

    def clear_memory(self, prefix: str = None) -> None:
        """
        メモリストアをクリアする。
        prefix を指定した場合はそのプレフィックスで始まるキーのみ削除する。
        """
        self._memory.clear(prefix)


# Singleton instance
storage_adapter = StorageAdapter()