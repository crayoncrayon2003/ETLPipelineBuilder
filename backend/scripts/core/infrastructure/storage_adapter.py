import os
from pathlib import Path
import pandas as pd
import shutil
from typing import Dict, Any, Optional,Union
import s3fs

# Import the Enum from our single source of truth
from core.data_container.formats import SupportedFormats

from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

class StorageAdapter:
    """
    Acts as an adapter to various storage systems (local filesystem, S3, etc.),
    providing a unified interface for the framework to read and write data.
    """

    def _get_storage_options(self, path: str) -> Dict[str, Any]:
        """
        Prepares storage options for pandas I/O functions.
        For S3, s3fs will automatically use credentials configured for boto3.
        """
        if path.startswith("s3://"):
            return {}
        return {}

    # def read_df(self, path: str, read_options: Dict[str, Any] | None = None) -> pd.DataFrame:
    def read_df(self, path: str, read_options: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Reads a file from a given path (local or S3) into a pandas DataFrame,
        inferring the format from the file extension.
        """
        logger.info(f"Reading DataFrame from: {path}")
        options = self._get_storage_options(path)
        read_opts = read_options or {}

        file_format = SupportedFormats.from_path(path)

        try:
            if file_format == SupportedFormats.CSV:
                return pd.read_csv(path, storage_options=options, **read_opts)
            elif file_format == SupportedFormats.PARQUET:
                return pd.read_parquet(path, storage_options=options, **read_opts)
            elif file_format == SupportedFormats.EXCEL:
                return pd.read_excel(path, storage_options=options, **read_opts)
            elif file_format in [SupportedFormats.JSON, SupportedFormats.JSONL]:
                return pd.read_json(path, lines=True, storage_options=options, **read_opts)
            else:
                raise ValueError(f"Reading DataFrame from format '{file_format.value}' is not supported.")
        except Exception as e:
            logger.error(f"Failed to read file from '{path}': {e}")
            raise

    # def write_df(self, df: pd.DataFrame, path: str, write_options: Dict[str, Any] | None = None):
    def write_df(self, df: pd.DataFrame, path: str, write_options: Optional[Dict[str, Any]] = None):
        """
        Writes a pandas DataFrame to a given path (local or S3).
        """
        logger.info(f"Writing {len(df)} rows to: {path}")
        options = self._get_storage_options(path)
        write_opts = write_options or {}

        if not path.startswith("s3://"):
            Path(path).parent.mkdir(parents=True, exist_ok=True)

        file_format = SupportedFormats.from_path(path)

        try:
            if file_format == SupportedFormats.CSV:
                df.to_csv(path, index=False, storage_options=options, **write_opts)
            elif file_format == SupportedFormats.PARQUET:
                df.to_parquet(path, index=False, storage_options=options, **write_opts)
            elif file_format == SupportedFormats.EXCEL:
                df.to_excel(path, index=False, storage_options=options, **write_opts)
            elif file_format in [SupportedFormats.JSON, SupportedFormats.JSONL]:
                df.to_json(path, orient='records', lines=True, storage_options=options, **write_opts)
            else:
                raise ValueError(f"Writing DataFrame to format '{file_format.value}' is not supported.")
        except Exception as e:
            logger.error(f"Failed to write file to '{path}': {e}")
            raise

    def read_text(self, path: str) -> str:
        """
        Reads a file from a given path (local or S3) as a string.
        """
        logger.info(f"Reading text from: {path}")
        try:
            if path.startswith("s3://"):
                s3 = s3fs.S3FileSystem()
                with s3.open(path, 'r', encoding='utf-8') as f:
                    return f.read()
            else:
                p = Path(path)
                if not p.exists():
                    raise FileNotFoundError(f"Local file not found: {path}")
                return p.read_text(encoding='utf-8')
        except ImportError:
            raise ImportError("s3fs is required for reading from S3.")
        except Exception as e:
            logger.error(f"Failed to read text from '{path}': {e}")
            raise

    def write_text(self, text_content: str, path: str):
        """
        Writes a string of text to a given path (local or S3).
        """
        logger.info(f"Writing text content to: {path}")
        if path.startswith("s3://"):
            try:
                s3 = s3fs.S3FileSystem()
                with s3.open(path, 'w', encoding='utf-8') as f:
                    f.write(text_content)
            except ImportError:
                raise ImportError("s3fs is required for writing text to S3.")
        else:
            p = Path(path)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(text_content, encoding='utf-8')

    def read_bytes(self, path: str) -> bytes:
        """
        Reads a file from a given path (local or S3) as a byte string.
        """
        logger.info(f"Reading bytes from: {path}")
        try:
            if path.startswith("s3://"):
                s3 = s3fs.S3FileSystem()
                with s3.open(path, 'rb') as f:
                    return f.read()
            else:
                p = Path(path)
                if not p.exists():
                    raise FileNotFoundError(f"Local file not found: {path}")
                return p.read_bytes()
        except ImportError:
            raise ImportError("s3fs is required for reading from S3.")
        except Exception as e:
            logger.error(f"Failed to read bytes from '{path}': {e}")
            raise

    def write_bytes(self, content: bytes, path: str):
        """
        Writes a byte string to a given path (local or S3).
        """
        logger.info(f"Writing {len(content)} bytes to: {path}")
        if path.startswith("s3://"):
            try:
                s3 = s3fs.S3FileSystem()
                with s3.open(path, 'wb') as f:
                    f.write(content)
            except ImportError:
                raise ImportError("s3fs is required for writing bytes to S3.")
        else:
            p = Path(path)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(content)

    # def download_remote_file(self, remote_path: str, local_path: str | Path):
    def download_remote_file(self, remote_path: str, local_path: Union[str, Path]):
        """
        Downloads a file from a remote storage location (currently S3) to a local path.
        If the remote path is local, it performs a copy.
        """
        local_p = Path(local_path)
        logger.info(f"Downloading remote file '{remote_path}' to '{local_p}'...")

        # Ensure the parent directory for the local path exists
        local_p.parent.mkdir(parents=True, exist_ok=True)

        if remote_path.startswith("s3://"):
            try:
                import boto3
                s3 = boto3.client('s3')
                bucket = remote_path.split('/')[2]
                key = '/'.join(remote_path.split('/')[3:])
                s3.download_file(bucket, key, str(local_p))
                logger.info("Download from S3 complete.")
            except ImportError:
                raise ImportError("boto3 is required for S3 downloads. Please install it.")
            except Exception as e:
                logger.error(f"Failed to download from S3: {e}")
                raise
        else:
            # For local-to-local, this is a copy operation
            source = Path(remote_path)
            if not source.exists():
                raise FileNotFoundError(f"Remote file to download not found: {source}")
            shutil.copy(source, local_p)
            logger.info("Copied from local path complete.")

    # def upload_local_file(self, local_path: str | Path, remote_path: str):
    def upload_local_file(self, local_path: Union[str, Path], remote_path: str):
        """
        Uploads a local file to a remote storage location (currently S3).
        If the remote path is local, it performs a copy.
        """
        local_p = Path(local_path)
        logger.info(f"Uploading local file '{local_p}' to '{remote_path}'...")

        if not local_p.exists():
            raise FileNotFoundError(f"Local file to upload not found: {local_p}")

        if remote_path.startswith("s3://"):
            try:
                import boto3
                s3 = boto3.client('s3')
                bucket = remote_path.split('/')[2]
                key = '/'.join(remote_path.split('/')[3:])
                s3.upload_file(str(local_p), bucket, key)
                logger.info("Upload to S3 complete.")
            except ImportError:
                raise ImportError("boto3 is required for S3 uploads. Please install it.")
            except Exception as e:
                logger.error(f"Failed to upload to S3: {e}")
                raise
        else:
            # For local-to-local, this is a copy operation
            dest = Path(remote_path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(local_p, dest)
            logger.info("Copied to local path complete.")

    def copy_file(self, source_path: str, dest_path: str):
        """
        Copies a file from a source to a destination.
        This is a simplified version using read/write for now.
        """
        logger.info(f"Copying file from '{source_path}' to '{dest_path}'...")
        # A direct, more efficient copy (e.g., s3-to-s3) would be a future improvement.
        df = self.read_df(source_path)
        self.write_df(df, dest_path)
        logger.info("Copy complete.")

# Create a singleton instance to be used across the framework
storage_adapter = StorageAdapter()