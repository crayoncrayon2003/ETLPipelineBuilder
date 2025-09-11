import os
import pandas as pd
import shutil
from typing import Dict, Any, Optional, Union
import s3fs

from core.data_container.formats import SupportedFormats
from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

class StorageAdapter:

    def _get_storage_options(self, path: str) -> Dict[str, Any]:
        if path.startswith("s3://"):
            return {}
        return {}

    def read_df(self, path: str, read_options: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
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

    def write_df(self, df: pd.DataFrame, path: str, write_options: Optional[Dict[str, Any]] = None):
        logger.info(f"Writing {len(df)} rows to: {path}")
        options = self._get_storage_options(path)
        write_opts = write_options or {}

        if not path.startswith("s3://"):
            os.makedirs(os.path.dirname(path), exist_ok=True)

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
        logger.info(f"Reading text from: {path}")
        try:
            if path.startswith("s3://"):
                s3 = s3fs.S3FileSystem()
                with s3.open(path, 'r', encoding='utf-8') as f:
                    return f.read()
            else:
                if not os.path.isfile(path):
                    raise FileNotFoundError(f"Local file not found: {path}")
                with open(path, 'r', encoding='utf-8') as f:
                    return f.read()
        except ImportError:
            raise ImportError("s3fs is required for reading from S3.")
        except Exception as e:
            logger.error(f"Failed to read text from '{path}': {e}")
            raise

    def write_text(self, text_content: str, path: str):
        logger.info(f"Writing text content to: {path}")
        if path.startswith("s3://"):
            try:
                s3 = s3fs.S3FileSystem()
                with s3.open(path, 'w', encoding='utf-8') as f:
                    f.write(text_content)
            except ImportError:
                raise ImportError("s3fs is required for writing text to S3.")
        else:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'w', encoding='utf-8') as f:
                f.write(text_content)

    def read_bytes(self, path: str) -> bytes:
        logger.info(f"Reading bytes from: {path}")
        try:
            if path.startswith("s3://"):
                s3 = s3fs.S3FileSystem()
                with s3.open(path, 'rb') as f:
                    return f.read()
            else:
                if not os.path.isfile(path):
                    raise FileNotFoundError(f"Local file not found: {path}")
                with open(path, 'rb') as f:
                    return f.read()
        except ImportError:
            raise ImportError("s3fs is required for reading from S3.")
        except Exception as e:
            logger.error(f"Failed to read bytes from '{path}': {e}")
            raise

    def write_bytes(self, content: bytes, path: str):
        logger.info(f"Writing {len(content)} bytes to: {path}")
        if path.startswith("s3://"):
            try:
                s3 = s3fs.S3FileSystem()
                with s3.open(path, 'wb') as f:
                    f.write(content)
            except ImportError:
                raise ImportError("s3fs is required for writing bytes to S3.")
        else:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'wb') as f:
                f.write(content)

    def download_remote_file(self, remote_path: str, local_path: Union[str, os.PathLike]):
        local_path = os.path.abspath(local_path)
        logger.info(f"Downloading remote file '{remote_path}' to '{local_path}'...")

        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        if remote_path.startswith("s3://"):
            try:
                import boto3
                s3 = boto3.client('s3')
                bucket = remote_path.split('/')[2]
                key = '/'.join(remote_path.split('/')[3:])
                s3.download_file(bucket, key, local_path)
                logger.info("Download from S3 complete.")
            except ImportError:
                raise ImportError("boto3 is required for S3 downloads. Please install it.")
            except Exception as e:
                logger.error(f"Failed to download from S3: {e}")
                raise
        else:
            if not os.path.isfile(remote_path):
                raise FileNotFoundError(f"Remote file to download not found: {remote_path}")
            shutil.copy(remote_path, local_path)
            logger.info("Copied from local path complete.")

    def upload_local_file(self, local_path: Union[str, os.PathLike], remote_path: str):
        local_path = os.path.abspath(local_path)
        logger.info(f"Uploading local file '{local_path}' to '{remote_path}'...")

        if not os.path.isfile(local_path):
            raise FileNotFoundError(f"Local file to upload not found: {local_path}")

        if remote_path.startswith("s3://"):
            try:
                import boto3
                s3 = boto3.client('s3')
                bucket = remote_path.split('/')[2]
                key = '/'.join(remote_path.split('/')[3:])
                s3.upload_file(local_path, bucket, key)
                logger.info("Upload to S3 complete.")
            except ImportError:
                raise ImportError("boto3 is required for S3 uploads. Please install it.")
            except Exception as e:
                logger.error(f"Failed to upload to S3: {e}")
                raise
        else:
            os.makedirs(os.path.dirname(remote_path), exist_ok=True)
            shutil.copy(local_path, remote_path)
            logger.info("Copied to local path complete.")

    def copy_file(self, source_path: str, dest_path: str):
        logger.info(f"Copying file from '{source_path}' to '{dest_path}'...")
        df = self.read_df(source_path)
        self.write_df(df, dest_path)
        logger.info("Copy complete.")

# Singleton
storage_adapter = StorageAdapter()
