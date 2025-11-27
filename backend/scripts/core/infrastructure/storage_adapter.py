import os
import pandas as pd
import shutil
from typing import Dict, Any, Optional, Union
import s3fs
from datetime import datetime
from core.data_container.formats import SupportedFormats
from .storage_path_utils import normalize_path, is_remote_path, is_local_path

from utils.logger import setup_logger

logger = setup_logger(__name__)

class StorageAdapter:

    def _get_storage_options(self, path: str) -> Dict[str, Any]:
        # Normalize path to handle schemes and local paths
        if is_remote_path(path):
            # For remote paths like s3, http, https, etc.
            return {}
        return {}

    def read_df(self, path: str, read_options: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        logger.info(f"Reading DataFrame from: {path}")
        options = self._get_storage_options(path)
        read_opts = read_options.copy() if read_options else {}

        spark = read_opts.pop("spark", None)
        normalized_path = normalize_path(path, os.getcwd())
        file_format = SupportedFormats.from_path(normalized_path)

        try:
            if spark is not None:
                # Return as Spark DataFrame
                if file_format == SupportedFormats.CSV:
                    return spark.read.options(**read_opts).csv(normalized_path)
                elif file_format == SupportedFormats.PARQUET:
                    return spark.read.options(**read_opts).parquet(normalized_path)
                elif file_format in [SupportedFormats.JSON, SupportedFormats.JSONL]:
                    return spark.read.options(**read_opts).json(normalized_path)
                else:
                    raise ValueError(f"Spark read not supported for format '{file_format.value}'")
            else:
                # Return as pandas DataFrame
                if file_format == SupportedFormats.CSV:
                    return pd.read_csv(normalized_path, storage_options=options, **read_opts)
                elif file_format == SupportedFormats.PARQUET:
                    return pd.read_parquet(normalized_path, storage_options=options, **read_opts)
                elif file_format == SupportedFormats.EXCEL:
                    return pd.read_excel(normalized_path, storage_options=options, **read_opts)
                elif file_format in [SupportedFormats.JSON, SupportedFormats.JSONL]:
                    return pd.read_json(normalized_path, lines=True, storage_options=options, **read_opts)
                else:
                    raise ValueError(f"Reading DataFrame from format '{file_format.value}' is not supported.")
        except Exception as e:
            logger.error(f"Failed to read file from '{path}': {e}")
            raise

    def write_df(self, df: pd.DataFrame, path: str, write_options: Optional[Dict[str, Any]] = None):
        logger.info(f"Writing {len(df)} rows to: {path}")
        options = self._get_storage_options(path)
        write_opts = write_options.copy() if write_options else {}

        spark = write_opts.pop("spark", None)
        normalized_path = normalize_path(path, os.getcwd())
        file_format = SupportedFormats.from_path(normalized_path)

        try:
            if spark is not None:
                # Write as Spark DataFrame
                if file_format == SupportedFormats.CSV:
                    df.write.options(**write_opts).mode("overwrite").csv(normalized_path)
                elif file_format == SupportedFormats.PARQUET:
                    df.write.options(**write_opts).mode("overwrite").parquet(normalized_path)
                elif file_format in [SupportedFormats.JSON, SupportedFormats.JSONL]:
                    df.write.options(**write_opts).mode("overwrite").json(normalized_path)
                else:
                    raise ValueError(f"Spark write not supported for format '{file_format.value}'")
            else:
                # Write as pandas DataFrame
                if not is_remote_path(path):
                    os.makedirs(os.path.dirname(normalized_path), exist_ok=True)

                if file_format == SupportedFormats.CSV:
                    df.to_csv(normalized_path, index=False, storage_options=options, **write_opts)
                elif file_format == SupportedFormats.PARQUET:
                    df.to_parquet(normalized_path, index=False, storage_options=options, **write_opts)
                elif file_format == SupportedFormats.EXCEL:
                    df.to_excel(normalized_path, index=False, storage_options=options, **write_opts)
                elif file_format in [SupportedFormats.JSON, SupportedFormats.JSONL]:
                    df.to_json(normalized_path, orient='records', lines=True, storage_options=options, **write_opts)
                else:
                    raise ValueError(f"Writing DataFrame to format '{file_format.value}' is not supported.")
        except Exception as e:
            logger.error(f"Failed to write file to '{path}': {e}")
            raise

    def read_text(self, path: str, encoding: str='utf-8') -> str:
        logger.info(f"Reading text from: {path}")
        try:
            normalized_path = normalize_path(path, os.getcwd())

            if is_remote_path(path):
                s3 = s3fs.S3FileSystem()
                with s3.open(normalized_path, 'r', encoding=encoding) as f:
                    return f.read()
            else:
                if not os.path.isfile(normalized_path):
                    raise FileNotFoundError(f"Local file not found: {normalized_path}")
                with open(normalized_path, 'r', encoding=encoding) as f:
                    return f.read()
        except ImportError:
            raise ImportError("s3fs is required for reading from S3.")
        except Exception as e:
            logger.error(f"Failed to read text from '{path}': {e}")
            raise

    def write_text(self, text_content: str, path: str, encoding: str='utf-8'):
        logger.info(f"Writing text content to: {path}")
        normalized_path = normalize_path(path, os.getcwd())

        if is_remote_path(path):
            try:
                s3 = s3fs.S3FileSystem()
                with s3.open(normalized_path, 'w', encoding=encoding) as f:
                    f.write(text_content)
            except ImportError:
                raise ImportError("s3fs is required for writing text to S3.")
        else:
            os.makedirs(os.path.dirname(normalized_path), exist_ok=True)
            with open(normalized_path, 'w', encoding=encoding) as f:
                f.write(text_content)

    def read_bytes(self, path: str) -> bytes:
        logger.info(f"Reading bytes from: {path}")
        try:
            normalized_path = normalize_path(path, os.getcwd())

            if is_remote_path(path):
                s3 = s3fs.S3FileSystem()
                with s3.open(normalized_path, 'rb') as f:
                    return f.read()
            else:
                if not os.path.isfile(normalized_path):
                    raise FileNotFoundError(f"Local file not found: {normalized_path}")
                with open(normalized_path, 'rb') as f:
                    return f.read()
        except ImportError:
            raise ImportError("s3fs is required for reading from S3.")
        except Exception as e:
            logger.error(f"Failed to read bytes from '{path}': {e}")
            raise

    def write_bytes(self, content: bytes, path: str):
        logger.info(f"Writing {len(content)} bytes to: {path}")
        normalized_path = normalize_path(path, os.getcwd())

        if is_remote_path(path):
            try:
                s3 = s3fs.S3FileSystem()
                with s3.open(normalized_path, 'wb') as f:
                    f.write(content)
            except ImportError:
                raise ImportError("s3fs is required for writing bytes to S3.")
        else:
            os.makedirs(os.path.dirname(normalized_path), exist_ok=True)
            with open(normalized_path, 'wb') as f:
                f.write(content)

    def download_remote_file(self, remote_path: str, local_path: Union[str, os.PathLike]):
        local_path = os.path.abspath(local_path)
        logger.info(f"Downloading remote file '{remote_path}' to '{local_path}'...")

        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        normalized_remote_path = normalize_path(remote_path, os.getcwd())

        if is_remote_path(remote_path):
            try:
                import boto3
                s3 = boto3.client('s3')
                bucket = normalized_remote_path.split('/')[2]
                key = '/'.join(normalized_remote_path.split('/')[3:])
                s3.download_file(bucket, key, local_path)
                logger.info("Download from S3 complete.")
            except ImportError:
                raise ImportError("boto3 is required for S3 downloads. Please install it.")
            except Exception as e:
                logger.error(f"Failed to download from S3: {e}")
                raise
        else:
            if not os.path.isfile(normalized_remote_path):
                raise FileNotFoundError(f"Remote file to download not found: {normalized_remote_path}")
            shutil.copy(normalized_remote_path, local_path)
            logger.info("Copied from local path complete.")

    def upload_local_file(self, local_path: Union[str, os.PathLike], remote_path: str):
        local_path = os.path.abspath(local_path)
        logger.info(f"Uploading local file '{local_path}' to '{remote_path}'...")

        if not os.path.isfile(local_path):
            raise FileNotFoundError(f"Local file to upload not found: {local_path}")

        normalized_remote_path = normalize_path(remote_path, os.getcwd())

        if is_remote_path(remote_path):
            try:
                import boto3
                s3 = boto3.client('s3')
                bucket = normalized_remote_path.split('/')[2]
                key = '/'.join(normalized_remote_path.split('/')[3:])
                s3.upload_file(local_path, bucket, key)
                logger.info("Upload to S3 complete.")
            except ImportError:
                raise ImportError("boto3 is required for S3 uploads. Please install it.")
            except Exception as e:
                logger.error(f"Failed to upload to S3: {e}")
                raise
        else:
            os.makedirs(os.path.dirname(normalized_remote_path), exist_ok=True)
            shutil.copy(local_path, normalized_remote_path)
            logger.info("Copied to local path complete.")

    def list_files(self, path: str):
        normalized_path = normalize_path(path, os.getcwd())

        if is_remote_path(path):
            try:
                import boto3
                s3 = boto3.client("s3")
                bucket = normalized_path.split('/')[2]
                prefix = '/'.join(normalized_path.split('/')[3:])
                result = []
                paginator = s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                    for obj in page.get("Contents", []):
                        result.append(f"s3://{bucket}/{obj['Key']}")
                return result
            except ImportError:
                raise ImportError("boto3 is required for listing S3 files.")
        else:
            if os.path.isfile(normalized_path):
                return [normalized_path]
            elif os.path.isdir(normalized_path):
                result = []
                for root, _, files in os.walk(normalized_path):
                    for f in files:
                        result.append(os.path.join(root, f))
                return result
            else:
                raise FileNotFoundError(f"Path not found: {normalized_path}")

    def exists(self, path: str) -> bool:
        normalized_path = normalize_path(path, os.getcwd())
        if is_remote_path(path):
            try:
                import boto3
                s3 = boto3.client("s3")
                bucket = normalized_path.split("/")[2]
                key = "/".join(normalized_path.split("/")[3:])
                try:
                    s3.head_object(Bucket=bucket, Key=key)
                    return True
                except s3.exceptions.ClientError:
                    return False
            except ImportError:
                raise ImportError("boto3 is required for checking existence in S3.")
        else:
            return os.path.exists(normalized_path)

    def delete(self, path: str):
        normalized_path = normalize_path(path, os.getcwd())
        if is_remote_path(path):
            try:
                import boto3
                s3 = boto3.client("s3")
                bucket = normalized_path.split("/")[2]
                key = "/".join(normalized_path.split("/")[3:])
                s3.delete_object(Bucket=bucket, Key=key)
                logger.info(f"Deleted S3 object: {path}")
            except ImportError:
                raise ImportError("boto3 is required for deleting S3 files.")
        else:
            if os.path.isfile(normalized_path):
                os.remove(normalized_path)
                logger.info(f"Deleted local file: {normalized_path}")
            else:
                raise FileNotFoundError(f"Local file not found: {normalized_path}")

    def get_size(self, path: str) -> int:
        normalized_path = normalize_path(path, os.getcwd())
        if is_remote_path(path):
            try:
                import boto3
                s3 = boto3.client("s3")
                bucket = normalized_path.split("/")[2]
                key = "/".join(normalized_path.split("/")[3:])
                response = s3.head_object(Bucket=bucket, Key=key)
                return response["ContentLength"]
            except ImportError:
                raise ImportError("boto3 is required for getting S3 object size.")
        else:
            if not os.path.isfile(normalized_path):
                raise FileNotFoundError(f"Local file not found: {normalized_path}")
            return os.path.getsize(normalized_path)

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
        """
        Create a directory (local or S3 prefix).
        For S3, we simulate by creating a dummy "folder" key ending with "/".
        """
        normalized_path = normalize_path(path, os.getcwd())

        if is_remote_path(path):
            # S3 prefix "directories" are logical, so we just ensure it exists by writing a marker
            import boto3
            s3 = boto3.client("s3")
            bucket = normalized_path.split('/')[2]
            prefix = '/'.join(normalized_path.split('/')[3:]).rstrip('/') + "/"
            if not exist_ok:
                # Check if prefix already exists
                response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
                if "Contents" in response:
                    raise FileExistsError(f"S3 prefix already exists: {normalized_path}")
            # Create a zero-byte object to represent the directory
            s3.put_object(Bucket=bucket, Key=prefix)
            logger.info(f"Created S3 directory prefix: {normalized_path}")
        else:
            os.makedirs(normalized_path, exist_ok=exist_ok)
            logger.info(f"Created local directory: {normalized_path}")

    def is_dir(self, path: str) -> bool:
        """
        Check if a path is a directory (local or S3 prefix).
        """
        normalized_path = normalize_path(path, os.getcwd())

        if is_remote_path(path):
            import boto3
            s3 = boto3.client("s3")
            bucket = normalized_path.split('/')[2]
            prefix = '/'.join(normalized_path.split('/')[3:]).rstrip('/') + "/"
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
            return "Contents" in response
        else:
            return os.path.isdir(normalized_path)

    def rename(self, old_path: str, new_path: str):
        """
        Rename/move a file or directory.
        On S3, performs copy + delete.
        """
        old_normalized = normalize_path(old_path, os.getcwd())
        new_normalized = normalize_path(new_path, os.getcwd())

        if is_remote_path(old_path):
            import boto3
            s3 = boto3.client("s3")
            old_bucket = old_normalized.split('/')[2]
            old_key = '/'.join(old_normalized.split('/')[3:])
            new_bucket = new_normalized.split('/')[2]
            new_key = '/'.join(new_normalized.split('/')[3:])

            # Copy to new location
            s3.copy_object(
                Bucket=new_bucket,
                CopySource={"Bucket": old_bucket, "Key": old_key},
                Key=new_key,
            )
            # Delete old object
            s3.delete_object(Bucket=old_bucket, Key=old_key)
            logger.info(f"Renamed S3 object: {old_normalized} → {new_normalized}")
        else:
            os.rename(old_normalized, new_normalized)
            logger.info(f"Renamed local file: {old_normalized} → {new_normalized}")

    def stat(self, path: str) -> Dict[str, Any]:
        """
        Get file metadata (size, modified time, etc.).
        Returns a dictionary with common fields.
        """
        normalized_path = normalize_path(path, os.getcwd())

        if is_remote_path(path):
            import boto3
            s3 = boto3.client("s3")
            bucket = normalized_path.split('/')[2]
            key = '/'.join(normalized_path.split('/')[3:])
            response = s3.head_object(Bucket=bucket, Key=key)
            return {
                "size": response["ContentLength"],
                "last_modified": response["LastModified"],
                "content_type": response.get("ContentType"),
                "etag": response.get("ETag"),
                "storage_class": response.get("StorageClass", "STANDARD"),
            }
        else:
            stat_result = os.stat(normalized_path)
            return {
                "size": stat_result.st_size,
                "last_modified": datetime.fromtimestamp(stat_result.st_mtime),
                "mode": stat_result.st_mode,
                "uid": stat_result.st_uid,
                "gid": stat_result.st_gid,
            }

# Singleton instance
storage_adapter = StorageAdapter()
