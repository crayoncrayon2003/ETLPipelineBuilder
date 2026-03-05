import os
from typing import Any, Dict, List

from .base_backend import BaseStorageBackend
from core.infrastructure.storage_path_utils import parse_s3_path
from utils.logger import setup_logger

logger = setup_logger(__name__)


class S3StorageBackend(BaseStorageBackend):
    """
    AWS S3用バックエンド。
    boto3 / s3fs は遅延importで読み込む（未インストール環境でのクラッシュを防ぐ）。
    """

    def _s3_client(self):
        try:
            import boto3
            return boto3.client('s3')
        except ImportError:
            raise ImportError("boto3 is required for S3 operations. Please install it.")

    def _s3fs(self):
        try:
            import s3fs
            return s3fs.S3FileSystem()
        except ImportError:
            raise ImportError("s3fs is required for S3 text/stream operations. Please install it.")

    def read_bytes(self, path: str) -> bytes:
        s3 = self._s3fs()
        with s3.open(path, 'rb') as f:
            return f.read()

    def write_bytes(self, path: str, data: bytes) -> None:
        s3 = self._s3fs()
        with s3.open(path, 'wb') as f:
            f.write(data)

    def read_text(self, path: str, encoding: str = 'utf-8') -> str:
        s3 = self._s3fs()
        with s3.open(path, 'r', encoding=encoding) as f:
            return f.read()

    def write_text(self, path: str, text_content: str, encoding: str = 'utf-8') -> None:
        s3 = self._s3fs()
        with s3.open(path, 'w', encoding=encoding) as f:
            f.write(text_content)

    def exists(self, path: str) -> bool:
        try:
            import boto3
            import botocore.exceptions
            s3 = boto3.client("s3")
            bucket, key = parse_s3_path(path)
            try:
                s3.head_object(Bucket=bucket, Key=key)
                return True
            except botocore.exceptions.ClientError:
                return False
        except ImportError:
            raise ImportError("boto3 is required for checking existence in S3.")

    def delete(self, path: str) -> None:
        s3 = self._s3_client()
        bucket, key = parse_s3_path(path)
        s3.delete_object(Bucket=bucket, Key=key)
        logger.info(f"Deleted S3 object: {path}")

    def get_size(self, path: str) -> int:
        s3 = self._s3_client()
        bucket, key = parse_s3_path(path)
        response = s3.head_object(Bucket=bucket, Key=key)
        return response["ContentLength"]

    def list_files(self, path: str) -> List[str]:
        s3 = self._s3_client()
        bucket, prefix = parse_s3_path(path)
        result = []
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj['Key']
                if not key.endswith("/"):
                    result.append(f"s3://{bucket}/{key}")
        return result

    def mkdir(self, path: str, exist_ok: bool = True) -> None:
        s3 = self._s3_client()
        bucket, key = parse_s3_path(path)
        prefix = key.rstrip('/') + "/"
        if not exist_ok:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
            if "Contents" in response:
                raise FileExistsError(f"S3 prefix already exists: {path}")
        s3.put_object(Bucket=bucket, Key=prefix)
        logger.info(f"Created S3 directory prefix: {path}")

    def is_dir(self, path: str) -> bool:
        s3 = self._s3_client()
        bucket, key = parse_s3_path(path)
        if not key or key.endswith("/"):
            return True
        prefix = key.rstrip('/') + "/"
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        return "Contents" in response

    def rename(self, old_path: str, new_path: str) -> None:
        s3 = self._s3_client()
        old_bucket, old_key = parse_s3_path(old_path)
        new_bucket, new_key = parse_s3_path(new_path)
        s3.copy_object(
            Bucket=new_bucket,
            CopySource={"Bucket": old_bucket, "Key": old_key},
            Key=new_key,
        )
        s3.delete_object(Bucket=old_bucket, Key=old_key)
        logger.info(f"Renamed S3 object: {old_path} -> {new_path}")

    def stat(self, path: str) -> Dict[str, Any]:
        s3 = self._s3_client()
        bucket, key = parse_s3_path(path)
        response = s3.head_object(Bucket=bucket, Key=key)
        return {
            "size": response["ContentLength"],
            "last_modified": response["LastModified"],  # S3はUTC aware datetime
            "content_type": response.get("ContentType"),
            "etag": response.get("ETag"),
            "storage_class": response.get("StorageClass", "STANDARD"),
        }

    def download_file(self, remote_path: str, local_path: str) -> None:
        s3 = self._s3_client()
        bucket, key = parse_s3_path(remote_path)
        s3.download_file(bucket, key, local_path)
        logger.info("Download from S3 complete.")

    def upload_file(self, local_path: str, remote_path: str) -> None:
        s3 = self._s3_client()
        bucket, key = parse_s3_path(remote_path)
        s3.upload_file(local_path, bucket, key)
        logger.info("Upload to S3 complete.")