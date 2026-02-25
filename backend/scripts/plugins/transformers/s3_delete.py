import re
from typing import Dict, Any, List

import boto3
from botocore.exceptions import ClientError

from core.data_container.container import DataContainer
from core.plugin_manager.base_plugin import BasePlugin

import pluggy
from utils.logger import setup_logger

logger = setup_logger(__name__)

hookimpl = pluggy.HookimplMarker("etl_framework")


def _parse_s3_path(s3_path: str):
    """
    Splits 's3://bucket-name/path/to/key' into (bucket, key_prefix).
    Returns an empty string for key_prefix when pointing to the bucket root.
    """
    match = re.match(r"^s3://([^/]+)(?:/(.*))?$", s3_path)
    if not match:
        raise ValueError(f"Invalid S3 path format: '{s3_path}'. Expected 's3://bucket/...' .")
    bucket = match.group(1)
    prefix = match.group(2) or ""
    return bucket, prefix


def _is_folder_path(prefix: str) -> bool:
    """
    Returns True if the prefix ends with '/' or is empty (bucket root).
    """
    return prefix == "" or prefix.endswith("/")


class S3DeletePlugin(BasePlugin):
    """
    Plugin for deleting objects from S3.

    Accepts a string or array of strings in s3_paths.
    - Folder path (trailing /): deletes all objects under the prefix.
    - Object path: deletes the single specified object.
    - Both types can be mixed in the same array.
    """

    @hookimpl
    def get_plugin_name(self) -> str:
        return "s3_delete"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "s3_paths": {
                    "oneOf": [
                        {"type": "string"},
                        {"type": "array", "items": {"type": "string"}, "minItems": 1},
                    ],
                    "title": "S3 Path(s) to delete",
                    "description": (
                        "Target S3 path(s). Accepts a string or an array of strings.\n"
                        "  Folder: s3://bucket/prefix/  -> deletes all objects under prefix\n"
                        "  File:   s3://bucket/path/to/file.csv -> deletes single object\n"
                        "  Array example: [\"s3://bucket/folder/\", \"s3://bucket/file.csv\"]"
                    ),
                },
            },
            "required": ["s3_paths"],
        }

    # ------------------------------------------------------------------
    # 内部ヘルパー
    # ------------------------------------------------------------------

    def _build_s3_client(self):
        return boto3.client("s3")

    def _list_objects(self, s3_client, bucket: str, prefix: str):
        """
        Lists all object keys under the given prefix with pagination support.
        """
        keys = []
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys

    def _bulk_delete(self, s3_client, bucket: str, keys):
        """
        Deletes objects in bulk using the S3 delete_objects API (up to 1000 keys per request).
        Processes all chunks before returning errors.
        """
        errors = []
        # Process in chunks of 1000
        chunk_size = 1000
        for i in range(0, len(keys), chunk_size):
            chunk = keys[i: i + chunk_size]
            delete_payload = {"Objects": [{"Key": k} for k in chunk], "Quiet": True}
            response = s3_client.delete_objects(Bucket=bucket, Delete=delete_payload)
            for err in response.get("Errors", []):
                errors.append(err)
                logger.error(
                    f"[{self.get_plugin_name()}] Failed to delete '{err['Key']}': "
                    f"{err['Code']} - {err['Message']}"
                )
        return errors

    # ------------------------------------------------------------------
    # 単一パスの処理（フォルダ or ファイル）
    # ------------------------------------------------------------------

    def _process_folder(self, s3_client, bucket: str, prefix: str) -> Dict[str, Any]:
        """Deletes all objects under the given prefix and returns the result."""
        s3_path = f"s3://{bucket}/{prefix}"
        logger.info(f"[{self.get_plugin_name()}] Listing objects under {s3_path} ...")
        keys = self._list_objects(s3_client, bucket, prefix)

        if not keys:
            logger.info(f"[{self.get_plugin_name()}] No objects found under {s3_path}. Skipping.")
            return {"s3_path": s3_path, "mode": "folder", "objects_found": 0, "objects_deleted": 0}

        logger.info(f"[{self.get_plugin_name()}] Found {len(keys)} object(s) under {s3_path}.")

        errors = self._bulk_delete(s3_client, bucket, keys)
        if errors:
            raise RuntimeError(
                f"{len(errors)} object(s) failed to delete under {s3_path}. "
                f"First error: {errors[0]}"
            )
        deleted_count = len(keys)
        logger.info(f"[{self.get_plugin_name()}] Deleted {deleted_count} object(s) from {s3_path}.")

        return {"s3_path": s3_path, "mode": "folder", "objects_found": len(keys), "objects_deleted": deleted_count}

    def _process_single_object(self, s3_client, bucket: str, key: str) -> Dict[str, Any]:
        """Deletes a single object and returns the result."""
        s3_path = f"s3://{bucket}/{key}"
        logger.info(f"[{self.get_plugin_name()}] Checking existence: {s3_path} ...")

        # Check existence
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ("404", "NoSuchKey"):
                # Object does not exist - treat as success
                logger.info(f"[{self.get_plugin_name()}] Object not found, skipping: {s3_path}")
                return {"s3_path": s3_path, "mode": "single_object", "objects_found": 0, "objects_deleted": 0}
            raise RuntimeError(f"Failed to check object existence for {s3_path}: {e}") from e

        # Delete object - raise immediately on failure to stop further processing
        try:
            s3_client.delete_object(Bucket=bucket, Key=key)
            logger.info(f"[{self.get_plugin_name()}] Deleted: {s3_path}")
        except ClientError as e:
            raise RuntimeError(f"Failed to delete object {s3_path}: {e}") from e

        return {"s3_path": s3_path, "mode": "single_object", "objects_found": 1, "objects_deleted": 1}

    # ------------------------------------------------------------------
    # BasePlugin.run 実装
    # ------------------------------------------------------------------

    def run(self, input_data: DataContainer, container: DataContainer) -> DataContainer:
        raw_paths = self.params.get("s3_paths")

        if not raw_paths:
            raise ValueError("Missing required parameter: 's3_paths'.")

        # Normalize to list if a single string is given
        s3_paths: List[str] = [raw_paths] if isinstance(raw_paths, str) else list(raw_paths)

        if not s3_paths:
            raise ValueError("'s3_paths' must contain at least one path.")

        s3_client = self._build_s3_client()
        results = []
        total_found = 0
        total_deleted = 0

        for s3_path in s3_paths:
            s3_path = s3_path.strip()
            bucket, prefix = _parse_s3_path(s3_path)

            if _is_folder_path(prefix):
                result = self._process_folder(s3_client, bucket, prefix)
            else:
                result = self._process_single_object(s3_client, bucket, prefix)

            results.append(result)
            total_found += result["objects_found"]
            total_deleted += result["objects_deleted"]

        logger.info(
            f"[{self.get_plugin_name()}] All done. "
            f"paths={len(s3_paths)}, found={total_found}, deleted={total_deleted}"
        )

        return self.finalize_container(
            container,
            metadata={
                "s3_paths": s3_paths,
                "total_objects_found": total_found,
                "total_objects_deleted": total_deleted,
                "details": results,
            },
        )