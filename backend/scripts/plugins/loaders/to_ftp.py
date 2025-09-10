import os
import ftplib
from pathlib import Path
from typing import Dict, Any, Optional
import pluggy
import tempfile

from core.data_container.container import DataContainer
from core.infrastructure import storage_adapter

from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

hookimpl = pluggy.HookimplMarker("etl_framework")

class FtpLoader:
    """
    (Storage Aware) Loads (uploads) a file from local or S3 to an FTP server.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "to_ftp"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {"type": "string", "title": "Input File Path (local/s3)"},
                "host": {"type": "string", "title": "FTP Host"},
                "remote_dir": {"type": "string", "title": "Remote Directory", "default": "/"},
                "user": {"type": "string", "title": "Username"},
                "password": {"type": "string", "title": "Password", "format": "password"}
            },
            "required": ["input_path", "host"]
        }

    @hookimpl
    def execute_plugin(self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]) -> Optional[DataContainer]:
        input_path_str = str(params.get("input_path"))
        host = params.get("host")
        user = params.get("user")
        password = params.get("password")
        remote_dir = params.get("remote_dir", "/")

        if not input_path_str or not host:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'input_path' and 'host'.")

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_local_path = Path(temp_dir) / Path(input_path_str).name

            if input_path_str.startswith("s3://"):
                logger.info(f"Downloading '{input_path_str}' from S3 to temporary location using StorageAdapter...")
                try:
                    file_content_bytes = storage_adapter.read_bytes(input_path_str)
                    temp_local_path.write_bytes(file_content_bytes)
                    local_file_to_upload = temp_local_path
                except Exception as e:
                    raise IOError(f"Failed to download file from S3 using StorageAdapter: {e}") from e
            else:
                local_file_to_upload = Path(input_path_str)

            if not local_file_to_upload.exists():
                raise FileNotFoundError(f"Input file could not be found or downloaded: {local_file_to_upload}")

            remote_filename = local_file_to_upload.name
            logger.info(f"Connecting to FTP at {host} to upload '{remote_filename}'...")

            try:
                with ftplib.FTP(host, timeout=60) as ftp:
                    ftp.login(user=user, passwd=password)
                    if remote_dir != '/':
                        try:
                            ftp.cwd(remote_dir)
                        except ftplib.error_perm:
                            logger.error(f"Remote directory '{remote_dir}' not found, attempting to create...")
                            for part in Path(remote_dir).parts:
                                if part:
                                    try:
                                        ftp.mkd(part)
                                    except ftplib.error_perm:
                                        pass
                                    ftp.cwd(part)
                            logger.info(f"Successfully navigated/created to '{remote_dir}'.")

                    logger.info(f"Uploading '{local_file_to_upload.name}' to '{remote_dir}/{remote_filename}'...")
                    with open(local_file_to_upload, 'rb') as local_file:
                        ftp.storbinary(f'STOR {remote_filename}', local_file)
                logger.info(f"File '{remote_filename}' uploaded successfully to FTP.")
            except ftplib.all_errors as e:
                logger.error(f"FTP upload failed: {e}")
                raise
        return None