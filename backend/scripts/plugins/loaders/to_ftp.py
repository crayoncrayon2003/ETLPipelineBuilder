import os
import ftplib
import tempfile
from typing import Dict, Any
import pluggy

from core.data_container.container import DataContainer, DataContainerStatus
from core.infrastructure import storage_adapter
from core.plugin_manager.base_plugin import BasePlugin

from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

hookimpl = pluggy.HookimplMarker("etl_framework")

class FtpLoader(BasePlugin):
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
    def execute(self, input_data: DataContainer) -> DataContainer:
        input_path_str = str(self.params.get("input_path"))
        host = self.params.get("host")
        user = self.params.get("user")
        password = self.params.get("password")
        remote_dir = self.params.get("remote_dir", "/")

        container = DataContainer()

        if not input_path_str or not host:
            container.set_status(DataContainerStatus.ERROR)
            container.add_error("Missing required parameters: 'input_path' and 'host'.")
            return container

        def basename(path: str) -> str:
            return os.path.basename(path.rstrip('/'))

        def split_path_parts(path: str):
            return [part for part in path.strip('/').split('/') if part]

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_local_path = os.path.join(temp_dir, basename(input_path_str))

            try:
                if input_path_str.startswith("s3://"):
                    logger.info(f"Downloading '{input_path_str}' from S3...")
                    file_content_bytes = storage_adapter.read_bytes(input_path_str)
                    with open(temp_local_path, 'wb') as f:
                        f.write(file_content_bytes)
                    local_file_to_upload = temp_local_path
                else:
                    local_file_to_upload = input_path_str

                if not os.path.isfile(local_file_to_upload):
                    raise FileNotFoundError(f"File not found: {local_file_to_upload}")
            except Exception as e:
                container.set_status(DataContainerStatus.ERROR)
                container.add_error(f"Failed to prepare file for upload: {str(e)}")
                return container

            remote_filename = basename(local_file_to_upload)

            try:
                with ftplib.FTP(host, timeout=60) as ftp:
                    ftp.login(user=user, passwd=password)
                    if remote_dir != '/':
                        try:
                            ftp.cwd(remote_dir)
                        except ftplib.error_perm:
                            logger.warning(f"Remote directory '{remote_dir}' not found. Attempting to create...")
                            for part in split_path_parts(remote_dir):
                                try:
                                    ftp.mkd(part)
                                except ftplib.error_perm:
                                    pass
                                ftp.cwd(part)
                            logger.info(f"Navigated to '{remote_dir}'.")

                    logger.info(f"Uploading '{remote_filename}' to FTP...")
                    with open(local_file_to_upload, 'rb') as local_file:
                        ftp.storbinary(f'STOR {remote_filename}', local_file)
                    logger.info(f"Upload successful.")
            except ftplib.all_errors as e:
                container.set_status(DataContainerStatus.ERROR)
                container.add_error(f"FTP upload failed: {str(e)}")
                return container

        container.set_status(DataContainerStatus.SUCCESS)
        container.metadata.update({
            "input_path": input_path_str,
            "ftp_host": host,
            "remote_dir": remote_dir,
            "uploaded_filename": remote_filename
        })
        container.add_history(self.get_plugin_name())
        return container
