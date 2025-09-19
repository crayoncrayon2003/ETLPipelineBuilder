import os
import ftplib
import tempfile
from typing import Dict, Any
import pluggy

from core.infrastructure import storage_adapter
from core.data_container.container import DataContainer, DataContainerStatus
from core.plugin_manager.base_plugin import BasePlugin

from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

hookimpl = pluggy.HookimplMarker("etl_framework")

class FtpExtractor(BasePlugin):
    """
    (Storage Aware) Downloads a file from an FTP server.
    It downloads to a temporary local file first, then uses the StorageAdapter
    to move it to the final destination (local or S3).
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "from_ftp"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "host": {
                    "type": "string",
                    "title": "FTP Host",
                    "description": "Hostname or IP address of the FTP server."
                },
                "remote_path": {
                    "type": "string",
                    "title": "Remote File Path",
                    "description": "The full path to the file on the FTP server."
                },
                "output_path": {
                    "type": "string",
                    "title": "Output Path (local or s3://)",
                    "description": "The final destination for the downloaded file."
                },
                "user": {
                    "type": "string",
                    "title": "Username",
                    "description": "(Optional) Username for FTP authentication."
                },
                "password": {
                    "type": "string",
                    "title": "Password",
                    "description": "(Optional) Password for FTP authentication.",
                    "format": "password"
                }
            },
            "required": ["host", "remote_path", "output_path"]
        }

    @hookimpl
    def execute(self, input_data: DataContainer) -> DataContainer:
        host = self.params.get("host")
        user = self.params.get("user")
        password = self.params.get("password")
        remote_path = self.params.get("remote_path")
        output_path_str = str(self.params.get("output_path"))

        container = DataContainer()

        if not all([host, remote_path, output_path_str]):
            container.set_status(DataContainerStatus.ERROR)
            container.add_error("Missing required FTP parameters.")
            return container

        filename = os.path.basename(remote_path)

        with tempfile.TemporaryDirectory() as temp_dir:
            local_temp_path = os.path.join(temp_dir, filename)

            logger.info(f"Connecting to FTP at {host} to download to temporary storage...")
            try:
                with ftplib.FTP(host, timeout=60) as ftp:
                    ftp.login(user=user, passwd=password)
                    with open(local_temp_path, 'wb') as f:
                        ftp.retrbinary(f'RETR {remote_path}', f.write)
                logger.info(f"Successfully downloaded to temporary location: {local_temp_path}")
            except ftplib.all_errors as e:
                logger.error(f"FTP download operation failed: {e}")
                container.set_status(DataContainerStatus.ERROR)
                container.add_error(str(e))
                return container

            # Upload to final destination
            try:
                storage_adapter.upload_local_file(local_temp_path, output_path_str)
            except Exception as e:
                logger.error(f"Storage upload failed: {e}")
                container.set_status(DataContainerStatus.ERROR)
                container.add_error(str(e))
                return container

        container.set_status(DataContainerStatus.SUCCESS)
        container.add_file_path(output_path_str)
        container.metadata.update({
            'source_type': 'ftp',
            'ftp_host': host,
            'remote_path': remote_path
        })
        container.add_history(self.get_plugin_name())
        return container
