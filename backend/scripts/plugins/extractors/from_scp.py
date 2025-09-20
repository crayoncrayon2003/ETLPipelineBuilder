import os
import paramiko
import tempfile
from typing import Dict, Any
import pluggy

from core.infrastructure import storage_adapter
from core.data_container.container import DataContainer
from core.plugin_manager.base_plugin import BasePlugin
from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

hookimpl = pluggy.HookimplMarker("etl_framework")

class ScpExtractor(BasePlugin):
    """
    (Storage Aware) Downloads a file via SCP. If the output path is S3,
    it downloads to a temporary local file first, then uploads to S3
    using the StorageAdapter.
    """

    @hookimpl
    def get_plugin_name(self) -> str:
        return "from_scp"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "host": {"type": "string", "title": "SSH Host"},
                "user": {"type": "string", "title": "SSH Username"},
                "remote_path": {"type": "string", "title": "Remote File Path"},
                "output_path": {"type": "string", "title": "Output Path (local or s3://)"},
                "password": {"type": "string", "title": "Password", "format": "password"},
                "key_filepath": {"type": "string", "title": "SSH Key File Path"}
            },
            "required": ["host", "user", "remote_path", "output_path"]
        }

    def run(self, input_data: DataContainer, container: DataContainer) -> DataContainer:
        host = self.params.get("host")
        port = self.params.get("port", 22)
        user = self.params.get("user")
        password = self.params.get("password")
        key_filepath = self.params.get("key_filepath")
        remote_path = self.params.get("remote_path")
        output_path_str = str(self.params.get("output_path"))

        if not all([host, user, remote_path, output_path_str]):
            raise ValueError("Missing required parameters: 'host', 'user', 'remote_path', 'output_path'.")
        if not password and not key_filepath:
            raise ValueError("Either 'password' or 'key_filepath' must be provided.")

        def basename(path: str) -> str:
            return os.path.basename(path.rstrip('/'))

        with tempfile.TemporaryDirectory() as temp_dir:
            local_temp_path = os.path.join(temp_dir, basename(remote_path))

            ssh_client = None
            try:
                ssh_client = paramiko.SSHClient()
                ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                logger.info(f"[{self.get_plugin_name()}] Connecting to {host} as user '{user}' for SCP download...")
                ssh_client.connect(
                    hostname=host, port=port, username=user,
                    password=password, key_filename=key_filepath, timeout=30
                )
                with ssh_client.open_sftp() as sftp:
                    logger.info(f"[{self.get_plugin_name()}] Downloading '{remote_path}' to temporary location...")
                    sftp.get(remote_path, local_temp_path)
                logger.info(f"[{self.get_plugin_name()}] Successfully downloaded to {local_temp_path}")
            except Exception as e:
                raise RuntimeError(f"SCP download operation failed: {e}")
            finally:
                if ssh_client:
                    ssh_client.close()

            try:
                storage_adapter.upload_local_file(local_temp_path, output_path_str)
            except Exception as e:
                raise RuntimeError(f"Storage upload failed: {e}")

        return self.finalize_container(
            container,
            output_path=output_path_str,
            metadata={
                'source_type': 'scp',
                'remote_host': host,
                'remote_path': remote_path
            }
        )
