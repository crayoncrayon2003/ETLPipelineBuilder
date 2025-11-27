import os
import paramiko
import tempfile
from typing import Dict, Any
import pluggy

from core.data_container.container import DataContainer
from core.infrastructure import storage_adapter
from core.plugin_manager.base_plugin import BasePlugin

from utils.logger import setup_logger

logger = setup_logger(__name__)

hookimpl = pluggy.HookimplMarker("etl_framework")

class ScpLoader(BasePlugin):
    """
    (Storage Aware) Loads (uploads) a file from local or S3 to a remote server using SCP.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "to_scp"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {"type": "string", "title": "Input File Path (local/s3)"},
                "host": {"type": "string", "title": "SSH Host"},
                "port": {"type": "integer", "title": "SSH Port", "default": 22},
                "user": {"type": "string", "title": "SSH Username"},
                "remote_path": {"type": "string", "title": "Remote Path"},
                "password": {"type": "string", "title": "Password (Optional)", "format": "password"},
                "key_filepath": {"type": "string", "title": "SSH Key File Path (Optional)"},
                "timeout": {"type": "integer", "title": "Connection Timeout in seconds", "default": 30}
            },
            "required": ["input_path", "host", "user", "remote_path"]
        }

    def run(self, input_data: DataContainer, container: DataContainer) -> DataContainer:
        input_path_str = str(self.params.get("input_path"))
        host = self.params.get("host")
        port = self.params.get("port", 22)
        user = self.params.get("user")
        password = self.params.get("password")
        key_filepath = self.params.get("key_filepath")
        remote_path_str = str(self.params.get("remote_path"))
        timeout = self.params.get("timeout", 30)

        if not all([input_path_str, host, user, remote_path_str]):
            raise ValueError("Missing required parameters: 'input_path', 'host', 'user', 'remote_path'.")
        if not password and not key_filepath:
            raise ValueError("Either 'password' or 'key_filepath' must be provided.")

        def basename(path: str) -> str:
            return os.path.basename(path.rstrip('/'))

        def dirname(path: str) -> str:
            return os.path.dirname(path.rstrip('/'))

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_local_path = os.path.join(temp_dir, basename(input_path_str))

            try:
                if input_path_str.startswith("s3://"):
                    logger.info(f"[{self.get_plugin_name()}] Downloading '{input_path_str}' from S3...")
                    file_content_bytes = storage_adapter.read_bytes(input_path_str)
                    with open(temp_local_path, 'wb') as f:
                        f.write(file_content_bytes)
                    local_file_to_upload = temp_local_path
                else:
                    local_file_to_upload = input_path_str

                if not os.path.isfile(local_file_to_upload):
                    raise FileNotFoundError(f"File not found: {local_file_to_upload}")
            except Exception as e:
                raise RuntimeError(f"Failed to prepare file for upload: {str(e)}")

            ssh_client = None
            try:
                ssh_client = paramiko.SSHClient()
                ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                logger.info(f"[{self.get_plugin_name()}] Connecting to {host}:{port} as user '{user}'...")
                ssh_client.connect(
                    hostname=host, port=port, username=user,
                    password=password, key_filename=key_filepath, timeout=timeout
                )

                with ssh_client.open_sftp() as sftp:
                    final_remote_path = remote_path_str
                    if remote_path_str.endswith('/') or not basename(remote_path_str):
                        final_remote_path = remote_path_str.rstrip('/') + '/' + basename(local_file_to_upload)

                    remote_dir = dirname(final_remote_path)
                    try:
                        sftp.stat(remote_dir)
                    except FileNotFoundError:
                        logger.info(f"[{self.get_plugin_name()}] Creating remote directory '{remote_dir}'...")
                        current_path = ""
                        for part in remote_dir.split('/'):
                            if not part:
                                continue
                            current_path = current_path + '/' + part if current_path else part
                            try:
                                sftp.stat(current_path)
                            except FileNotFoundError:
                                sftp.mkdir(current_path)

                    logger.info(f"[{self.get_plugin_name()}] Uploading to '{final_remote_path}'...")
                    sftp.put(local_file_to_upload, final_remote_path)
            except Exception as e:
                raise RuntimeError(f"SCP upload failed: {str(e)}")
            finally:
                if ssh_client:
                    ssh_client.close()

        return self.finalize_container(
            container,
            metadata={
                "input_path": input_path_str,
                "remote_host": host,
                "remote_path": remote_path_str,
                "uploaded_filename": basename(local_file_to_upload)
            }
        )
