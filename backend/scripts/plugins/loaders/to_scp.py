import os
import paramiko
import tempfile
from typing import Dict, Any, Optional
import pluggy

from core.data_container.container import DataContainer
from core.infrastructure import storage_adapter

from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

hookimpl = pluggy.HookimplMarker("etl_framework")

class ScpLoader:
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

    @hookimpl
    def execute_plugin(self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]) -> Optional[DataContainer]:
        input_path_str = str(params.get("input_path"))
        host = params.get("host")
        port = params.get("port", 22)
        user = params.get("user")
        password = params.get("password")
        key_filepath = params.get("key_filepath")
        remote_path_str = str(params.get("remote_path"))
        timeout = params.get("timeout", 30)

        if not all([input_path_str, host, user, remote_path_str]):
            raise ValueError("ScpLoader requires 'input_path', 'host', 'user', and 'remote_path'.")
        if not password and not key_filepath:
            raise ValueError("ScpLoader requires either 'password' or 'key_filepath'.")

        def basename(path: str) -> str:
            return os.path.basename(path.rstrip('/'))

        def dirname(path: str) -> str:
            return os.path.dirname(path.rstrip('/'))

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_local_path = os.path.join(temp_dir, basename(input_path_str))

            if input_path_str.startswith("s3://"):
                logger.info(f"Downloading '{input_path_str}' from S3 to temporary location using StorageAdapter...")
                try:
                    file_content_bytes = storage_adapter.read_bytes(input_path_str)
                    with open(temp_local_path, 'wb') as f:
                        f.write(file_content_bytes)
                    local_file_to_upload = temp_local_path
                except Exception as e:
                    raise IOError(f"Failed to download file from S3 using StorageAdapter: {e}") from e
            else:
                local_file_to_upload = input_path_str

            if not os.path.isfile(local_file_to_upload):
                raise FileNotFoundError(f"Input file could not be found or downloaded: {local_file_to_upload}")

            ssh_client = None
            try:
                ssh_client = paramiko.SSHClient()
                ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                logger.info(f"Connecting to {host}:{port} as user '{user}' for SCP upload...")
                ssh_client.connect(hostname=host, port=port, username=user, password=password, key_filename=key_filepath, timeout=timeout)

                with ssh_client.open_sftp() as sftp:
                    final_remote_path = remote_path_str
                    # remote_pathがディレクトリで終わっている、またはファイル名が空文字の場合はファイル名を結合
                    if remote_path_str.endswith('/') or not basename(remote_path_str):
                        final_remote_path = remote_path_str.rstrip('/') + '/' + basename(local_file_to_upload)

                    remote_dir = dirname(final_remote_path)
                    try:
                        sftp.stat(remote_dir)
                    except FileNotFoundError:
                        logger.error(f"Remote directory '{remote_dir}' not found, attempting to create...")
                        current_path = ""
                        for part in remote_dir.split('/'):
                            if not part:
                                continue
                            current_path = current_path + '/' + part if current_path else part
                            try:
                                sftp.stat(current_path)
                            except FileNotFoundError:
                                sftp.mkdir(current_path)
                        logger.error(f"Successfully created remote directory '{remote_dir}'.")

                    logger.info(f"Uploading '{basename(local_file_to_upload)}' to '{final_remote_path}'...")
                    sftp.put(local_file_to_upload, final_remote_path)
                logger.info("File uploaded successfully via SCP.")
            except Exception as e:
                logger.error(f"SCP upload failed: {e}")
                raise
            finally:
                if ssh_client:
                    ssh_client.close()
        return None
