import paramiko
from pathlib import Path
from typing import Dict, Any, Optional
import pluggy
import tempfile

from core.infrastructure import storage_adapter
from core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class ScpExtractor:
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
                "host": {
                    "type": "string",
                    "title": "SSH Host",
                    "description": "Hostname or IP address of the remote server."
                },
                "user": {
                    "type": "string",
                    "title": "SSH Username"
                },
                "remote_path": {
                    "type": "string",
                    "title": "Remote File Path",
                    "description": "The full path to the file on the remote server."
                },
                "output_path": {
                    "type": "string",
                    "title": "Output Path (local or s3://)",
                    "description": "The final destination for the downloaded file."
                },
                "password": {
                    "type": "string",
                    "title": "Password (Optional)",
                    "description": "Password for authentication. Use of SSH key is recommended.",
                    "format": "password"
                },
                "key_filepath": {
                    "type": "string",
                    "title": "SSH Key File Path (Optional)",
                    "description": "Local path to the private SSH key for authentication."
                }
            },
            "required": ["host", "user", "remote_path", "output_path"]
        }

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        host = params.get("host")
        port = params.get("port", 22)
        user = params.get("user")
        password = params.get("password")
        key_filepath = params.get("key_filepath")
        remote_path = params.get("remote_path")
        output_path_str = str(params.get("output_path"))

        if not all([host, user, remote_path, output_path_str]):
            raise ValueError("ScpExtractor requires 'host', 'user', 'remote_path', and 'output_path'.")
        if not password and not key_filepath:
            raise ValueError("ScpExtractor requires either 'password' or 'key_filepath'.")

        # Use a temporary directory to download the file from SCP
        with tempfile.TemporaryDirectory() as temp_dir:
            local_temp_path = Path(temp_dir) / Path(remote_path).name

            # Download the file from the SCP server to the temporary local path
            ssh_client = None
            try:
                ssh_client = paramiko.SSHClient()
                ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                print(f"Connecting to {host} as user '{user}' for SCP download...")
                ssh_client.connect(
                    hostname=host, port=port, username=user,
                    password=password, key_filename=key_filepath, timeout=30
                )
                with ssh_client.open_sftp() as sftp:
                    print(f"Downloading '{remote_path}' to temporary location...")
                    sftp.get(remote_path, str(local_temp_path))
                print(f"Successfully downloaded to {local_temp_path}")
            except Exception as e:
                print(f"SCP download operation failed: {e}")
                raise
            finally:
                if ssh_client:
                    ssh_client.close()

            # Use the StorageAdapter to move the temporary file to its final destination
            storage_adapter.upload_local_file(local_temp_path, output_path_str)

        # The pipeline continues with the pointer to the final destination path.
        container = DataContainer()
        container.add_file_path(output_path_str)
        container.metadata.update({
            'source_type': 'scp',
            'remote_host': host,
            'remote_path': remote_path
        })
        return container