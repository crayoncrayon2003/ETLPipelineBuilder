import paramiko
from pathlib import Path
from typing import Dict, Any, Optional
import pluggy

from core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class ScpExtractor:
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
                "output_path": {"type": "string", "title": "Output File/Directory Path"},
                "password": {"type": "string", "title": "Password (Optional)", "format": "password"},
                "key_filepath": {"type": "string", "title": "SSH Key File Path (Optional)"}
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
        output_path = params.get("output_path")

        if not all([host, user, remote_path, output_path]):
            raise ValueError("ScpExtractor requires 'host', 'user', 'remote_path', and 'output_path'.")
        if not password and not key_filepath:
            raise ValueError("ScpExtractor requires either 'password' or 'key_filepath'.")

        final_output_path = output_path
        if output_path.is_dir():
            filename = Path(remote_path).name
            final_output_path = output_path / filename

        final_output_path.parent.mkdir(parents=True, exist_ok=True)
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
                print(f"Downloading '{remote_path}' to '{final_output_path}'...")
                sftp.get(remote_path, str(final_output_path))
                print("File downloaded successfully.")
        except Exception as e:
            print(f"SCP operation failed: {e}")
            if final_output_path.exists(): final_output_path.unlink()
            raise
        finally:
            if ssh_client: ssh_client.close()

        container = DataContainer()
        container.add_file_path(final_output_path)
        container.metadata.update({'source_type': 'scp', 'remote_host': host, 'remote_path': remote_path})
        return container