# backend/plugins/loaders/to_scp.py

import paramiko
from pathlib import Path
from typing import Dict, Any, Optional
import pluggy

from core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class ScpLoader:
    """
    (File-based) Loads (uploads) a local file to a remote server using SCP.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "to_scp"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {"type": "string", "title": "Input File Path"},
                "host": {"type": "string", "title": "SSH Host"},
                "user": {"type": "string", "title": "SSH Username"},
                "remote_path": {"type": "string", "title": "Remote Path"},
                "password": {"type": "string", "title": "Password (Optional)", "format": "password"},
                "key_filepath": {"type": "string", "title": "SSH Key File Path (Optional)"}
            },
            "required": ["input_path", "host", "user", "remote_path"]
        }

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = Path(params.get("input_path"))
        host = params.get("host")
        port = params.get("port", 22)
        user = params.get("user")
        password = params.get("password")
        key_filepath = params.get("key_filepath")
        remote_path_str = params.get("remote_path")

        if not all([input_path, host, user, remote_path_str]):
            raise ValueError("ScpLoader requires 'input_path', 'host', 'user', and 'remote_path'.")
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found at: {input_path}")
        if not password and not key_filepath:
            raise ValueError("ScpLoader requires either 'password' or 'key_filepath'.")

        ssh_client = None
        try:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            print(f"Connecting to {host} as user '{user}' for SCP upload...")
            ssh_client.connect(
                hostname=host, port=port, username=user,
                password=password, key_filename=key_filepath, timeout=30
            )

            with ssh_client.open_sftp() as sftp:
                if remote_path_str.endswith('/'):
                    final_remote_path = remote_path_str + input_path.name
                else: final_remote_path = remote_path_str
                remote_dir = str(Path(final_remote_path).parent)
                try: sftp.stat(remote_dir)
                except FileNotFoundError: sftp.mkdir(remote_dir)

                print(f"Uploading '{input_path.name}' to '{final_remote_path}'...")
                sftp.put(str(input_path), final_remote_path)
            print("File uploaded successfully.")
        except Exception as e:
            print(f"SCP upload failed: {e}")
            raise
        finally:
            if ssh_client: ssh_client.close()
        return None