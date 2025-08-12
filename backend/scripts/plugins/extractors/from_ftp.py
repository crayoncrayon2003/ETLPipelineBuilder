import ftplib
from pathlib import Path
from typing import Dict, Any, Optional
import pluggy

from core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class FtpExtractor:
    @hookimpl
    def get_plugin_name(self) -> str:
        return "from_ftp"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "host": {"type": "string", "title": "FTP Host"},
                "remote_path": {"type": "string", "title": "Remote File Path"},
                "output_path": {"type": "string", "title": "Output File/Directory Path"},
                "user": {"type": "string", "title": "Username"},
                "password": {"type": "string", "title": "Password", "format": "password"}
            },
            "required": ["host", "remote_path", "output_path"]
        }

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        host = params.get("host")
        user = params.get("user")
        password = params.get("password")
        remote_path = params.get("remote_path")
        output_path = params.get("output_path")

        if not all([host, remote_path, output_path]):
            raise ValueError("FtpExtractor requires 'host', 'remote_path', and 'output_path'.")

        final_output_path = output_path
        if output_path.is_dir():
            filename = Path(remote_path).name
            final_output_path = output_path / filename

        final_output_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"Connecting to FTP server at {host}...")
        try:
            with ftplib.FTP(host) as ftp:
                ftp.login(user=user, passwd=password)
                print(f"Downloading '{remote_path}' to '{final_output_path}'...")
                with open(final_output_path, 'wb') as local_file:
                    ftp.retrbinary(f'RETR {remote_path}', local_file.write)
                print("File downloaded successfully.")
        except ftplib.all_errors as e:
            print(f"FTP operation failed: {e}")
            if final_output_path.exists(): final_output_path.unlink()
            raise

        container = DataContainer()
        container.add_file_path(final_output_path)
        container.metadata.update({'source_type': 'ftp', 'ftp_host': host, 'remote_path': remote_path})
        return container