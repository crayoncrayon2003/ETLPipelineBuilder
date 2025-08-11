# backend/plugins/loaders/to_ftp.py

import ftplib
from pathlib import Path
from typing import Dict, Any, Optional
import pluggy

from core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class FtpLoader:
    """
    (File-based) Loads (uploads) a local file to an FTP server.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "to_ftp"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {"type": "string", "title": "Input File Path"},
                "host": {"type": "string", "title": "FTP Host"},
                "remote_dir": {"type": "string", "title": "Remote Directory", "default": "/"},
                "user": {"type": "string", "title": "Username"},
                "password": {"type": "string", "title": "Password", "format": "password"}
            },
            "required": ["input_path", "host"]
        }

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        input_path = Path(params.get("input_path"))
        host = params.get("host")
        user = params.get("user")
        password = params.get("password")
        remote_dir = params.get("remote_dir", "/")

        if not input_path or not host:
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'input_path' and 'host'.")
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found at: {input_path}")

        remote_filename = input_path.name
        print(f"Connecting to FTP at {host} to upload '{input_path.name}'...")

        try:
            with ftplib.FTP(host, timeout=60) as ftp:
                ftp.login(user=user, passwd=password)
                if remote_dir != '/': ftp.cwd(remote_dir)
                print(f"Uploading to '{remote_dir}/{remote_filename}'...")
                with open(input_path, 'rb') as local_file:
                    ftp.storbinary(f'STOR {remote_filename}', local_file)
            print("File uploaded successfully.")
        except ftplib.all_errors as e:
            print(f"FTP upload failed: {e}")
            raise
        return None