import requests
from pathlib import Path
from typing import Dict, Any, Optional
import pluggy
from urllib.parse import urlparse

from core.data_container.container import DataContainer

hookimpl = pluggy.HookimplMarker("etl_framework")

class HttpBasicAuthExtractor:
    """
    Downloads a file from an HTTP(S) source that requires Basic Authentication.
    """
    @hookimpl
    def get_plugin_name(self) -> str:
        return "from_http_with_basic_auth"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "title": "Source URL",
                    "description": "The URL to download the data file from."
                },
                "output_path": {
                    "type": "string",
                    "title": "Output File/Directory Path",
                    "description": "The local path to save the downloaded file."
                },
                "username": {
                    "type": "string",
                    "title": "Username",
                    "description": "The username for Basic Authentication. Can be a secret reference."
                },
                "password": {
                    "type": "string",
                    "title": "Password",
                    "description": "The password for Basic Authentication. Should be a secret reference.",
                    "format": "password" # This tells RJSF to use a password input
                }
            },
            "required": ["url", "output_path", "username", "password"]
        }

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:
        url = params.get("url")
        output_path = params.get("output_path") # Assumes this is a Path object
        username = params.get("username")
        password = params.get("password")

        if not all([url, output_path, username, password]):
            raise ValueError(f"Plugin '{self.get_plugin_name()}' requires 'url', 'output_path', 'username', and 'password'.")

        final_output_path = output_path
        if output_path.is_dir():
            parsed_url = urlparse(url)
            filename = Path(parsed_url.path).name
            if not filename:
                raise ValueError("Could not infer filename from URL.")
            final_output_path = output_path / filename

        print(f"Downloading from '{url}' to '{final_output_path}' using Basic Auth...")
        try:
            # Use the `auth` parameter of requests with a tuple (username, password)
            response = requests.get(
                url,
                auth=(username, password),
                stream=True,
                timeout=60
            )
            response.raise_for_status()

            final_output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(final_output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print("File downloaded successfully.")
        except requests.RequestException as e:
            print(f"HTTP request with Basic Auth failed: {e}")
            raise

        container = DataContainer()
        container.add_file_path(final_output_path)
        container.metadata['source_url'] = url
        return container