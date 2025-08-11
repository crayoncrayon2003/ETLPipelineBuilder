# backend/core/config/loader.py

import yaml
from pathlib import Path
from typing import Dict, Any, Optional

class ConfigLoader:
    """
    Loads configuration settings from various sources, primarily YAML files.

    This loader is intended for framework-level or shared configurations,
    such as database credentials, API keys, or service endpoints, which
    are distinct from the pipeline-specific step definitions handled by
    `PipelineParser`.
    """
    def __init__(self, config_path: Optional[str | Path] = None):
        """
        Initializes the ConfigLoader.

        Args:
            config_path (Optional[str | Path]): The path to the main
                configuration file to be loaded.
        """
        self.config_path = Path(config_path) if config_path else None
        self._config: Dict[str, Any] = {}

    def load(self) -> Dict[str, Any]:
        """
        Loads the configuration from the specified file path.

        If a path was provided during initialization, this method reads
        and parses the YAML file.

        Returns:
            Dict[str, Any]: The loaded configuration as a dictionary.

        Raises:
            FileNotFoundError: If the config file does not exist.
            yaml.YAMLError: If there is an error parsing the YAML file.
        """
        if not self.config_path:
            print("Warning: ConfigLoader initialized without a path. No config loaded.")
            return self._config

        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")

        try:
            with self.config_path.open('r', encoding='utf-8') as f:
                self._config = yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Error parsing configuration file {self.config_path}: {e}")

        return self._config

    def get(self, key: str, default: Any = None) -> Any:
        """
        Retrieves a configuration value for a given key.

        This method supports nested keys using dot notation (e.g., 'database.host').

        Args:
            key (str): The key of the value to retrieve.
            default (Any, optional): The default value to return if the key
                                     is not found. Defaults to None.

        Returns:
            Any: The configuration value, or the default if not found.
        """
        if not self._config:
            return default

        keys = key.split('.')
        value = self._config
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default