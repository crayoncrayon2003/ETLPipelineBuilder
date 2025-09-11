import os
import yaml
from typing import Dict, Any, Optional, Union

from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

class ConfigLoader:
    """
    Loads configuration settings from various sources, primarily YAML files.
    """

    def __init__(self, config_path: Optional[Union[str, os.PathLike]] = None):
        """
        Initializes the ConfigLoader.

        Args:
            config_path (Optional[str | os.PathLike]): Path to the YAML config file.
        """
        self.config_path = os.path.abspath(config_path) if config_path else None
        self._config: Dict[str, Any] = {}

    def load(self) -> Dict[str, Any]:
        """
        Loads the configuration from the specified YAML file.

        Returns:
            Dict[str, Any]: Loaded configuration dictionary.

        Raises:
            FileNotFoundError: If file does not exist.
            yaml.YAMLError: If parsing fails.
        """
        if not self.config_path:
            logger.info("Warning: ConfigLoader initialized without a path. No config loaded.")
            return self._config

        if not os.path.isfile(self.config_path):
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")

        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self._config = yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Error parsing configuration file {self.config_path}: {e}")

        return self._config

    def get(self, key: str, default: Any = None) -> Any:
        """
        Retrieves a configuration value by key (supports nested via dot notation).

        Args:
            key (str): Key string (e.g., "database.host").
            default (Any): Fallback value if key not found.

        Returns:
            Any: Retrieved value or default.
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
