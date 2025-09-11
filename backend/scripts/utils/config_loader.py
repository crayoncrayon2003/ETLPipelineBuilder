import os
import yaml
from typing import Dict, Any

def load_config_from_path(config_path: str) -> Dict[str, Any]:
    """
    Loads a YAML configuration file from a given path.

    Args:
        config_path (str): The path to the YAML configuration file.

    Returns:
        Dict[str, Any]: The contents of the YAML file as a dictionary.

    Raises:
        FileNotFoundError: If the configuration file does not exist.
        yaml.YAMLError: If the file contains invalid YAML.
    """
    abs_path = os.path.abspath(config_path)

    if not os.path.isfile(abs_path):
        raise FileNotFoundError(f"Configuration file not found at: {abs_path}")

    try:
        with open(abs_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"Error parsing YAML file {abs_path}: {e}")

    return config or {}
