import yaml
from pathlib import Path
from typing import Dict, Any, Union

# def load_config_from_path(config_path: str | Path) -> Dict[str, Any]:
def load_config_from_path(config_path: Union[str, Path]) -> Dict[str, Any]:
    """
    Loads a YAML configuration file from a given path.

    This is a simple, stateless utility function for reading a YAML file
    and returning its contents as a dictionary.

    Args:
        config_path (str | Path): The path to the YAML configuration file.

    Returns:
        Dict[str, Any]: The contents of the YAML file as a dictionary.

    Raises:
        FileNotFoundError: If the configuration file does not exist.
        yaml.YAMLError: If the file contains invalid YAML.
    """
    path = Path(config_path)

    if not path.is_file():
        raise FileNotFoundError(f"Configuration file not found at: {path}")

    try:
        with path.open('r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"Error parsing YAML file {path}: {e}")

    return config or {}