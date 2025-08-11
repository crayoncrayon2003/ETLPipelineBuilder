# backend/utils/__init__.py

"""
Utilities Package

This package provides a collection of shared, reusable utility functions
and classes that can be used across the entire ETL framework.

These utilities are not specific to any single ETL phase but provide
common functionalities like:
- Logging configuration.
- File system operations (e.g., safe creation of directories).
- Configuration loading helpers.
- SQL template rendering.
"""

# Import key utility functions/classes to make them easily accessible
# from the 'utils' package.
from .config_loader import load_config_from_path
from .file_utils import ensure_directory_exists, get_project_root
from .logger import setup_logger
from .sql_template import render_sql_template

# Define the public API for the 'utils' package.
__all__ = [
    'load_config_from_path',
    'ensure_directory_exists',
    'get_project_root',
    'setup_logger',
    'render_sql_template',
]