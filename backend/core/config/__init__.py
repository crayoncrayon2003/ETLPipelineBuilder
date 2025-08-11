# backend/core/config/__init__.py

"""
Configuration Management Module

This module provides classes and functions for loading, validating,
and accessing configuration settings for the ETL framework and its plugins.
It is distinct from the pipeline-specific configuration parsed by
`PipelineParser`.
"""

# Import the main class for loading configuration files.
from .loader import ConfigLoader

# Import the main class for validating configuration schemas.
from .validator import ConfigValidator

# Define the public API for the 'config' package.
__all__ = [
    'ConfigLoader',
    'ConfigValidator',
]