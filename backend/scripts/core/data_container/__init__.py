# backend/core/data_container/__init__.py

"""
Data Container Module

This module provides the data structures used to pass data between
pipeline steps. The central component is the `DataContainer` class,
which acts as a standardized wrapper for various data formats
and associated metadata.
"""

from .container import DataContainer
from .formats import SupportedFormats

__all__ = [
    'DataContainer',
    'SupportedFormats',
]