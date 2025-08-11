# backend/core/__init__.py

"""
ETL Framework Core Module

This module serves as the primary entry point for the core functionalities
of the ETL framework. It exposes key components needed to build and run ETL flows.
"""

# --- Public API of the Core Framework ---

# DataContainer is the fundamental data carrier.
from .data_container.container import DataContainer

# StepExecutor is the engine that runs a single plugin.
from .pipeline.step_executor import StepExecutor

# Define the public API of the 'core' package using __all__.
# These are the components that an ETL flow developer is expected to use.
__all__ = [
    'DataContainer',
    'StepExecutor',
]