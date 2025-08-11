# backend/core/pipeline/__init__.py

"""
Pipeline Module

This module provides the core execution engine for a single ETL step.
The main component is the `StepExecutor`.
"""

# In the Prefect-native design, the StepExecutor is the primary public
# component of this package.
from .step_executor import StepExecutor

# Define the public API for the 'pipeline' package.
__all__ = [
    'StepExecutor',
]