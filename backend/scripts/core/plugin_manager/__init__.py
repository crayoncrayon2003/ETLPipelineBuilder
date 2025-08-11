# backend/core/plugin_manager/__init__.py

"""
Plugin Manager Module (powered by pluggy)

This module handles the discovery, registration, and management of
all plugins within the ETL framework using the `pluggy` library.
Its components are primarily used internally by the `StepExecutor`.
"""

# This file can be nearly empty as other parts of the framework
# will directly import the specific components they need, like the
# `framework_manager` singleton from the `manager` module.