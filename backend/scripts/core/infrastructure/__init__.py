"""
Infrastructure Package

This package provides adapters and resolvers for interacting with external
systems and infrastructure, such as storage backends (local, S3) and
secret management services.

It abstracts away the technical details of these interactions from the
core business logic of the ETL plugins.
"""

from .storage_adapter import storage_adapter
from .secret_resolver import secret_resolver
from .storage_path_utils import normalize_path, is_remote_path, is_local_path

__all__ = [
    "storage_adapter",
    "secret_resolver",
    "normalize_path",
    "is_remote_path",
    "is_local_path",
]