# backend/api/routers/__init__.py

# Import the router modules from this package so they can be accessed
# via the `backend.api.routers` namespace.
from . import plugins
from . import pipelines

# Define the public API for this package.
__all__ = [
    "plugins",
    "pipelines",
]