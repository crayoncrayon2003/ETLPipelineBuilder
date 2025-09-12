from urllib.parse import urlparse
from typing import Callable, Dict

NormalizeFunc = Callable[[str, str], str]

def get_scheme(path: str) -> str:
    return urlparse(path).scheme

def is_remote_path(path: str) -> bool:
    scheme = get_scheme(path)
    return scheme in {"s3", "http", "https"}

def is_local_path(path: str) -> bool:
    scheme = get_scheme(path)
    return scheme in {"", "file"}

def _normalize_s3_path(path: str, project_root: str) -> str:
    # For s3 scheme, return the path as is
    return path

def _normalize_http_path(path: str, project_root: str) -> str:
    # For http/https schemes, return the path as is
    return path

def _normalize_file_path(path: str, project_root: str) -> str:
    # For file scheme, convert to absolute local path
    if not path.startswith("file://"):
        return path
    local_path = path[len("file://"):]
    if not local_path.startswith("/"):
        local_path = f"{project_root}/{local_path}"
    return local_path

def _normalize_local_path(path: str, project_root: str) -> str:
    # For local paths without scheme, convert to absolute path relative to project_root
    if not path.startswith("/"):
        return f"{project_root}/{path}"
    return path

# Mapping table from scheme to normalization function
SCHEME_NORMALIZERS: Dict[str, NormalizeFunc] = {
    "s3": _normalize_s3_path,
    "http": _normalize_http_path,
    "https": _normalize_http_path,
    "file": _normalize_file_path,
    "": _normalize_local_path,  # No scheme means local file path
}

def normalize_path(path: str, project_root: str) -> str:
    """
    Normalize path based on its scheme.
    New schemes can be supported by adding them to SCHEME_NORMALIZERS.
    """
    parsed = urlparse(path)
    scheme = parsed.scheme

    normalizer = SCHEME_NORMALIZERS.get(scheme)
    if normalizer is None:
        # Unknown schemes can raise error or return path as-is
        raise ValueError(f"Unknown scheme '{scheme}' in path '{path}'")
    return normalizer(path, project_root)
