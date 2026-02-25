from urllib.parse import urlparse
from urllib.request import url2pathname
from pathlib import Path
from typing import Callable, Dict

NormalizeFunc = Callable[[str, str], str]

def get_scheme(path: str) -> str:
    parsed = urlparse(path)
    scheme = parsed.scheme
    if len(scheme) == 1 and scheme.isalpha():
        return ""
    return scheme

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
    parsed = urlparse(path)
    local_path = url2pathname(parsed.path)
    return _normalize_local_path(local_path, project_root)

def _normalize_local_path(path: str, project_root: str) -> str:

    p = Path(path)
    if p.is_absolute():
        return str(p)

    return str(Path(project_root) / p)

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
    scheme = get_scheme(path)

    normalizer = SCHEME_NORMALIZERS.get(scheme)
    if normalizer is None:
        # Unknown schemes can raise error or return path as-is
        raise ValueError(f"Unknown scheme '{scheme}' in path '{path}'")
    return normalizer(path, project_root)
