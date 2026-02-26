import re
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


def _is_absolute_path(path: str) -> bool:
    if path.startswith("/"):
        return True

    if re.match(r'^[a-zA-Z]:[/\\]', path):
        return True
    return False


def _normalize_s3_path(path: str, project_root: str) -> str:
    return path


def _normalize_http_path(path: str, project_root: str) -> str:
    return path


def _normalize_file_path(path: str, project_root: str) -> str:
    parsed = urlparse(path)
    local_path = url2pathname(parsed.path)
    return _normalize_local_path(local_path, project_root)


def _normalize_local_path(path: str, project_root: str) -> str:
    if _is_absolute_path(path):
        return str(Path(path))
    return str(Path(project_root) / Path(path))


SCHEME_NORMALIZERS: Dict[str, NormalizeFunc] = {
    "s3": _normalize_s3_path,
    "http": _normalize_http_path,
    "https": _normalize_http_path,
    "file": _normalize_file_path,
    "": _normalize_local_path,
}


def normalize_path(path: str, project_root: str) -> str:
    scheme = get_scheme(path)
    normalizer = SCHEME_NORMALIZERS.get(scheme)
    if normalizer is None:
        raise ValueError(f"Unknown scheme '{scheme}' in path '{path}'")
    return normalizer(path, project_root)

def parse_s3_path(path: str) -> tuple[str, str]:
    parsed = urlparse(path)
    if parsed.scheme != "s3":
        raise ValueError(f"Not an S3 path: {path}")
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    return bucket, key
