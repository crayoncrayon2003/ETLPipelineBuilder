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


def is_memory_path(path: str) -> bool:
    return get_scheme(path) == "memory"


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


def _normalize_memory_path(path: str, project_root: str) -> str:
    return path


def _normalize_file_path(path: str, project_root: str) -> str:
    """
    Normalize a file:// URI to a local path.

    RFC 3986 では file://host/path の形式だが、
    file://relative/path.txt のように相対パスを表す意図で使われることがある。
    そのため netloc が存在する場合は netloc/path を相対パスとして結合する。

    Examples:
        file:///tmp/foo.txt  -> /tmp/foo.txt
        file://data/foo.txt  -> {project_root}/data/foo.txt
        file://              -> {project_root}/
    """
    parsed = urlparse(path)

    if parsed.netloc:
        # netloc が存在する場合: netloc + path を相対パスとして project_root に結合
        # 例: file://data/foo.txt → netloc="data", path="/foo.txt"
        #     → "data/foo.txt" として project_root に結合
        local_path = parsed.netloc + ("/" if parsed.path else "") + parsed.path.lstrip("/")
    else:
        # netloc が空の場合: path をそのまま使用
        # 例: file:///tmp/foo.txt → path="/tmp/foo.txt"
        local_path = url2pathname(parsed.path)

    return _normalize_local_path(local_path, project_root)


def _normalize_local_path(path: str, project_root: str) -> str:
    """
    Normalize a local path (relative or absolute) against project_root.

    - 絶対パス (Unix: /foo, Windows: C:/foo) はそのまま返す
    - 相対パスは project_root に結合する
    - 空文字列または末尾が "/" のパスは末尾スラッシュを保持する
    """
    preserve_trailing_slash = path == "" or path.endswith("/")

    if _is_absolute_path(path):
        # Windowsパス (C:/foo) は Path() に渡すと Linux 上で誤動作するため文字列のまま扱う
        if re.match(r'^[a-zA-Z]:[/\\]', path):
            result = path
        else:
            result = str(Path(path))
    else:
        result = str(Path(project_root) / path)

    # Path() は末尾スラッシュを除去するため、必要な場合は復元する
    if preserve_trailing_slash and not result.endswith("/"):
        result += "/"

    return result


SCHEME_NORMALIZERS: Dict[str, NormalizeFunc] = {
    "s3": _normalize_s3_path,
    "http": _normalize_http_path,
    "https": _normalize_http_path,
    "file": _normalize_file_path,
    "memory": _normalize_memory_path,
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