import os
from typing import Union
from enum import Enum


class SupportedFormats(Enum):
    """
    An enumeration of the data formats supported by the ETL framework.
    This serves as the single source of truth for file format handling.
    """

    # --- Tabular File Formats ---
    CSV = 'csv'
    PARQUET = 'parquet'
    EXCEL = 'excel'

    # --- Structured Text Formats ---
    JSON = 'json'
    JSONL = 'jsonl'
    XML = 'xml'

    # --- Geospatial Formats ---
    SHAPEFILE = 'shapefile'
    GEOJSON = 'geojson'

    # --- Transport/Specification Formats ---
    GTFS = 'gtfs'
    GTFS_RT = 'gtfs-rt'

    # --- Compressed Formats ---
    ZIP = 'zip'
    TAR = 'tar'

    # --- Generic/Other ---
    TEXT = 'text'
    BINARY = 'binary'
    UNKNOWN = 'unknown'

    @classmethod
    def from_string(cls, value: str) -> 'SupportedFormats':
        """
        Converts a string value to a SupportedFormats member.
        This is case-insensitive.

        Raises:
            ValueError: If the value does not match any supported format.
        """
        # Enum が内部で保持する_value2member_map_ を使うことで定数時間で解決できる。
        member = cls._value2member_map_.get(value.lower())
        if member is not None:
            return member
        raise ValueError(f"'{value}' is not a valid supported format.")

    @classmethod
    def from_path(cls, path: Union[str, os.PathLike]) -> 'SupportedFormats':
        """
        Infers the SupportedFormats member from a file path's extension.

        Notes:
            - GTFS and GTFS_RT cannot be identified by extension alone.
              GTFS is distributed as .zip, GTFS-RT as binary streams (.pb).
              Use from_string() when the format is already known.
            - BINARY has no associated file extension and is never returned
              by this method.
            - Compound extensions such as .tar.gz are not fully supported;
              only the last extension (.gz) is examined, which returns UNKNOWN.
        """
        # .tar.gz のような複合拡張子を部分的に対応する。
        # os.path.splitext は最後の拡張子しか取れないため、
        # stem 部分を再度 splitext にかけて .tar を検出する。
        # 例: "data.tar.gz" → ext=".gz", stem_ext=".tar" → TAR
        str_path = os.fspath(path)
        stem, ext = os.path.splitext(str_path)
        suffix = ext.lower().lstrip('.')

        # .gz / .bz2 / .xz は圧縮ラッパーの可能性があるため
        # stem の拡張子を追加で確認する
        if suffix in ('gz', 'bz2', 'xz'):
            _, inner_ext = os.path.splitext(stem)
            inner_suffix = inner_ext.lower().lstrip('.')
            if inner_suffix == 'tar':
                return cls.TAR

        # .pb (Protocol Buffers) を GTFS_RT として認識する。
        # GTFS Realtime フィードは .pb 形式で配布されることが多い。
        if suffix == 'pb':
            return cls.GTFS_RT

        if suffix == 'csv':     return cls.CSV
        if suffix == 'parquet': return cls.PARQUET
        if suffix in ('xls', 'xlsx'): return cls.EXCEL
        if suffix == 'json':    return cls.JSON
        if suffix == 'jsonl':   return cls.JSONL
        if suffix == 'xml':     return cls.XML
        if suffix == 'shp':     return cls.SHAPEFILE
        if suffix == 'geojson': return cls.GEOJSON
        if suffix == 'txt':     return cls.TEXT
        if suffix == 'zip':     return cls.ZIP
        if suffix == 'tar':     return cls.TAR

        return cls.UNKNOWN