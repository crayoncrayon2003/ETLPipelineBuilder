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
        """
        search_val = value.lower()
        for member in cls:
            if member.value == search_val:
                return member
        raise ValueError(f"'{value}' is not a valid supported format.")

    @classmethod
    def from_path(cls, path: Union[str, os.PathLike]) -> 'SupportedFormats':
        """
        Infers the SupportedFormats member from a file path's extension.
        """
        _, ext = os.path.splitext(path)
        suffix = ext.lower().lstrip('.')

        if suffix == 'csv': return cls.CSV
        if suffix == 'parquet': return cls.PARQUET
        if suffix in ['xls', 'xlsx']: return cls.EXCEL
        if suffix == 'json': return cls.JSON
        if suffix == 'jsonl': return cls.JSONL
        if suffix == 'xml': return cls.XML
        if suffix == 'shp': return cls.SHAPEFILE
        if suffix == 'geojson': return cls.GEOJSON
        if suffix == 'txt': return cls.TEXT
        if suffix == 'zip': return cls.ZIP
        if suffix == 'tar': return cls.TAR

        return cls.UNKNOWN