# backend/core/data_container/formats.py

from enum import Enum, auto

class SupportedFormats(Enum):
    """
    An enumeration of the data formats supported by the ETL framework.

    Using an Enum for formats helps prevent typos and makes the code
    more readable and maintainable compared to using plain strings.
    It provides a single source of truth for all supported formats.
    """

    # --- Standard File Formats ---
    CSV = 'csv'
    JSON = 'json'
    PARQUET = 'parquet'
    XML = 'xml'

    # --- Geospatial Formats ---
    SHAPEFILE = 'shapefile'
    GEOJSON = 'geojson'

    # --- Transport/Specification Formats ---
    GTFS = 'gtfs'
    GTFS_RT = 'gtfs-rt'
    NGSI = 'ngsi'
    NGSI_LD = 'ngsi-ld'

    # --- In-memory Representations ---
    PANDAS_DATAFRAME = 'dataframe'

    # --- Archive Formats ---
    ZIP = 'zip'
    GZIP = 'gzip'
    TAR = 'tar'

    # --- Generic/Unknown ---
    FILE = 'file' # Represents a generic, unprocessed file
    TEXT = 'text' # Represents plain text content
    BINARY = 'binary' # Represents raw binary data
    UNKNOWN = 'unknown'

    @classmethod
    def from_string(cls, value: str) -> 'SupportedFormats':
        """
        Converts a string value to a SupportedFormats member.
        Case-insensitive matching.

        Args:
            value (str): The string representation of the format.

        Returns:
            SupportedFormats: The corresponding Enum member.

        Raises:
            ValueError: If the string does not match any member.
        """
        for member in cls:
            if member.value.lower() == value.lower():
                return member
        raise ValueError(f"'{value}' is not a valid supported format.")