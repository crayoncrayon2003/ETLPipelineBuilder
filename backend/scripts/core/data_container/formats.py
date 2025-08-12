from enum import Enum, auto

class SupportedFormats(Enum):
    """
    An enumeration of the data formats supported by the ETL framework.

    Using an Enum for formats helps prevent typos and makes the code
    more readable and maintainable compared to using plain strings.
    It provides a single source of truth for all supported formats.
    """

    CSV = 'csv'
    JSON = 'json'
    PARQUET = 'parquet'
    XML = 'xml'
    SHAPEFILE = 'shapefile'
    GEOJSON = 'geojson'
    GTFS = 'gtfs'
    GTFS_RT = 'gtfs-rt'
    NGSI = 'ngsi'
    NGSI_LD = 'ngsi-ld'
    PANDAS_DATAFRAME = 'dataframe'
    ZIP = 'zip'
    GZIP = 'gzip'
    TAR = 'tar'
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