import pytest
from scripts.core.data_container.formats import SupportedFormats


class TestSupportedFormats:
    """Test class for SupportedFormats"""

    @pytest.mark.parametrize(
        "input_str, expected",
        [
            ("csv", SupportedFormats.CSV),
            ("CSV", SupportedFormats.CSV),
            ("parquet", SupportedFormats.PARQUET),
            ("unknown_format", None),
        ]
    )
    def test_from_string(self, input_str, expected):
        """Test the from_string method"""
        # Act & Assert
        if expected is None:
            with pytest.raises(ValueError):
                SupportedFormats.from_string(input_str)
        else:
            result = SupportedFormats.from_string(input_str)
            assert result == expected

    @pytest.mark.parametrize(
        "file_path, expected",
        [
            ("data/file.csv", SupportedFormats.CSV),
            ("data/file.parquet", SupportedFormats.PARQUET),
            ("data/file.xlsx", SupportedFormats.EXCEL),
            ("data/file.xls", SupportedFormats.EXCEL),
            ("data/file.json", SupportedFormats.JSON),
            ("data/file.jsonl", SupportedFormats.JSONL),
            ("data/file.xml", SupportedFormats.XML),
            ("data/file.shp", SupportedFormats.SHAPEFILE),
            ("data/file.geojson", SupportedFormats.GEOJSON),
            ("data/file.txt", SupportedFormats.TEXT),
            ("data/file.zip", SupportedFormats.ZIP),
            ("data/file.tar", SupportedFormats.TAR),
            ("data/file.unknownext", SupportedFormats.UNKNOWN),
            ("data/file", SupportedFormats.UNKNOWN),
        ]
    )
    def test_from_path(self, file_path, expected):
        """Test the from_path method"""
        # Act
        result = SupportedFormats.from_path(file_path)

        # Assert
        assert result == expected


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
