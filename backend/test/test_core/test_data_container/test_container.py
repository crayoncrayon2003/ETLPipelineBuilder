import pytest
import pandas as pd
from pathlib import Path
from scripts.core.data_container.container import DataContainer, DataContainerStatus


class TestDataContainer:
    """Test class for DataContainer"""

    @pytest.fixture
    def dc(self):
        """Fixture that returns a default DataContainer instance"""
        return DataContainer()

    def test_initialization(self, dc):
        """Test the initial state of DataContainer"""
        assert dc.data is None
        assert dc.metadata == {}
        assert dc.file_paths == []
        assert dc.errors == []
        assert dc.status == DataContainerStatus.PENDING
        assert dc.history == []
        assert dc.schema is None

    def test_add_and_get_file_paths(self, dc):
        """Test adding and retrieving file paths"""
        dc.add_file_path("file1.csv")
        dc.add_file_path(Path("file2.csv"))
        paths = dc.get_file_paths()
        assert paths == ["file1.csv", "file2.csv"]
        assert dc.get_primary_file_path() == "file1.csv"

    def test_get_primary_file_path_raises_error_when_empty(self):
        """Test that an exception is raised if file paths are empty"""
        dc_empty = DataContainer()
        with pytest.raises(FileNotFoundError):
            dc_empty.get_primary_file_path()

    def test_set_and_get_status(self, dc):
        """Test setting and getting the status"""
        dc.set_status(DataContainerStatus.SUCCESS)
        assert dc.get_status() == DataContainerStatus.SUCCESS

    def test_add_and_get_history(self, dc):
        """Test adding and retrieving history"""
        dc.add_history("pluginA")
        dc.add_history("pluginB")
        assert dc.get_history() == ["pluginA", "pluginB"]

    def test_set_and_get_schema(self, dc):
        """Test setting and retrieving the schema"""
        schema = {"fields": ["a", "b"]}
        dc.set_schema(schema)
        assert dc.get_schema() == schema

    def test_add_error_updates_metadata(self, dc):
        """Test that adding errors updates metadata"""
        dc.add_error("error1")
        dc.add_error("error2")
        assert dc.errors == ["error1", "error2"]
        assert dc.metadata["errors"] == ["error1", "error2"]

    def test_add_error_with_existing_metadata(self):
        """Test that errors can be added to existing metadata"""
        dc = DataContainer(metadata={"errors": ["existing"]})
        dc.add_error("new_error")
        assert dc.errors == ["new_error"]
        assert dc.metadata["errors"] == ["existing", "new_error"]

    def test_to_dict_with_data(self):
        """Test converting a DataContainer with DataFrame to a dictionary"""
        df = pd.DataFrame({"a": [1, 2]})
        dc = DataContainer(data=df, metadata={"user": "test"})
        dc.add_file_path("file.csv")
        dc.add_error("some error")
        dc.add_history("plugin1")
        dc.set_schema({"a": "int"})
        dc_dict = dc.to_dict()

        assert dc_dict["data"] == df.to_dict()
        assert dc_dict["metadata"]["user"] == "test"
        assert dc_dict["file_paths"] == ["file.csv"]
        assert dc_dict["errors"] == ["some error"]
        assert dc_dict["status"] == "pending"
        assert dc_dict["history"] == ["plugin1"]
        assert dc_dict["schema"] == {"a": "int"}

    def test_to_dict_without_data(self, dc):
        """Test to_dict output when data is None"""
        dc_dict = dc.to_dict()
        assert dc_dict["data"] is None
        assert dc_dict["metadata"] == {}
        assert dc_dict["file_paths"] == []
        assert dc_dict["errors"] == []
        assert dc_dict["status"] == "pending"
        assert dc_dict["history"] == []
        assert dc_dict["schema"] is None

    def test_repr_with_data(self):
        """Test repr output when data exists"""
        df = pd.DataFrame({"x": [1, 2, 3]})
        dc = DataContainer(data=df)
        dc.add_file_path("path.csv")
        rep = repr(dc)
        assert "Data Shape: (3, 1)" in rep
        assert "File Paths: 1" in rep
        assert "Status: pending" in rep

    def test_repr_without_data(self):
        """Test repr output when data does not exist"""
        dc = DataContainer()
        rep = repr(dc)
        assert "N/A (file-based)" in rep

    def test_status_enum_values(self):
        """Test that DataContainerStatus enum values are correct"""
        expected_statuses = {
            "PENDING": "pending",
            "SUCCESS": "success",
            "ERROR": "error",
            "SKIPPED": "skipped",
            "VALIDATION_FAILED": "validation_failed",
            "TRANSFORMED": "transformed",
            "LOADED": "loaded",
        }

        for name, value in expected_statuses.items():
            assert hasattr(DataContainerStatus, name), f"{name} is not defined"
            enum_member = getattr(DataContainerStatus, name)
            assert enum_member.value == value, f"{name} value is not {value}"

        # Check representative type
        assert isinstance(DataContainerStatus.PENDING, DataContainerStatus)
        assert str(DataContainerStatus.PENDING) == "DataContainerStatus.PENDING"

    def test_add_multiple_file_paths_and_order(self, dc):
        """Test the order when multiple files are added sequentially"""
        dc.add_file_path("fileA.csv")
        dc.add_file_path("fileB.csv")
        dc.add_file_path("fileC.csv")
        assert dc.get_file_paths() == ["fileA.csv", "fileB.csv", "fileC.csv"]
        assert dc.get_primary_file_path() == "fileA.csv"

    def test_to_dict_reflects_latest_status(self, dc):
        """Test that to_dict reflects the latest status"""
        dc.set_status(DataContainerStatus.SUCCESS)
        result = dc.to_dict()
        assert result["status"] == "success"

    def test_add_error_does_not_overwrite_existing_metadata(self):
        """Test that existing metadata is not overwritten when adding errors"""
        dc = DataContainer(metadata={"source": "test"})
        dc.add_error("errX")
        assert "source" in dc.metadata
        assert "errors" in dc.metadata
        assert dc.metadata["source"] == "test"
        assert dc.metadata["errors"] == ["errX"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
