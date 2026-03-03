import pytest
import pandas as pd
from pathlib import Path
from core.data_container.container import DataContainer, DataContainerStatus


class TestDataContainer:
    """Test class for DataContainer"""

    @pytest.fixture
    def dc(self):
        """Fixture that returns a default DataContainer instance"""
        return DataContainer()

    # ======================================================================
    # __init__
    # MCDC:
    #   条件A: data is None (デフォルト) vs 指定あり
    #   条件B: metadata is None (デフォルト) vs 指定あり
    #   条件C: status がデフォルト vs 指定あり
    # ======================================================================

    def test_initialization_defaults(self, dc):
        """A=None, B=None, C=default: デフォルト状態の初期値確認"""
        assert dc.data is None
        assert dc.metadata == {}
        assert dc.file_paths == []
        assert dc.errors == []
        assert dc.status == DataContainerStatus.PENDING
        assert dc.history == []
        assert dc.schema is None

    def test_initialization_with_data(self):
        """A=DataFrameあり: data が正しく設定される"""
        df = pd.DataFrame({"a": [1, 2]})
        dc = DataContainer(data=df)
        assert dc.data is not None
        assert dc.data.equals(df)

    def test_initialization_with_metadata(self):
        """B=dictあり: metadata が正しく設定される"""
        meta = {"source": "db", "version": 1}
        dc = DataContainer(metadata=meta)
        assert dc.metadata == {"source": "db", "version": 1}

    def test_initialization_with_status(self):
        """C=status指定あり: 指定したステータスで初期化される"""
        dc = DataContainer(status=DataContainerStatus.SUCCESS)
        assert dc.status == DataContainerStatus.SUCCESS

    # ======================================================================
    # add_file_path / get_file_paths / get_primary_file_path
    # MCDC:
    #   条件D: file_paths が空 vs 空でない
    # ======================================================================

    def test_add_and_get_file_paths(self, dc):
        """str と pathlib.Path の両方を追加して正しく取得できる"""
        dc.add_file_path("file1.csv")
        dc.add_file_path(Path("file2.csv"))
        assert dc.get_file_paths() == ["file1.csv", "file2.csv"]

    def test_add_multiple_file_paths_preserves_order(self, dc):
        """追加順が保持される"""
        dc.add_file_path("fileA.csv")
        dc.add_file_path("fileB.csv")
        dc.add_file_path("fileC.csv")
        assert dc.get_file_paths() == ["fileA.csv", "fileB.csv", "fileC.csv"]

    def test_get_primary_file_path_returns_first(self, dc):
        """D=空でない: 最初に追加したパスを返す"""
        dc.add_file_path("file1.csv")
        dc.add_file_path("file2.csv")
        assert dc.get_primary_file_path() == "file1.csv"

    def test_get_primary_file_path_raises_value_error_when_empty(self):
        """D=空: ValueError を raise する"""
        dc = DataContainer()
        with pytest.raises(ValueError):
            dc.get_primary_file_path()

    # ======================================================================
    # set_status / get_status
    # MCDC:
    #   条件E: status が DataContainerStatus のインスタンスか否か
    # ======================================================================

    @pytest.mark.parametrize("status", list(DataContainerStatus))
    def test_set_status_all_valid_values(self, dc, status):
        """E=True: 全ステータス値を正しく設定できる"""
        dc.set_status(status)
        assert dc.get_status() == status

    @pytest.mark.parametrize("invalid_value", [
        "success",      # str
        1,              # int
        None,           # None
        DataContainerStatus.SUCCESS.value,  # 文字列 "success"
    ])
    def test_set_status_invalid_type_raises_type_error(self, dc, invalid_value):
        """E=False: DataContainerStatus 以外の型 → TypeError"""
        with pytest.raises(TypeError):
            dc.set_status(invalid_value)

    # ======================================================================
    # add_history / get_history
    # ======================================================================

    def test_add_and_get_history(self, dc):
        """追加順が保持されて取得できる"""
        dc.add_history("pluginA")
        dc.add_history("pluginB")
        assert dc.get_history() == ["pluginA", "pluginB"]

    # ======================================================================
    # set_schema / get_schema
    # ======================================================================

    def test_set_and_get_schema(self, dc):
        """スキーマを設定して取得できる"""
        schema = {"fields": ["a", "b"]}
        dc.set_schema(schema)
        assert dc.get_schema() == schema

    def test_get_schema_returns_none_by_default(self, dc):
        """デフォルトは None"""
        assert dc.get_schema() is None

    # ======================================================================
    # add_error
    # MCDC:
    #   self.errors のみが唯一の情報源
    # ======================================================================

    def test_add_error_appends_to_errors(self, dc):
        """エラーが self.errors に追加される"""
        dc.add_error("error1")
        dc.add_error("error2")
        assert dc.errors == ["error1", "error2"]

    def test_add_error_does_not_write_to_metadata(self, dc):
        """add_error は metadata['errors'] に書き込まない"""
        dc.add_error("error1")
        assert "errors" not in dc.metadata

    def test_add_error_does_not_affect_existing_metadata_keys(self):
        """ 既存のmetadataキーは保持され、errorsキーは追加されない"""
        dc = DataContainer(metadata={"source": "test"})
        dc.add_error("errX")
        assert dc.metadata["source"] == "test"
        assert "errors" not in dc.metadata

    # ======================================================================
    # metadata コピー
    # ======================================================================

    def test_metadata_is_copied_not_referenced(self):
        """外部から渡した dict は内部でコピーされ参照共有しない"""
        external = {"source": "db"}
        dc = DataContainer(metadata=external)
        dc.add_error("err")
        dc.metadata["extra"] = "x"
        # 呼び出し元の dict は変化しない
        assert external == {"source": "db"}

    # ======================================================================
    # to_dict
    # MCDC:
    #   条件F: data is None vs DataFrameあり
    # ======================================================================

    def test_to_dict_without_data(self, dc):
        """F=None: data が None のとき to_dict の data キーは None"""
        result = dc.to_dict()
        assert result["data"] is None
        assert result["metadata"] == {}
        assert result["file_paths"] == []
        assert result["errors"] == []
        assert result["status"] == "pending"
        assert result["history"] == []
        assert result["schema"] is None

    def test_to_dict_with_data_uses_records_orient(self):
        """F=DataFrameあり: data は orient='records' 形式 """
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        dc = DataContainer(data=df)
        result = dc.to_dict()
        assert result["data"] == [{"a": 1, "b": 3}, {"a": 2, "b": 4}]

    def test_to_dict_with_all_fields(self):
        """全フィールドが to_dict に反映される"""
        df = pd.DataFrame({"a": [1, 2]})
        dc = DataContainer(data=df, metadata={"user": "test"})
        dc.add_file_path("file.csv")
        dc.add_error("some error")
        dc.add_history("plugin1")
        dc.set_schema({"a": "int"})
        result = dc.to_dict()

        assert result["data"] == df.to_dict(orient="records")
        assert result["metadata"]["user"] == "test"
        assert result["file_paths"] == ["file.csv"]
        assert result["errors"] == ["some error"]
        assert result["status"] == "pending"
        assert result["history"] == ["plugin1"]
        assert result["schema"] == {"a": "int"}

    def test_to_dict_reflects_latest_status(self, dc):
        """status 変更が to_dict に反映される"""
        dc.set_status(DataContainerStatus.SUCCESS)
        assert dc.to_dict()["status"] == "success"

    # ======================================================================
    # __repr__
    # ======================================================================

    def test_repr_with_data(self):
        """data があるとき shape が表示される"""
        df = pd.DataFrame({"x": [1, 2, 3]})
        dc = DataContainer(data=df)
        dc.add_file_path("path.csv")
        rep = repr(dc)
        assert "Data Shape: (3, 1)" in rep
        assert "File Paths: 1" in rep
        assert "Status: pending" in rep

    def test_repr_without_data(self):
        """data がないとき N/A (file-based) が表示される"""
        dc = DataContainer()
        rep = repr(dc)
        assert "N/A (file-based)" in rep

    def test_repr_reflects_status(self):
        """status 変更が repr に反映される"""
        dc = DataContainer()
        dc.set_status(DataContainerStatus.SUCCESS)
        assert "Status: success" in repr(dc)

    # ======================================================================
    # DataContainerStatus Enum
    # ======================================================================

    def test_status_enum_values(self):
        """全ステータス値が正しく定義されている"""
        expected = {
            "PENDING":           "pending",
            "SUCCESS":           "success",
            "ERROR":             "error",
            "SKIPPED":           "skipped",
            "VALIDATION_FAILED": "validation_failed",
            "TRANSFORMED":       "transformed",
            "LOADED":            "loaded",
        }
        for name, value in expected.items():
            assert hasattr(DataContainerStatus, name), f"{name} is not defined"
            assert getattr(DataContainerStatus, name).value == value
        assert isinstance(DataContainerStatus.PENDING, DataContainerStatus)
        assert str(DataContainerStatus.PENDING) == "DataContainerStatus.PENDING"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])