import os
import pytest
import pandas as pd
from pathlib import Path
from scripts.core.infrastructure.storage_adapter import StorageAdapter
from scripts.core.infrastructure.storage_path_utils import is_local_path, is_remote_path
from scripts.core.data_container.formats import SupportedFormats

class TestStorageAdapter:

    @pytest.fixture
    def sa(self):
        return StorageAdapter()

    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})

    # -----------------------------
    # read_df / write_df
    # -----------------------------
    @pytest.mark.parametrize("fmt", ["csv", "parquet", "json"])
    def test_read_write_df_local(self, sa, tmp_path, sample_df, fmt):
        file_path = tmp_path / f"test.{fmt}"
        sa.write_df(sample_df, str(file_path))
        assert file_path.exists()
        df = sa.read_df(str(file_path))
        pd.testing.assert_frame_equal(df, sample_df)

    # -----------------------------
    # read_text / write_text
    # -----------------------------
    def test_read_write_text_local(self, sa, tmp_path):
        file_path = tmp_path / "text.txt"
        content = "hello\nworld"
        sa.write_text(content, str(file_path))
        assert file_path.exists()
        result = sa.read_text(str(file_path))
        assert result == content

    # -----------------------------
    # read_bytes / write_bytes
    # -----------------------------
    def test_read_write_bytes_local(self, sa, tmp_path):
        file_path = tmp_path / "bytes.bin"
        data = b"\x00\x01\x02"
        sa.write_bytes(data, str(file_path))
        assert file_path.exists()
        result = sa.read_bytes(str(file_path))
        assert result == data

    # -----------------------------
    # exists / delete / mkdir / is_dir
    # -----------------------------
    def test_exists_delete_mkdir_is_dir(self, sa, tmp_path):
        dir_path = tmp_path / "subdir"
        file_path = dir_path / "file.txt"
        sa.mkdir(str(dir_path))
        assert sa.is_dir(str(dir_path))
        assert dir_path.exists()
        sa.write_text("abc", str(file_path))
        assert sa.exists(str(file_path))
        sa.delete(str(file_path))
        assert not sa.exists(str(file_path))

    # -----------------------------
    # copy_file / copy_file_raw / move_file / rename
    # -----------------------------
    def test_copy_move_rename(self, sa, tmp_path, sample_df):
        src_file = tmp_path / "src.csv"
        dst_file = tmp_path / "dst.csv"
        moved_file = tmp_path / "moved.csv"
        renamed_file = tmp_path / "renamed.csv"

        sa.write_df(sample_df, str(src_file))
        sa.copy_file(str(src_file), str(dst_file))
        df = sa.read_df(str(dst_file))
        pd.testing.assert_frame_equal(df, sample_df)

        raw_file = tmp_path / "raw.txt"
        raw_copy = tmp_path / "raw_copy.txt"
        sa.write_text("raw content", str(raw_file))
        sa.copy_file_raw(str(raw_file), str(raw_copy))
        assert sa.read_text(str(raw_copy)) == "raw content"

        sa.move_file(str(raw_copy), str(moved_file))
        assert not os.path.exists(raw_copy)
        assert os.path.exists(moved_file)

        sa.rename(str(moved_file), str(renamed_file))
        assert not os.path.exists(moved_file)
        assert os.path.exists(renamed_file)

    # -----------------------------
    # get_size / stat
    # -----------------------------
    def test_get_size_stat(self, sa, tmp_path):
        file_path = tmp_path / "file.txt"
        sa.write_text("abc", str(file_path))
        size = sa.get_size(str(file_path))
        assert size == 3
        stat_info = sa.stat(str(file_path))
        assert stat_info["size"] == 3
        assert "last_modified" in stat_info

    # -----------------------------
    # list_files
    # -----------------------------
    def test_list_files(self, sa, tmp_path):
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        file1 = subdir / "f1.txt"
        file2 = subdir / "f2.txt"
        sa.write_text("1", str(file1))
        sa.write_text("2", str(file2))
        files = sa.list_files(str(subdir))
        assert set(files) == {str(file1), str(file2)}

    # -----------------------------
    # normalize_path, is_remote_path, is_local_path
    # -----------------------------
    def test_path_utils(self, sa, tmp_path):
        local_file = tmp_path / "f.txt"
        local_file.touch()
        path = str(local_file)
        assert sa.exists(path)
        assert is_local_path(path)
        assert not is_remote_path(path)
        norm = sa._get_storage_options(path)
        assert isinstance(norm, dict)

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
