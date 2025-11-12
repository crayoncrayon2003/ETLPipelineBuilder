import os
import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import s3fs
from scripts.core.infrastructure.storage_adapter import StorageAdapter, storage_adapter
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

    def test_read_df_excel(self, sa, tmp_path, sample_df):
        """Test reading Excel files"""
        file_path = tmp_path / "test.xlsx"
        sa.write_df(sample_df, str(file_path))
        df = sa.read_df(str(file_path))
        pd.testing.assert_frame_equal(df, sample_df)

    def test_read_df_jsonl(self, sa, tmp_path, sample_df):
        """Test reading JSONL files"""
        file_path = tmp_path / "test.jsonl"
        sa.write_df(sample_df, str(file_path))
        df = sa.read_df(str(file_path))
        pd.testing.assert_frame_equal(df, sample_df)

    def test_read_df_with_read_options(self, sa, tmp_path, sample_df):
        """Test read_df with custom read options"""
        file_path = tmp_path / "test.csv"
        sa.write_df(sample_df, str(file_path))
        df = sa.read_df(str(file_path), read_options={"nrows": 1})
        assert len(df) == 1

    def test_write_df_with_write_options(self, sa, tmp_path, sample_df):
        """Test write_df with custom write options"""
        file_path = tmp_path / "test.csv"
        sa.write_df(sample_df, str(file_path), write_options={"sep": ";"})
        content = sa.read_text(str(file_path))
        assert ";" in content

    def test_read_df_unsupported_format_raises_error(self, sa, tmp_path):
        """Test that unsupported format raises ValueError"""
        file_path = tmp_path / "test.unsupported"
        file_path.touch()
        with pytest.raises(ValueError, match="not supported"):
            sa.read_df(str(file_path))

    def test_write_df_unsupported_format_raises_error(self, sa, tmp_path, sample_df):
        """Test that unsupported format raises ValueError"""
        file_path = tmp_path / "test.unsupported"
        with pytest.raises(ValueError, match="not supported"):
            sa.write_df(sample_df, str(file_path))

    def test_read_df_nonexistent_file_raises_error(self, sa, tmp_path):
        """Test that reading nonexistent file raises error"""
        file_path = tmp_path / "nonexistent.csv"
        with pytest.raises(Exception):
            sa.read_df(str(file_path))

    def test_write_df_creates_parent_directories(self, sa, tmp_path, sample_df):
        """Test that write_df creates parent directories if they don't exist"""
        file_path = tmp_path / "nested" / "dir" / "test.csv"
        sa.write_df(sample_df, str(file_path))
        assert file_path.exists()

    # -----------------------------
    # Spark DataFrame tests
    # -----------------------------
    def test_read_df_spark_csv(self, sa, tmp_path, sample_df):
        """Test reading CSV with Spark"""
        file_path = tmp_path / "test.csv"
        sa.write_df(sample_df, str(file_path))

        mock_spark = Mock()
        mock_spark.read.options().csv.return_value = "spark_df"

        result = sa.read_df(str(file_path), read_options={"spark": mock_spark})
        mock_spark.read.options().csv.assert_called_once()

    def test_read_df_spark_parquet(self, sa, tmp_path, sample_df):
        """Test reading Parquet with Spark"""
        file_path = tmp_path / "test.parquet"
        sa.write_df(sample_df, str(file_path))

        mock_spark = Mock()
        mock_spark.read.options().parquet.return_value = "spark_df"

        result = sa.read_df(str(file_path), read_options={"spark": mock_spark})
        mock_spark.read.options().parquet.assert_called_once()

    def test_read_df_spark_json(self, sa, tmp_path, sample_df):
        """Test reading JSON with Spark"""
        file_path = tmp_path / "test.json"
        sa.write_df(sample_df, str(file_path))

        mock_spark = Mock()
        mock_spark.read.options().json.return_value = "spark_df"

        result = sa.read_df(str(file_path), read_options={"spark": mock_spark})
        mock_spark.read.options().json.assert_called_once()


    # -----------------------------
    # read_text / write_text (including encoding tests)
    # -----------------------------
    def test_read_write_text_local(self, sa, tmp_path):
        file_path = tmp_path / "text.txt"
        content = "hello\nworld"
        sa.write_text(content, str(file_path))
        assert file_path.exists()
        result = sa.read_text(str(file_path))
        assert result == content

    def test_read_write_text_utf8_encoding(self, sa, tmp_path):
        """Test reading and writing text with UTF-8 encoding (default)"""
        file_path = tmp_path / "text_utf8.txt"
        content = "こんにちは世界🌍"
        sa.write_text(content, str(file_path), encoding='utf-8')
        result = sa.read_text(str(file_path), encoding='utf-8')
        assert result == content

    def test_read_write_text_shift_jis_encoding(self, sa, tmp_path):
        """Test reading and writing text with Shift-JIS encoding"""
        file_path = tmp_path / "text_sjis.txt"
        content = "こんにちは世界"
        sa.write_text(content, str(file_path), encoding='shift_jis')
        result = sa.read_text(str(file_path), encoding='shift_jis')
        assert result == content

    def test_read_write_text_latin1_encoding(self, sa, tmp_path):
        """Test reading and writing text with Latin-1 encoding"""
        file_path = tmp_path / "text_latin1.txt"
        content = "Héllo Wörld"
        sa.write_text(content, str(file_path), encoding='latin-1')
        result = sa.read_text(str(file_path), encoding='latin-1')
        assert result == content

    def test_read_write_text_ascii_encoding(self, sa, tmp_path):
        """Test reading and writing text with ASCII encoding"""
        file_path = tmp_path / "text_ascii.txt"
        content = "Hello World"
        sa.write_text(content, str(file_path), encoding='ascii')
        result = sa.read_text(str(file_path), encoding='ascii')
        assert result == content

    def test_read_text_wrong_encoding_may_raise_error(self, sa, tmp_path):
        """Test that reading with wrong encoding may cause issues"""
        file_path = tmp_path / "text_utf8.txt"
        content = "こんにちは"
        sa.write_text(content, str(file_path), encoding='utf-8')
        # Reading with wrong encoding may raise UnicodeDecodeError
        with pytest.raises(UnicodeDecodeError):
            sa.read_text(str(file_path), encoding='ascii')

    def test_read_text_nonexistent_file_raises_error(self, sa, tmp_path):
        """Test that reading nonexistent file raises FileNotFoundError"""
        file_path = tmp_path / "nonexistent.txt"
        with pytest.raises(FileNotFoundError):
            sa.read_text(str(file_path))

    def test_write_text_creates_parent_directories(self, sa, tmp_path):
        """Test that write_text creates parent directories"""
        file_path = tmp_path / "nested" / "dir" / "text.txt"
        sa.write_text("content", str(file_path))
        assert file_path.exists()

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

    def test_read_bytes_nonexistent_file_raises_error(self, sa, tmp_path):
        """Test that reading nonexistent file raises FileNotFoundError"""
        file_path = tmp_path / "nonexistent.bin"
        with pytest.raises(FileNotFoundError):
            sa.read_bytes(str(file_path))

    def test_write_bytes_creates_parent_directories(self, sa, tmp_path):
        """Test that write_bytes creates parent directories"""
        file_path = tmp_path / "nested" / "dir" / "bytes.bin"
        sa.write_bytes(b"\x00", str(file_path))
        assert file_path.exists()

    def test_write_bytes_empty_content(self, sa, tmp_path):
        """Test writing empty bytes"""
        file_path = tmp_path / "empty.bin"
        sa.write_bytes(b"", str(file_path))
        assert file_path.exists()
        assert sa.get_size(str(file_path)) == 0

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

    def test_exists_nonexistent_file(self, sa, tmp_path):
        """Test exists returns False for nonexistent file"""
        file_path = tmp_path / "nonexistent.txt"
        assert not sa.exists(str(file_path))

    def test_delete_nonexistent_file_raises_error(self, sa, tmp_path):
        """Test that deleting nonexistent file raises FileNotFoundError"""
        file_path = tmp_path / "nonexistent.txt"
        with pytest.raises(FileNotFoundError):
            sa.delete(str(file_path))

    def test_mkdir_exist_ok_true(self, sa, tmp_path):
        """Test mkdir with exist_ok=True doesn't raise error"""
        dir_path = tmp_path / "subdir"
        sa.mkdir(str(dir_path), exist_ok=True)
        sa.mkdir(str(dir_path), exist_ok=True)  # Should not raise
        assert dir_path.exists()

    def test_mkdir_exist_ok_false(self, sa, tmp_path):
        """Test mkdir with exist_ok=False raises error if directory exists"""
        dir_path = tmp_path / "subdir"
        sa.mkdir(str(dir_path), exist_ok=False)
        with pytest.raises(FileExistsError):
            sa.mkdir(str(dir_path), exist_ok=False)

    def test_is_dir_false_for_file(self, sa, tmp_path):
        """Test is_dir returns False for files"""
        file_path = tmp_path / "file.txt"
        sa.write_text("content", str(file_path))
        assert not sa.is_dir(str(file_path))

    def test_is_dir_false_for_nonexistent(self, sa, tmp_path):
        """Test is_dir returns False for nonexistent path"""
        dir_path = tmp_path / "nonexistent"
        assert not sa.is_dir(str(dir_path))

    @patch('boto3.client')
    def test_exists_remote_s3_true(self, mock_boto3, sa):
        """Test exists returns True for existing S3 object"""
        remote_path = "s3://bucket/file.txt"
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3
        mock_s3.head_object.return_value = {}

        result = sa.exists(remote_path)
        assert result is True

    @patch('boto3.client')
    def test_exists_remote_s3_false(self, mock_boto3, sa):
        """Test exists returns False for nonexistent S3 object"""
        remote_path = "s3://bucket/file.txt"
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3
        from botocore.exceptions import ClientError
        mock_s3.head_object.side_effect = ClientError({'Error': {'Code': '404'}}, 'HeadObject')
        mock_s3.exceptions.ClientError = ClientError

        result = sa.exists(remote_path)
        assert result is False

    @patch('boto3.client')
    def test_delete_remote_s3(self, mock_boto3, sa):
        """Test deleting S3 object"""
        remote_path = "s3://bucket/file.txt"
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3

        sa.delete(remote_path)
        mock_s3.delete_object.assert_called_once()

    @patch('boto3.client')
    def test_mkdir_remote_s3(self, mock_boto3, sa):
        """Test creating S3 directory prefix"""
        remote_path = "s3://bucket/dir"
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {}

        sa.mkdir(remote_path)
        mock_s3.put_object.assert_called_once()

    @patch('boto3.client')
    def test_mkdir_remote_s3_exist_ok_false(self, mock_boto3, sa):
        """Test creating S3 directory with exist_ok=False raises error"""
        remote_path = "s3://bucket/dir"
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {"Contents": [{}]}

        with pytest.raises(FileExistsError):
            sa.mkdir(remote_path, exist_ok=False)

    @patch('boto3.client')
    def test_is_dir_remote_s3_true(self, mock_boto3, sa):
        """Test is_dir returns True for S3 prefix"""
        remote_path = "s3://bucket/dir"
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {"Contents": [{}]}

        result = sa.is_dir(remote_path)
        assert result is True

    @patch('boto3.client')
    def test_is_dir_remote_s3_false(self, mock_boto3, sa):
        """Test is_dir returns False for non-prefix S3 path"""
        remote_path = "s3://bucket/file.txt"
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {}

        result = sa.is_dir(remote_path)
        assert result is False

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

    def test_copy_file_preserves_content(self, sa, tmp_path, sample_df):
        """Test that copy_file preserves DataFrame content correctly"""
        src_file = tmp_path / "src.parquet"
        dst_file = tmp_path / "dst.parquet"

        sa.write_df(sample_df, str(src_file))
        sa.copy_file(str(src_file), str(dst_file))

        df_src = sa.read_df(str(src_file))
        df_dst = sa.read_df(str(dst_file))
        pd.testing.assert_frame_equal(df_src, df_dst)

    def test_copy_file_raw_binary_content(self, sa, tmp_path):
        """Test copy_file_raw with binary content"""
        src_file = tmp_path / "src.bin"
        dst_file = tmp_path / "dst.bin"

        binary_data = b"\x00\x01\x02\x03\x04"
        sa.write_bytes(binary_data, str(src_file))
        sa.copy_file_raw(str(src_file), str(dst_file))

        assert sa.read_bytes(str(dst_file)) == binary_data

    def test_move_file_removes_source(self, sa, tmp_path):
        """Test that move_file removes the source file"""
        src_file = tmp_path / "src.txt"
        dst_file = tmp_path / "dst.txt"

        sa.write_text("content", str(src_file))
        assert sa.exists(str(src_file))

        sa.move_file(str(src_file), str(dst_file))

        assert not sa.exists(str(src_file))
        assert sa.exists(str(dst_file))
        assert sa.read_text(str(dst_file)) == "content"

    def test_rename_file(self, sa, tmp_path):
        """Test renaming a file"""
        old_file = tmp_path / "old.txt"
        new_file = tmp_path / "new.txt"

        sa.write_text("content", str(old_file))
        sa.rename(str(old_file), str(new_file))

        assert not os.path.exists(old_file)
        assert os.path.exists(new_file)
        assert sa.read_text(str(new_file)) == "content"

    @patch('boto3.client')
    def test_rename_remote_s3(self, mock_boto3, sa):
        """Test renaming S3 object"""
        old_path = "s3://bucket/old.txt"
        new_path = "s3://bucket/new.txt"
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3

        sa.rename(old_path, new_path)

        mock_s3.copy_object.assert_called_once()
        mock_s3.delete_object.assert_called_once()

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

    def test_get_size_nonexistent_file_raises_error(self, sa, tmp_path):
        """Test that get_size on nonexistent file raises FileNotFoundError"""
        file_path = tmp_path / "nonexistent.txt"
        with pytest.raises(FileNotFoundError):
            sa.get_size(str(file_path))

    def test_get_size_zero_byte_file(self, sa, tmp_path):
        """Test get_size on empty file"""
        file_path = tmp_path / "empty.txt"
        sa.write_text("", str(file_path))
        assert sa.get_size(str(file_path)) == 0

    def test_stat_local_file_metadata(self, sa, tmp_path):
        """Test stat returns correct metadata for local files"""
        file_path = tmp_path / "file.txt"
        sa.write_text("test content", str(file_path))

        stat_info = sa.stat(str(file_path))

        assert stat_info["size"] == 12
        assert "last_modified" in stat_info
        assert "mode" in stat_info
        assert "uid" in stat_info
        assert "gid" in stat_info

    @patch('boto3.client')
    def test_get_size_remote_s3(self, mock_boto3, sa):
        """Test get_size for S3 object"""
        remote_path = "s3://bucket/file.txt"
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3
        mock_s3.head_object.return_value = {"ContentLength": 1024}

        size = sa.get_size(remote_path)
        assert size == 1024

    @patch('boto3.client')
    def test_stat_remote_s3(self, mock_boto3, sa):
        """Test stat for S3 object"""
        remote_path = "s3://bucket/file.txt"
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3
        from datetime import datetime
        mock_s3.head_object.return_value = {
            "ContentLength": 1024,
            "LastModified": datetime.now(),
            "ContentType": "text/plain",
            "ETag": "abc123",
            "StorageClass": "STANDARD"
        }

        stat_info = sa.stat(remote_path)

        assert stat_info["size"] == 1024
        assert "last_modified" in stat_info
        assert stat_info["content_type"] == "text/plain"
        assert stat_info["etag"] == "abc123"
        assert stat_info["storage_class"] == "STANDARD"

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

    def test_list_files_single_file(self, sa, tmp_path):
        """Test list_files on a single file returns that file"""
        file_path = tmp_path / "file.txt"
        sa.write_text("content", str(file_path))
        files = sa.list_files(str(file_path))
        assert files == [str(file_path)]

    def test_list_files_nested_directories(self, sa, tmp_path):
        """Test list_files recursively lists all files in nested directories"""
        subdir1 = tmp_path / "dir1"
        subdir2 = subdir1 / "dir2"
        subdir2.mkdir(parents=True)

        file1 = tmp_path / "f1.txt"
        file2 = subdir1 / "f2.txt"
        file3 = subdir2 / "f3.txt"

        sa.write_text("1", str(file1))
        sa.write_text("2", str(file2))
        sa.write_text("3", str(file3))

        files = sa.list_files(str(tmp_path))
        assert str(file1) in files
        assert str(file2) in files
        assert str(file3) in files

    def test_list_files_empty_directory(self, sa, tmp_path):
        """Test list_files on empty directory returns empty list"""
        subdir = tmp_path / "empty"
        subdir.mkdir()
        files = sa.list_files(str(subdir))
        assert files == []

    def test_list_files_nonexistent_path_raises_error(self, sa, tmp_path):
        """Test list_files on nonexistent path raises FileNotFoundError"""
        nonexistent = tmp_path / "nonexistent"
        with pytest.raises(FileNotFoundError):
            sa.list_files(str(nonexistent))

    @patch('boto3.client')
    def test_list_files_remote_s3(self, mock_boto3, sa):
        """Test list_files for S3 bucket"""
        remote_path = "s3://bucket/prefix"
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3

        mock_paginator = Mock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {"Contents": [{"Key": "prefix/file1.txt"}, {"Key": "prefix/file2.txt"}]},
            {"Contents": [{"Key": "prefix/file3.txt"}]}
        ]

        files = sa.list_files(remote_path)

        assert "s3://bucket/prefix/file1.txt" in files
        assert "s3://bucket/prefix/file2.txt" in files
        assert "s3://bucket/prefix/file3.txt" in files
        assert len(files) == 3

    @patch('boto3.client')
    def test_list_files_remote_s3_empty(self, mock_boto3, sa):
        """Test list_files for empty S3 prefix"""
        remote_path = "s3://bucket/prefix"
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3

        mock_paginator = Mock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{}]

        files = sa.list_files(remote_path)
        assert files == []

    # -----------------------------
    # download_remote_file / upload_local_file
    # -----------------------------
    def test_download_remote_file_local_to_local(self, sa, tmp_path):
        """Test download_remote_file with local paths (copy operation)"""
        src_file = tmp_path / "source.txt"
        dst_dir = tmp_path / "download"
        dst_file = dst_dir / "downloaded.txt"

        sa.write_text("content", str(src_file))
        sa.download_remote_file(str(src_file), str(dst_file))

        assert dst_file.exists()
        assert sa.read_text(str(dst_file)) == "content"

    def test_download_remote_file_creates_parent_directories(self, sa, tmp_path):
        """Test download_remote_file creates parent directories if they don't exist"""
        src_file = tmp_path / "source.txt"
        dst_file = tmp_path / "nested" / "dir" / "downloaded.txt"

        sa.write_text("content", str(src_file))
        sa.download_remote_file(str(src_file), str(dst_file))

        assert dst_file.exists()

    def test_download_remote_file_nonexistent_raises_error(self, sa, tmp_path):
        """Test download_remote_file with nonexistent source raises FileNotFoundError"""
        src_file = tmp_path / "nonexistent.txt"
        dst_file = tmp_path / "downloaded.txt"

        with pytest.raises(FileNotFoundError):
            sa.download_remote_file(str(src_file), str(dst_file))

    @patch('boto3.client')
    def test_download_remote_file_from_s3(self, mock_boto3, sa, tmp_path):
        """Test download_remote_file from S3"""
        remote_path = "s3://bucket/file.txt"
        local_path = tmp_path / "downloaded.txt"

        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3

        sa.download_remote_file(remote_path, str(local_path))

        mock_s3.download_file.assert_called_once()
        call_args = mock_s3.download_file.call_args[0]
        assert call_args[0] == "bucket"
        assert call_args[1] == "file.txt"
        assert call_args[2] == str(local_path)

    def test_upload_local_file_local_to_local(self, sa, tmp_path):
        """Test upload_local_file with local paths (copy operation)"""
        src_file = tmp_path / "source.txt"
        dst_file = tmp_path / "uploaded.txt"

        sa.write_text("content", str(src_file))
        sa.upload_local_file(str(src_file), str(dst_file))

        assert dst_file.exists()
        assert sa.read_text(str(dst_file)) == "content"

    def test_upload_local_file_creates_parent_directories(self, sa, tmp_path):
        """Test upload_local_file creates parent directories if they don't exist"""
        src_file = tmp_path / "source.txt"
        dst_file = tmp_path / "nested" / "dir" / "uploaded.txt"

        sa.write_text("content", str(src_file))
        sa.upload_local_file(str(src_file), str(dst_file))

        assert dst_file.exists()

    def test_upload_local_file_nonexistent_raises_error(self, sa, tmp_path):
        """Test upload_local_file with nonexistent source raises FileNotFoundError"""
        src_file = tmp_path / "nonexistent.txt"
        dst_file = tmp_path / "uploaded.txt"

        with pytest.raises(FileNotFoundError):
            sa.upload_local_file(str(src_file), str(dst_file))

    @patch('boto3.client')
    def test_upload_local_file_to_s3(self, mock_boto3, sa, tmp_path):
        """Test upload_local_file to S3"""
        local_path = tmp_path / "file.txt"
        remote_path = "s3://bucket/uploaded.txt"

        sa.write_text("content", str(local_path))

        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3

        sa.upload_local_file(str(local_path), remote_path)

        mock_s3.upload_file.assert_called_once()
        call_args = mock_s3.upload_file.call_args[0]
        assert call_args[0] == str(local_path)
        assert call_args[1] == "bucket"
        assert call_args[2] == "uploaded.txt"

    # -----------------------------
    # _get_storage_options
    # -----------------------------
    def test_get_storage_options_local_path(self, sa, tmp_path):
        """Test _get_storage_options for local path returns empty dict"""
        file_path = tmp_path / "file.txt"
        options = sa._get_storage_options(str(file_path))
        assert options == {}

    def test_get_storage_options_remote_path(self, sa):
        """Test _get_storage_options for remote path returns empty dict"""
        remote_path = "s3://bucket/file.txt"
        options = sa._get_storage_options(remote_path)
        assert options == {}

    def test_get_storage_options_http_path(self, sa):
        """Test _get_storage_options for HTTP path"""
        http_path = "https://example.com/file.txt"
        options = sa._get_storage_options(http_path)
        assert options == {}

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

    def test_is_remote_path_s3(self):
        """Test is_remote_path detects S3 paths"""
        assert is_remote_path("s3://bucket/file.txt")
        # assert is_remote_path("s3a://bucket/file.txt")

    def test_is_remote_path_http(self):
        """Test is_remote_path detects HTTP paths"""
        assert is_remote_path("http://example.com/file.txt")
        assert is_remote_path("https://example.com/file.txt")

    def test_is_local_path_absolute(self, tmp_path):
        """Test is_local_path detects absolute local paths"""
        assert is_local_path(str(tmp_path / "file.txt"))
        assert is_local_path("/absolute/path/file.txt")

    def test_is_local_path_relative(self):
        """Test is_local_path detects relative local paths"""
        assert is_local_path("relative/path/file.txt")
        assert is_local_path("./file.txt")
        assert is_local_path("../file.txt")

    # -----------------------------
    # Error handling and edge cases
    # -----------------------------
    def test_read_df_with_exception_logging(self, sa, tmp_path, caplog):
        """Test that read_df logs errors properly"""
        file_path = tmp_path / "nonexistent.csv"
        with pytest.raises(Exception):
            sa.read_df(str(file_path))
        assert "Failed to read file" in caplog.text

    def test_write_df_with_exception_logging(self, sa, tmp_path, sample_df, caplog):
        """Test that write_df logs errors properly"""
        # Try to write to an invalid path
        invalid_path = "/invalid/path/that/does/not/exist/file.csv"
        with pytest.raises(Exception):
            sa.write_df(sample_df, invalid_path)
        assert "Failed to write file" in caplog.text

    def test_read_text_with_exception_logging(self, sa, tmp_path, caplog):
        """Test that read_text logs errors properly"""
        file_path = tmp_path / "nonexistent.txt"
        with pytest.raises(Exception):
            sa.read_text(str(file_path))
        assert "Failed to read text" in caplog.text

    def test_read_bytes_with_exception_logging(self, sa, tmp_path, caplog):
        """Test that read_bytes logs errors properly"""
        file_path = tmp_path / "nonexistent.bin"
        with pytest.raises(Exception):
            sa.read_bytes(str(file_path))
        assert "Failed to read bytes" in caplog.text

    def test_write_text_with_invalid_encoding_raises_error(self, sa, tmp_path):
        """Test that write_text with invalid encoding raises error"""
        file_path = tmp_path / "file.txt"
        with pytest.raises(LookupError):
            sa.write_text("content", str(file_path), encoding="invalid_encoding")

    def test_read_text_with_invalid_encoding_raises_error(self, sa, tmp_path):
        """Test that read_text with invalid encoding raises error"""
        file_path = tmp_path / "file.txt"
        sa.write_text("content", str(file_path))
        with pytest.raises(LookupError):
            sa.read_text(str(file_path), encoding="invalid_encoding")

    @patch('boto3.client')
    def test_download_remote_file_s3_import_error(self, mock_boto3, sa, tmp_path):
        """Test that download_remote_file raises ImportError if boto3 is not available"""
        mock_boto3.side_effect = ImportError("boto3 not installed")
        remote_path = "s3://bucket/file.txt"
        local_path = tmp_path / "downloaded.txt"

        with pytest.raises(ImportError, match="boto3 is required"):
            sa.download_remote_file(remote_path, str(local_path))

    @patch('boto3.client')
    def test_upload_local_file_s3_import_error(self, mock_boto3, sa, tmp_path):
        """Test that upload_local_file raises ImportError if boto3 is not available"""
        mock_boto3.side_effect = ImportError("boto3 not installed")
        local_path = tmp_path / "file.txt"
        sa.write_text("content", str(local_path))
        remote_path = "s3://bucket/uploaded.txt"

        with pytest.raises(ImportError, match="boto3 is required"):
            sa.upload_local_file(str(local_path), remote_path)

    @patch('boto3.client')
    def test_list_files_s3_import_error(self, mock_boto3, sa):
        """Test that list_files raises ImportError if boto3 is not available"""
        mock_boto3.side_effect = ImportError("boto3 not installed")
        remote_path = "s3://bucket/prefix"

        with pytest.raises(ImportError, match="boto3 is required"):
            sa.list_files(remote_path)

    @patch('boto3.client')
    def test_exists_s3_import_error(self, mock_boto3, sa):
        """Test that exists raises ImportError if boto3 is not available"""
        mock_boto3.side_effect = ImportError("boto3 not installed")
        remote_path = "s3://bucket/file.txt"

        with pytest.raises(ImportError, match="boto3 is required"):
            sa.exists(remote_path)

    @patch('boto3.client')
    def test_delete_s3_import_error(self, mock_boto3, sa):
        """Test that delete raises ImportError if boto3 is not available"""
        mock_boto3.side_effect = ImportError("boto3 not installed")
        remote_path = "s3://bucket/file.txt"

        with pytest.raises(ImportError, match="boto3 is required"):
            sa.delete(remote_path)

    @patch('boto3.client')
    def test_get_size_s3_import_error(self, mock_boto3, sa):
        """Test that get_size raises ImportError if boto3 is not available"""
        mock_boto3.side_effect = ImportError("boto3 not installed")
        remote_path = "s3://bucket/file.txt"

        with pytest.raises(ImportError, match="boto3 is required"):
            sa.get_size(remote_path)

    # -----------------------------
    # Singleton instance test
    # -----------------------------
    def test_storage_adapter_singleton(self):
        """Test that storage_adapter singleton is an instance of StorageAdapter"""
        from scripts.core.infrastructure.storage_adapter import storage_adapter
        assert isinstance(storage_adapter, StorageAdapter)

    # -----------------------------
    # Integration tests with multiple formats
    # -----------------------------
    @pytest.mark.parametrize("fmt,content", [
        ("csv", pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})),
        ("parquet", pd.DataFrame({"x": [1.1, 2.2], "y": ["a", "b"]})),
        ("json", pd.DataFrame({"col": [True, False]})),
        ("xlsx", pd.DataFrame({"num": [10, 20], "text": ["hello", "world"]})),
    ])
    def test_round_trip_all_formats(self, sa, tmp_path, fmt, content):
        """Test round-trip write and read for all supported formats"""
        file_path = tmp_path / f"test.{fmt}"
        sa.write_df(content, str(file_path))
        df = sa.read_df(str(file_path))
        pd.testing.assert_frame_equal(df, content)

    def test_large_dataframe_write_read(self, sa, tmp_path):
        """Test handling of large DataFrames"""
        large_df = pd.DataFrame({
            "col1": range(10000),
            "col2": [f"text_{i}" for i in range(10000)],
            "col3": [i * 1.5 for i in range(10000)]
        })
        file_path = tmp_path / "large.parquet"
        sa.write_df(large_df, str(file_path))
        df = sa.read_df(str(file_path))
        pd.testing.assert_frame_equal(df, large_df)

    def test_dataframe_with_special_characters(self, sa, tmp_path):
        """Test DataFrame with special characters in column names and values"""
        special_df = pd.DataFrame({
            "col with spaces": [1, 2],
            "col-with-dashes": ["a", "b"],
            "日本語列": ["テスト", "データ"]
        })
        file_path = tmp_path / "special.csv"
        sa.write_df(special_df, str(file_path))
        df = sa.read_df(str(file_path))
        pd.testing.assert_frame_equal(df, special_df)


    def test_dataframe_with_null_values(self, sa, tmp_path):
        """Test DataFrame with null values"""
        null_df = pd.DataFrame({
            "col1": [1, None, 3],
            "col2": [None, "b", "c"]
        })
        file_path = tmp_path / "null.parquet"
        sa.write_df(null_df, str(file_path))
        df = sa.read_df(str(file_path))
        pd.testing.assert_frame_equal(df, null_df)

    # -----------------------------
    # Encoding edge cases
    # -----------------------------
    def test_text_encoding_utf16(self, sa, tmp_path):
        """Test UTF-16 encoding"""
        file_path = tmp_path / "utf16.txt"
        content = "UTF-16 テスト"
        sa.write_text(content, str(file_path), encoding='utf-16')
        result = sa.read_text(str(file_path), encoding='utf-16')
        assert result == content

    def test_text_encoding_cp932(self, sa, tmp_path):
        """Test CP932 (Windows Japanese) encoding"""
        file_path = tmp_path / "cp932.txt"
        content = "日本語テキスト"
        sa.write_text(content, str(file_path), encoding='cp932')
        result = sa.read_text(str(file_path), encoding='cp932')
        assert result == content

    def test_text_multiline_with_encoding(self, sa, tmp_path):
        """Test multiline text with various encodings"""
        file_path = tmp_path / "multiline.txt"
        content = "行1\n行2\n行3\nこんにちは"
        sa.write_text(content, str(file_path), encoding='utf-8')
        result = sa.read_text(str(file_path), encoding='utf-8')
        assert result == content
        assert result.count('\n') == 3

    def test_text_empty_string(self, sa, tmp_path):
        """Test writing and reading empty string"""
        file_path = tmp_path / "empty.txt"
        sa.write_text("", str(file_path))
        result = sa.read_text(str(file_path))
        assert result == ""

    def test_text_with_bom(self, sa, tmp_path):
        """Test UTF-8 with BOM"""
        file_path = tmp_path / "bom.txt"
        content = "BOM test"
        sa.write_text(content, str(file_path), encoding='utf-8-sig')
        result = sa.read_text(str(file_path), encoding='utf-8-sig')
        assert result == content


if __name__ == "__main__":
    pytest.main([__file__, "-v"])