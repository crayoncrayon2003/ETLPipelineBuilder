import os
import pytest
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, call
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

    # =========================================================
    # read_df
    # MCDC:
    #   条件A: spark is not None
    #   条件B: file_format (CSV/PARQUET/JSON/JSONL/EXCEL/other)
    # =========================================================

    @pytest.mark.parametrize("fmt", ["csv", "parquet", "json", "jsonl"])
    def test_read_write_df_local(self, sa, tmp_path, sample_df, fmt):
        """A=False(pandas) × B=各フォーマット: ローカル読み書き往復"""
        file_path = tmp_path / f"test.{fmt}"
        sa.write_df(sample_df, str(file_path))
        assert file_path.exists()
        df = sa.read_df(str(file_path))
        pd.testing.assert_frame_equal(df, sample_df)

    def test_read_df_excel(self, sa, tmp_path, sample_df):
        """A=False × B=EXCEL"""
        file_path = tmp_path / "test.xlsx"
        sa.write_df(sample_df, str(file_path))
        df = sa.read_df(str(file_path))
        pd.testing.assert_frame_equal(df, sample_df)

    def test_read_df_unsupported_format_raises(self, sa, tmp_path):
        """A=False × B=other → ValueError"""
        file_path = tmp_path / "test.unsupported"
        file_path.touch()
        with pytest.raises(ValueError, match="not supported"):
            sa.read_df(str(file_path))

    def test_read_df_nonexistent_raises(self, sa, tmp_path):
        """A=False: ファイル不在 → 例外"""
        with pytest.raises(Exception):
            sa.read_df(str(tmp_path / "nonexistent.csv"))

    def test_read_df_with_read_options(self, sa, tmp_path, sample_df):
        """A=False × B=CSV: read_options が渡される"""
        file_path = tmp_path / "test.csv"
        sa.write_df(sample_df, str(file_path))
        df = sa.read_df(str(file_path), read_options={"nrows": 1})
        assert len(df) == 1

    def test_read_df_spark_csv(self, sa, tmp_path, sample_df):
        """A=True(spark) × B=CSV"""
        file_path = tmp_path / "test.csv"
        sa.write_df(sample_df, str(file_path))
        mock_spark = Mock()
        sa.read_df(str(file_path), read_options={"spark": mock_spark})
        mock_spark.read.options().csv.assert_called_once()

    def test_read_df_spark_parquet(self, sa, tmp_path, sample_df):
        """A=True × B=PARQUET"""
        file_path = tmp_path / "test.parquet"
        sa.write_df(sample_df, str(file_path))
        mock_spark = Mock()
        sa.read_df(str(file_path), read_options={"spark": mock_spark})
        mock_spark.read.options().parquet.assert_called_once()

    def test_read_df_spark_json(self, sa, tmp_path, sample_df):
        """A=True × B=JSON"""
        file_path = tmp_path / "test.json"
        sa.write_df(sample_df, str(file_path))
        mock_spark = Mock()
        sa.read_df(str(file_path), read_options={"spark": mock_spark})
        mock_spark.read.options().json.assert_called_once()

    def test_read_df_spark_jsonl(self, sa, tmp_path, sample_df):
        """A=True × B=JSONL → JSON分岐に入る"""
        file_path = tmp_path / "test.jsonl"
        sa.write_df(sample_df, str(file_path))
        mock_spark = Mock()
        sa.read_df(str(file_path), read_options={"spark": mock_spark})
        mock_spark.read.options().json.assert_called_once()

    def test_read_df_spark_unsupported_raises(self, sa, tmp_path):
        """A=True × B=other(EXCEL) → ValueError"""
        file_path = tmp_path / "test.xlsx"
        file_path.touch()
        with pytest.raises(ValueError, match="Spark read not supported"):
            sa.read_df(str(file_path), read_options={"spark": Mock()})

    def test_read_df_error_is_logged(self, sa, tmp_path, caplog):
        """例外時にエラーログが出力される"""
        with pytest.raises(Exception):
            sa.read_df(str(tmp_path / "nonexistent.csv"))
        assert "Failed to read file" in caplog.text

    # =========================================================
    # write_df
    # MCDC:
    #   条件A: spark is not None
    #   条件B(spark): file_format
    #   条件C(pandas): is_remote_path(path)  → makedirs スキップ判定
    #   条件D(pandas,local): bool(parent)    → makedirs 空文字ガード
    #   条件E(pandas): file_format
    # =========================================================

    @pytest.mark.parametrize("fmt", ["csv", "parquet", "json", "jsonl"])
    def test_write_df_local_roundtrip(self, sa, tmp_path, sample_df, fmt):
        """A=False × C=False(local) × D=True(parent有) × E=各フォーマット"""
        file_path = tmp_path / f"test.{fmt}"
        sa.write_df(sample_df, str(file_path))
        assert file_path.exists()

    def test_write_df_excel(self, sa, tmp_path, sample_df):
        """A=False × C=False × E=EXCEL"""
        file_path = tmp_path / "test.xlsx"
        sa.write_df(sample_df, str(file_path))
        assert file_path.exists()

    def test_write_df_creates_parent_directories(self, sa, tmp_path, sample_df):
        """A=False × C=False × D=True: 親ディレクトリが自動生成される"""
        file_path = tmp_path / "nested" / "dir" / "test.csv"
        sa.write_df(sample_df, str(file_path))
        assert file_path.exists()

    def test_write_df_remote_path_skips_makedirs(self, sa, sample_df):
        """A=False × C=True(remote): makedirs が呼ばれない
        MCDC: is_remote_path=True の独立した影響を確認"""
        remote_path = "s3://bucket/test.csv"
        with patch("pandas.DataFrame.to_csv") as mock_to_csv, \
             patch("os.makedirs") as mock_makedirs:
            mock_to_csv.return_value = None
            sa.write_df(sample_df, remote_path)
            mock_makedirs.assert_not_called()

    def test_write_df_local_parent_empty_skips_makedirs(self, sa, sample_df):
        """A=False × C=False × D=False(parent空): makedirs がスキップされる
        MCDC: bool(parent)=False の独立した影響を確認"""
        with patch("os.path.dirname", return_value=""), \
             patch("os.makedirs") as mock_makedirs, \
             patch("pandas.DataFrame.to_csv") as mock_to_csv:
            mock_to_csv.return_value = None
            sa.write_df(sample_df, "/test.csv")
            mock_makedirs.assert_not_called()

    def test_write_df_unsupported_format_raises(self, sa, tmp_path, sample_df):
        """A=False × E=other → ValueError"""
        with pytest.raises(ValueError, match="not supported"):
            sa.write_df(sample_df, str(tmp_path / "test.unsupported"))

    def test_write_df_with_write_options(self, sa, tmp_path, sample_df):
        """write_options が渡される"""
        file_path = tmp_path / "test.csv"
        sa.write_df(sample_df, str(file_path), write_options={"sep": ";"})
        assert ";" in sa.read_text(str(file_path))

    def test_write_df_spark_csv(self, sa, tmp_path):
        """A=True × B=CSV"""
        mock_df = Mock()
        mock_df.__len__ = Mock(return_value=10)
        sa.write_df(mock_df, str(tmp_path / "test.csv"), write_options={"spark": Mock()})
        mock_df.write.options().mode().csv.assert_called_once()

    def test_write_df_spark_parquet(self, sa, tmp_path):
        """A=True × B=PARQUET"""
        mock_df = Mock()
        mock_df.__len__ = Mock(return_value=10)
        sa.write_df(mock_df, str(tmp_path / "test.parquet"), write_options={"spark": Mock()})
        mock_df.write.options().mode().parquet.assert_called_once()

    def test_write_df_spark_json(self, sa, tmp_path):
        """A=True × B=JSON"""
        mock_df = Mock()
        mock_df.__len__ = Mock(return_value=10)
        sa.write_df(mock_df, str(tmp_path / "test.json"), write_options={"spark": Mock()})
        mock_df.write.options().mode().json.assert_called_once()

    def test_write_df_spark_jsonl(self, sa, tmp_path):
        """A=True × B=JSONL → JSON分岐に入る"""
        mock_df = Mock()
        mock_df.__len__ = Mock(return_value=10)
        sa.write_df(mock_df, str(tmp_path / "test.jsonl"), write_options={"spark": Mock()})
        mock_df.write.options().mode().json.assert_called_once()

    def test_write_df_spark_unsupported_raises(self, sa, tmp_path):
        """A=True × B=other(EXCEL) → ValueError"""
        mock_df = Mock()
        mock_df.__len__ = Mock(return_value=10)
        with pytest.raises(ValueError, match="Spark write not supported"):
            sa.write_df(mock_df, str(tmp_path / "test.xlsx"), write_options={"spark": Mock()})

    def test_write_df_error_is_logged(self, sa, sample_df, caplog):
        """例外時にエラーログが出力される"""
        with pytest.raises(Exception):
            sa.write_df(sample_df, "/invalid/path/file.csv")
        assert "Failed to write file" in caplog.text

    # =========================================================
    # read_text
    # MCDC:
    #   条件A: is_remote_path(path)
    #   条件B(local): os.path.isfile(normalized_path)
    # =========================================================

    def test_read_text_local_file_exists(self, sa, tmp_path):
        """A=False × B=True: ローカルファイル読み込み成功"""
        file_path = tmp_path / "test.txt"
        file_path.write_text("hello", encoding="utf-8")
        assert sa.read_text(str(file_path)) == "hello"

    def test_read_text_local_file_not_found(self, sa, tmp_path):
        """A=False × B=False: ファイル不在 → FileNotFoundError"""
        with pytest.raises(FileNotFoundError):
            sa.read_text(str(tmp_path / "nonexistent.txt"))

    def test_read_text_s3(self, sa):
        """A=True: S3から読み込み (s3fs遅延import確認)"""
        with patch("s3fs.S3FileSystem") as mock_cls:
            mock_s3 = MagicMock()
            mock_cls.return_value = mock_s3
            mock_s3.open.return_value.__enter__.return_value.read.return_value = "s3 content"
            result = sa.read_text("s3://bucket/file.txt")
            assert result == "s3 content"

    @pytest.mark.parametrize("encoding,content", [
        # 日本語・絵文字を含む文字列 (utf-8/utf-16/utf-8-sig はすべて表現可能)
        ("utf-8",     "テスト Hello 🌍"),
        ("utf-16",    "テスト Hello"),
        ("utf-8-sig", "テスト Hello"),
        # Shift-JIS / CP932 は日本語を含むが絵文字・一部文字は不可
        ("shift_jis", "こんにちは Hello"),
        ("cp932",     "日本語テスト Hello"),
        # latin-1 は ASCII + ラテン拡張のみ (日本語不可)
        ("latin-1",   "Hello Héllo Wörld"),
    ])
    def test_read_write_text_encodings(self, sa, tmp_path, encoding, content):
        """各エンコーディングで表現可能な文字列での往復テスト"""
        file_path = tmp_path / f"test_{encoding.replace('-', '_')}.txt"
        sa.write_text(content, str(file_path), encoding=encoding)
        assert sa.read_text(str(file_path), encoding=encoding) == content

    def test_read_text_wrong_encoding_raises(self, sa, tmp_path):
        """誤ったエンコーディング → UnicodeDecodeError"""
        file_path = tmp_path / "test.txt"
        sa.write_text("こんにちは", str(file_path), encoding="utf-8")
        with pytest.raises(UnicodeDecodeError):
            sa.read_text(str(file_path), encoding="ascii")

    def test_read_text_invalid_encoding_raises(self, sa, tmp_path):
        """不正なエンコーディング名 → LookupError"""
        file_path = tmp_path / "test.txt"
        sa.write_text("hello", str(file_path))
        with pytest.raises(LookupError):
            sa.read_text(str(file_path), encoding="invalid_encoding")

    def test_read_text_error_is_logged(self, sa, tmp_path, caplog):
        """例外時にエラーログが出力される"""
        with pytest.raises(Exception):
            sa.read_text(str(tmp_path / "nonexistent.txt"))
        assert "Failed to read text" in caplog.text

    # =========================================================
    # write_text
    # MCDC:
    #   条件A: is_remote_path(path)
    #   条件B(local): bool(parent)  → makedirs 空文字ガード
    # =========================================================

    def test_write_text_local_creates_parent(self, sa, tmp_path):
        """A=False × B=True(parent有): 親ディレクトリを自動生成"""
        file_path = tmp_path / "nested" / "dir" / "test.txt"
        sa.write_text("content", str(file_path))
        assert file_path.exists()

    def test_write_text_local_parent_empty_skips_makedirs(self, sa):
        """A=False × B=False(parent空): makedirs がスキップされる
        MCDC: bool(parent)=False の独立した影響を確認"""
        with patch("os.path.dirname", return_value=""), \
             patch("os.makedirs") as mock_makedirs, \
             patch("builtins.open", MagicMock()):
            sa.write_text("content", "/test.txt")
            mock_makedirs.assert_not_called()

    def test_write_text_s3(self, sa):
        """A=True: S3へ書き込み (s3fs遅延import確認)"""
        with patch("s3fs.S3FileSystem") as mock_cls:
            mock_s3 = MagicMock()
            mock_cls.return_value = mock_s3
            mock_file = MagicMock()
            mock_s3.open.return_value.__enter__.return_value = mock_file
            sa.write_text("content", "s3://bucket/file.txt")
            mock_file.write.assert_called_once_with("content")

    def test_write_text_invalid_encoding_raises(self, sa, tmp_path):
        """不正なエンコーディング名 → LookupError"""
        with pytest.raises(LookupError):
            sa.write_text("hello", str(tmp_path / "test.txt"), encoding="invalid_encoding")

    def test_write_text_empty_string(self, sa, tmp_path):
        """空文字列の書き込み・読み込み"""
        file_path = tmp_path / "empty.txt"
        sa.write_text("", str(file_path))
        assert sa.read_text(str(file_path)) == ""

    # =========================================================
    # read_bytes
    # MCDC:
    #   条件A: is_remote_path(path)
    #   条件B(local): os.path.isfile(normalized_path)
    # =========================================================

    def test_read_bytes_local_file_exists(self, sa, tmp_path):
        """A=False × B=True: ローカルバイト読み込み成功"""
        file_path = tmp_path / "test.bin"
        file_path.write_bytes(b"\x00\x01\x02")
        assert sa.read_bytes(str(file_path)) == b"\x00\x01\x02"

    def test_read_bytes_local_file_not_found(self, sa, tmp_path):
        """A=False × B=False: ファイル不在 → FileNotFoundError"""
        with pytest.raises(FileNotFoundError):
            sa.read_bytes(str(tmp_path / "nonexistent.bin"))

    def test_read_bytes_s3(self, sa):
        """A=True: S3からバイト読み込み (s3fs遅延import確認)"""
        with patch("s3fs.S3FileSystem") as mock_cls:
            mock_s3 = MagicMock()
            mock_cls.return_value = mock_s3
            mock_s3.open.return_value.__enter__.return_value.read.return_value = b"\x00\x01"
            assert sa.read_bytes("s3://bucket/file.bin") == b"\x00\x01"

    def test_read_bytes_error_is_logged(self, sa, tmp_path, caplog):
        """例外時にエラーログが出力される"""
        with pytest.raises(Exception):
            sa.read_bytes(str(tmp_path / "nonexistent.bin"))
        assert "Failed to read bytes" in caplog.text

    # =========================================================
    # write_bytes
    # MCDC:
    #   条件A: is_remote_path(path)
    #   条件B(local): bool(parent)  → makedirs 空文字ガード
    # =========================================================

    def test_write_bytes_local_creates_parent(self, sa, tmp_path):
        """A=False × B=True(parent有): 親ディレクトリを自動生成"""
        file_path = tmp_path / "nested" / "dir" / "test.bin"
        sa.write_bytes(b"\x00\x01", str(file_path))
        assert file_path.exists()

    def test_write_bytes_local_parent_empty_skips_makedirs(self, sa):
        """A=False × B=False(parent空): makedirs がスキップされる
        MCDC: bool(parent)=False の独立した影響を確認"""
        with patch("os.path.dirname", return_value=""), \
             patch("os.makedirs") as mock_makedirs, \
             patch("builtins.open", MagicMock()):
            sa.write_bytes(b"\x00", "/test.bin")
            mock_makedirs.assert_not_called()

    def test_write_bytes_s3(self, sa):
        """A=True: S3へバイト書き込み (s3fs遅延import確認)"""
        with patch("s3fs.S3FileSystem") as mock_cls:
            mock_s3 = MagicMock()
            mock_cls.return_value = mock_s3
            mock_file = MagicMock()
            mock_s3.open.return_value.__enter__.return_value = mock_file
            sa.write_bytes(b"\x00\x01", "s3://bucket/file.bin")
            mock_file.write.assert_called_once_with(b"\x00\x01")

    def test_write_read_bytes_roundtrip(self, sa, tmp_path):
        """バイトの往復テスト"""
        file_path = tmp_path / "test.bin"
        data = b"\x00\x01\x02\x03"
        sa.write_bytes(data, str(file_path))
        assert sa.read_bytes(str(file_path)) == data

    def test_write_bytes_empty(self, sa, tmp_path):
        """空バイト列の書き込み"""
        file_path = tmp_path / "empty.bin"
        sa.write_bytes(b"", str(file_path))
        assert sa.get_size(str(file_path)) == 0

    # =========================================================
    # download_remote_file
    # MCDC:
    #   条件A: is_remote_path(remote_path)
    #   条件B(local): os.path.isfile(normalized_remote_path)
    #   条件C: bool(parent)  → makedirs 空文字ガード
    # =========================================================

    def test_download_local_to_local_file_exists(self, sa, tmp_path):
        """A=False × B=True × C=True: ローカルからローカルへコピー"""
        src = tmp_path / "source.txt"
        dst = tmp_path / "download" / "dest.txt"
        src.write_text("content")
        sa.download_remote_file(str(src), str(dst))
        assert dst.read_text() == "content"

    def test_download_local_to_local_file_not_found(self, sa, tmp_path):
        """A=False × B=False: ソース不在 → FileNotFoundError"""
        with pytest.raises(FileNotFoundError):
            sa.download_remote_file(str(tmp_path / "nonexistent.txt"), str(tmp_path / "dst.txt"))

    @patch("boto3.client")
    def test_download_s3_to_local(self, mock_boto3, sa, tmp_path):
        """A=True × C=True: S3からローカルへダウンロード"""
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3
        local_path = tmp_path / "downloaded.txt"
        sa.download_remote_file("s3://bucket/file.txt", str(local_path))
        args = mock_s3.download_file.call_args[0]
        assert args[0] == "bucket"
        assert args[1] == "file.txt"
        assert args[2] == str(local_path)

    def test_download_parent_empty_skips_makedirs(self, sa, tmp_path):
        """C=False(parent空): makedirs がスキップされる
        MCDC: bool(parent)=False の独立した影響を確認
        os.path.abspath を経由するため parent が空になるのはルートパスのみ"""
        src = tmp_path / "source.txt"
        src.write_text("content")
        with patch("os.path.dirname", return_value=""), \
             patch("os.makedirs") as mock_makedirs, \
             patch("shutil.copy"):
            sa.download_remote_file(str(src), "/dst.txt")
            mock_makedirs.assert_not_called()

    @patch("boto3.client")
    def test_download_s3_exception_propagates(self, mock_boto3, sa, tmp_path):
        """S3例外が伝播する"""
        mock_boto3.return_value.download_file.side_effect = Exception("S3 error")
        with pytest.raises(Exception):
            sa.download_remote_file("s3://bucket/file.txt", str(tmp_path / "dst.txt"))

    # =========================================================
    # upload_local_file
    # MCDC:
    #   条件A: is_remote_path(remote_path)
    #   条件B(S3): normalized_remote_path.endswith("/")  → ファイル名追記
    #   条件C(local): bool(parent)  → makedirs 空文字ガード
    # =========================================================

    def test_upload_local_to_local(self, sa, tmp_path):
        """A=False × C=True: ローカルからローカルへコピー"""
        src = tmp_path / "source.txt"
        dst = tmp_path / "dest.txt"
        src.write_text("content")
        sa.upload_local_file(str(src), str(dst))
        assert dst.read_text() == "content"

    def test_upload_local_creates_parent(self, sa, tmp_path):
        """A=False × C=True: 親ディレクトリを自動生成"""
        src = tmp_path / "source.txt"
        dst = tmp_path / "nested" / "dir" / "dest.txt"
        src.write_text("content")
        sa.upload_local_file(str(src), str(dst))
        assert dst.exists()

    def test_upload_local_parent_empty_skips_makedirs(self, sa, tmp_path):
        """A=False × C=False(parent空): makedirs がスキップされる
        MCDC: bool(parent)=False の独立した影響を確認"""
        src = tmp_path / "source.txt"
        src.write_text("content")
        with patch("os.path.dirname", return_value=""), \
             patch("os.makedirs") as mock_makedirs, \
             patch("shutil.copy"):
            sa.upload_local_file(str(src), "/dst.txt")
            mock_makedirs.assert_not_called()

    def test_upload_nonexistent_source_raises(self, sa, tmp_path):
        """ソースファイル不在 → FileNotFoundError"""
        with pytest.raises(FileNotFoundError):
            sa.upload_local_file(str(tmp_path / "nonexistent.txt"), str(tmp_path / "dst.txt"))

    @patch("boto3.client")
    def test_upload_to_s3_without_trailing_slash(self, mock_boto3, sa, tmp_path):
        """A=True × B=False(スラッシュなし): パスをそのまま使用してupload"""
        src = tmp_path / "file.txt"
        src.write_text("content")
        mock_boto3.return_value.upload_file = Mock()
        sa.upload_local_file(str(src), "s3://bucket/uploaded.txt")
        args = mock_boto3.return_value.upload_file.call_args[0]
        assert args[1] == "bucket"
        assert args[2] == "uploaded.txt"

    @patch("boto3.client")
    def test_upload_to_s3_with_trailing_slash(self, mock_boto3, sa, tmp_path):
        """A=True × B=True(スラッシュあり): ファイル名を末尾に追記してupload
        MCDC: endswith("/")=True の独立した影響を確認"""
        src = tmp_path / "myfile.txt"
        src.write_text("content")
        mock_boto3.return_value.upload_file = Mock()
        sa.upload_local_file(str(src), "s3://bucket/some/prefix/")
        args = mock_boto3.return_value.upload_file.call_args[0]
        assert args[1] == "bucket"
        assert args[2].endswith("myfile.txt")
        assert args[2] != "myfile.txt"  # prefixが付いている

    @patch("boto3.client")
    def test_upload_s3_exception_propagates(self, mock_boto3, sa, tmp_path):
        """S3例外が伝播する"""
        src = tmp_path / "file.txt"
        src.write_text("content")
        mock_boto3.return_value.upload_file.side_effect = Exception("S3 error")
        with pytest.raises(Exception):
            sa.upload_local_file(str(src), "s3://bucket/file.txt")

    # =========================================================
    # exists
    # MCDC:
    #   条件A: is_remote_path(path)
    #   条件B(S3): head_object が ClientError を raise するか
    # =========================================================

    def test_exists_local_true(self, sa, tmp_path):
        """A=False: 存在するローカルファイル → True"""
        file_path = tmp_path / "file.txt"
        file_path.touch()
        assert sa.exists(str(file_path)) is True

    def test_exists_local_false(self, sa, tmp_path):
        """A=False: 存在しないパス → False"""
        assert sa.exists(str(tmp_path / "nonexistent.txt")) is False

    @patch("boto3.client")
    def test_exists_s3_true(self, mock_boto3, sa):
        """A=True × B=True(head_object成功) → True"""
        mock_boto3.return_value.head_object.return_value = {}
        assert sa.exists("s3://bucket/file.txt") is True

    @patch("boto3.client")
    def test_exists_s3_false(self, mock_boto3, sa):
        """A=True × B=False(ClientError) → False
        MCDC: ClientError の独立した影響を確認"""
        from botocore.exceptions import ClientError
        mock_boto3.return_value.head_object.side_effect = ClientError(
            {"Error": {"Code": "404"}}, "HeadObject"
        )
        assert sa.exists("s3://bucket/nonexistent.txt") is False

    # =========================================================
    # delete
    # MCDC:
    #   条件A: is_remote_path(path)
    #   条件B(local): os.path.isfile(normalized_path)
    # =========================================================

    def test_delete_local_file_exists(self, sa, tmp_path):
        """A=False × B=True: ローカルファイル削除成功"""
        file_path = tmp_path / "file.txt"
        file_path.touch()
        sa.delete(str(file_path))
        assert not file_path.exists()

    def test_delete_local_file_not_found(self, sa, tmp_path):
        """A=False × B=False: ファイル不在 → FileNotFoundError"""
        with pytest.raises(FileNotFoundError):
            sa.delete(str(tmp_path / "nonexistent.txt"))

    @patch("boto3.client")
    def test_delete_s3(self, mock_boto3, sa):
        """A=True: S3オブジェクト削除"""
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3
        sa.delete("s3://bucket/file.txt")
        mock_s3.delete_object.assert_called_once()

    # =========================================================
    # get_size
    # =========================================================

    def test_get_size_local(self, sa, tmp_path):
        """ローカルファイルのサイズ取得"""
        file_path = tmp_path / "file.txt"
        file_path.write_text("abc")
        assert sa.get_size(str(file_path)) == 3

    def test_get_size_local_zero(self, sa, tmp_path):
        """空ファイルのサイズ = 0"""
        file_path = tmp_path / "empty.txt"
        file_path.write_text("")
        assert sa.get_size(str(file_path)) == 0

    def test_get_size_local_not_found(self, sa, tmp_path):
        """ローカルファイル不在 → FileNotFoundError"""
        with pytest.raises(FileNotFoundError):
            sa.get_size(str(tmp_path / "nonexistent.txt"))

    @patch("boto3.client")
    def test_get_size_s3(self, mock_boto3, sa):
        """S3オブジェクトのサイズ取得"""
        mock_boto3.return_value.head_object.return_value = {"ContentLength": 1024}
        assert sa.get_size("s3://bucket/file.txt") == 1024

    # =========================================================
    # list_files
    # MCDC:
    #   条件A: is_remote_path(path)
    #   条件B(local): os.path.isfile / os.path.isdir / それ以外
    # =========================================================

    def test_list_files_local_directory(self, sa, tmp_path):
        """A=False × B=isdir: ディレクトリ内ファイル一覧"""
        (tmp_path / "f1.txt").write_text("1")
        (tmp_path / "f2.txt").write_text("2")
        files = sa.list_files(str(tmp_path))
        assert set(files) == {str(tmp_path / "f1.txt"), str(tmp_path / "f2.txt")}

    def test_list_files_local_single_file(self, sa, tmp_path):
        """A=False × B=isfile: 単一ファイルを返す"""
        file_path = tmp_path / "file.txt"
        file_path.write_text("content")
        assert sa.list_files(str(file_path)) == [str(file_path)]

    def test_list_files_local_nested(self, sa, tmp_path):
        """A=False: ネストされたディレクトリを再帰的に列挙"""
        sub = tmp_path / "sub"
        sub.mkdir()
        (tmp_path / "f1.txt").write_text("1")
        (sub / "f2.txt").write_text("2")
        files = sa.list_files(str(tmp_path))
        assert str(tmp_path / "f1.txt") in files
        assert str(sub / "f2.txt") in files

    def test_list_files_local_empty_directory(self, sa, tmp_path):
        """A=False × B=isdir(空): 空リストを返す"""
        assert sa.list_files(str(tmp_path)) == []

    def test_list_files_local_not_found(self, sa, tmp_path):
        """A=False × B=その他: FileNotFoundError"""
        with pytest.raises(FileNotFoundError):
            sa.list_files(str(tmp_path / "nonexistent"))

    @patch("boto3.client")
    def test_list_files_s3(self, mock_boto3, sa):
        """A=True: S3プレフィックスのファイル一覧"""
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3
        mock_paginator = Mock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {"Contents": [{"Key": "prefix/f1.txt"}, {"Key": "prefix/f2.txt"}]},
            {"Contents": [{"Key": "prefix/f3.txt"}]},
        ]
        files = sa.list_files("s3://bucket/prefix")
        assert set(files) == {
            "s3://bucket/prefix/f1.txt",
            "s3://bucket/prefix/f2.txt",
            "s3://bucket/prefix/f3.txt",
        }

    @patch("boto3.client")
    def test_list_files_s3_empty(self, mock_boto3, sa):
        """A=True: S3プレフィックスが空 → 空リスト"""
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3
        mock_paginator = Mock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{}]
        assert sa.list_files("s3://bucket/prefix") == []

    # =========================================================
    # mkdir
    # MCDC:
    #   条件A: is_remote_path(path)
    #   条件B(S3): not exist_ok
    #   条件C(S3, exist_ok=False): "Contents" in response
    # =========================================================

    def test_mkdir_local(self, sa, tmp_path):
        """A=False: ローカルディレクトリ作成"""
        dir_path = tmp_path / "newdir"
        sa.mkdir(str(dir_path))
        assert dir_path.is_dir()

    def test_mkdir_local_exist_ok_true(self, sa, tmp_path):
        """A=False × B=False(exist_ok=True): 既存ディレクトリでもエラーなし"""
        dir_path = tmp_path / "newdir"
        sa.mkdir(str(dir_path))
        sa.mkdir(str(dir_path), exist_ok=True)  # 2回目はエラーにならない

    def test_mkdir_local_exist_ok_false(self, sa, tmp_path):
        """A=False × B=True(exist_ok=False): 既存ディレクトリで FileExistsError"""
        dir_path = tmp_path / "newdir"
        sa.mkdir(str(dir_path))
        with pytest.raises(FileExistsError):
            sa.mkdir(str(dir_path), exist_ok=False)

    @patch("boto3.client")
    def test_mkdir_s3_exist_ok_true(self, mock_boto3, sa):
        """A=True × B=False(exist_ok=True): チェックなしで put_object"""
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3
        sa.mkdir("s3://bucket/dir", exist_ok=True)
        mock_s3.put_object.assert_called_once()
        mock_s3.list_objects_v2.assert_not_called()

    @patch("boto3.client")
    def test_mkdir_s3_exist_ok_false_not_exists(self, mock_boto3, sa):
        """A=True × B=True × C=False: S3プレフィックス未存在 → put_object"""
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {}
        sa.mkdir("s3://bucket/dir", exist_ok=False)
        mock_s3.put_object.assert_called_once()

    @patch("boto3.client")
    def test_mkdir_s3_exist_ok_false_already_exists(self, mock_boto3, sa):
        """A=True × B=True × C=True: S3プレフィックス既存 → FileExistsError
        MCDC: Contents in response の独立した影響を確認"""
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {"Contents": [{}]}
        with pytest.raises(FileExistsError):
            sa.mkdir("s3://bucket/dir", exist_ok=False)

    # =========================================================
    # is_dir
    # MCDC:
    #   条件A: is_remote_path(path)
    #   条件B(S3): not key  (空キー)
    #   条件C(S3): key.endswith("/")
    #   条件D(local): os.path.isdir
    #   条件E(local): ext == ""
    # =========================================================

    def test_is_dir_local_existing_directory(self, sa, tmp_path):
        """A=False × D=True: 実在するディレクトリ → True"""
        assert sa.is_dir(str(tmp_path)) is True

    def test_is_dir_local_file_with_extension(self, sa, tmp_path):
        """A=False × D=False × E=False: 拡張子ありファイル → False"""
        file_path = tmp_path / "file.txt"
        file_path.touch()
        assert sa.is_dir(str(file_path)) is False

    def test_is_dir_local_nonexistent_without_extension(self, sa, tmp_path):
        """A=False × D=False × E=True: 拡張子なし非実在パス → True (現仕様)"""
        assert sa.is_dir(str(tmp_path / "nonexistent")) is True

    def test_is_dir_local_nonexistent_with_extension(self, sa, tmp_path):
        """A=False × D=False × E=False: 拡張子あり非実在パス → False"""
        assert sa.is_dir(str(tmp_path / "nonexistent.txt")) is False

    @patch("boto3.client")
    def test_is_dir_s3_empty_key(self, mock_boto3, sa):
        """A=True × B=True(not key): バケットルート → True (list_objects_v2を呼ばない)
        MCDC: not key の独立した影響を確認"""
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3
        # s3://bucket/ → key=""
        result = sa.is_dir("s3://bucket/")
        assert result is True
        mock_s3.list_objects_v2.assert_not_called()

    @patch("boto3.client")
    def test_is_dir_s3_key_ends_with_slash(self, mock_boto3, sa):
        """A=True × B=False × C=True(endswith "/"): スラッシュ終わりキー → True (list_objects_v2を呼ばない)
        MCDC: key.endswith("/") の独立した影響を確認"""
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3
        # s3://bucket/prefix/ → key="prefix/"
        result = sa.is_dir("s3://bucket/prefix/")
        assert result is True
        mock_s3.list_objects_v2.assert_not_called()

    @patch("boto3.client")
    def test_is_dir_s3_prefix_with_contents(self, mock_boto3, sa):
        """A=True × B=False × C=False: 通常キー → list_objects_v2 で判定 → True"""
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {"Contents": [{}]}
        assert sa.is_dir("s3://bucket/dir") is True

    @patch("boto3.client")
    def test_is_dir_s3_no_contents(self, mock_boto3, sa):
        """A=True × B=False × C=False: 通常キー → list_objects_v2 で判定 → False"""
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {}
        assert sa.is_dir("s3://bucket/file.txt") is False

    # =========================================================
    # rename
    # =========================================================

    def test_rename_local(self, sa, tmp_path):
        """ローカルファイルのリネーム"""
        old = tmp_path / "old.txt"
        new = tmp_path / "new.txt"
        old.write_text("content")
        sa.rename(str(old), str(new))
        assert not old.exists()
        assert new.read_text() == "content"

    @patch("boto3.client")
    def test_rename_s3(self, mock_boto3, sa):
        """S3オブジェクトのリネーム: copy + delete"""
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3
        sa.rename("s3://bucket/old.txt", "s3://bucket/new.txt")
        mock_s3.copy_object.assert_called_once()
        mock_s3.delete_object.assert_called_once()

    # =========================================================
    # stat
    # MCDC:
    #   条件A: is_remote_path(path)
    #    last_modified が常に timezone-aware (UTC) であること
    # =========================================================

    def test_stat_local_returns_aware_datetime(self, sa, tmp_path):
        """A=False: last_modified が timezone-aware (UTC) であることを確認"""
        file_path = tmp_path / "file.txt"
        file_path.write_text("test")
        stat_info = sa.stat(str(file_path))
        assert stat_info["size"] == 4
        assert stat_info["last_modified"].tzinfo is not None
        assert stat_info["last_modified"].tzinfo == timezone.utc
        assert "mode" in stat_info
        assert "uid" in stat_info
        assert "gid" in stat_info

    @patch("boto3.client")
    def test_stat_s3_returns_aware_datetime(self, mock_boto3, sa):
        """A=True: S3の last_modified も timezone-aware であることを確認"""
        s3_dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        mock_boto3.return_value.head_object.return_value = {
            "ContentLength": 1024,
            "LastModified": s3_dt,
            "ContentType": "text/plain",
            "ETag": "abc123",
            "StorageClass": "STANDARD",
        }
        stat_info = sa.stat("s3://bucket/file.txt")
        assert stat_info["size"] == 1024
        assert stat_info["last_modified"].tzinfo is not None

    def test_stat_local_and_s3_last_modified_comparable(self, sa, tmp_path):
        """ローカルとS3の last_modified が型として比較可能であることを確認"""
        file_path = tmp_path / "file.txt"
        file_path.write_text("test")
        local_stat = sa.stat(str(file_path))
        s3_dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        # TypeError が発生しないことを確認
        result = local_stat["last_modified"] > s3_dt
        assert isinstance(result, bool)

    # =========================================================
    # copy_file / copy_file_raw / move_file (複合操作)
    # =========================================================

    def test_copy_file(self, sa, tmp_path, sample_df):
        """DataFrameのコピー"""
        src = tmp_path / "src.parquet"
        dst = tmp_path / "dst.parquet"
        sa.write_df(sample_df, str(src))
        sa.copy_file(str(src), str(dst))
        pd.testing.assert_frame_equal(sa.read_df(str(src)), sa.read_df(str(dst)))

    def test_copy_file_raw(self, sa, tmp_path):
        """バイナリコピー"""
        src = tmp_path / "src.bin"
        dst = tmp_path / "dst.bin"
        sa.write_bytes(b"\x00\x01\x02", str(src))
        sa.copy_file_raw(str(src), str(dst))
        assert sa.read_bytes(str(dst)) == b"\x00\x01\x02"

    def test_move_file(self, sa, tmp_path):
        """移動: ソースが削除されデスティネーションに内容が移る"""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        sa.write_text("content", str(src))
        sa.move_file(str(src), str(dst))
        assert not src.exists()
        assert sa.read_text(str(dst)) == "content"

    # =========================================================
    # _get_storage_options
    # =========================================================

    def test_get_storage_options_local(self, sa, tmp_path):
        """ローカルパス → 空dict"""
        assert sa._get_storage_options(str(tmp_path / "file.txt")) == {}

    def test_get_storage_options_s3(self, sa):
        """S3パス → 空dict"""
        assert sa._get_storage_options("s3://bucket/file.txt") == {}

    # =========================================================
    # 遅延import の動作確認
    # =========================================================

    def test_s3fs_is_not_imported_at_module_level(self):
        """s3fsがモジュールレベルでimportされていないことを確認
        Windows環境でs3fs未インストール時のクラッシュ防止"""
        import scripts.core.infrastructure.storage_adapter as mod
        assert "s3fs" not in dir(mod) or not hasattr(mod, "s3fs")

    def test_botocore_is_not_imported_at_module_level(self):
        """botocore.exceptionsがモジュールレベルでimportされていないことを確認
        Windows環境でboto3未インストール時のクラッシュ防止"""
        import scripts.core.infrastructure.storage_adapter as mod
        assert not hasattr(mod, "botocore")

    def test_s3fs_lazy_import_in_read_text(self, sa):
        """read_textのS3分岐でs3fsが遅延importされる"""
        with patch("s3fs.S3FileSystem") as mock_cls:
            mock_s3 = MagicMock()
            mock_cls.return_value = mock_s3
            mock_s3.open.return_value.__enter__.return_value.read.return_value = ""
            sa.read_text("s3://bucket/file.txt")
            mock_cls.assert_called_once()

    def test_s3fs_lazy_import_in_write_text(self, sa):
        """write_textのS3分岐でs3fsが遅延importされる"""
        with patch("s3fs.S3FileSystem") as mock_cls:
            mock_s3 = MagicMock()
            mock_cls.return_value = mock_s3
            mock_s3.open.return_value.__enter__.return_value = MagicMock()
            sa.write_text("content", "s3://bucket/file.txt")
            mock_cls.assert_called_once()

    def test_s3fs_lazy_import_in_read_bytes(self, sa):
        """read_bytesのS3分岐でs3fsが遅延importされる"""
        with patch("s3fs.S3FileSystem") as mock_cls:
            mock_s3 = MagicMock()
            mock_cls.return_value = mock_s3
            mock_s3.open.return_value.__enter__.return_value.read.return_value = b""
            sa.read_bytes("s3://bucket/file.bin")
            mock_cls.assert_called_once()

    def test_s3fs_lazy_import_in_write_bytes(self, sa):
        """write_bytesのS3分岐でs3fsが遅延importされる"""
        with patch("s3fs.S3FileSystem") as mock_cls:
            mock_s3 = MagicMock()
            mock_cls.return_value = mock_s3
            mock_s3.open.return_value.__enter__.return_value = MagicMock()
            sa.write_bytes(b"\x00", "s3://bucket/file.bin")
            mock_cls.assert_called_once()

    # =========================================================
    # Singleton
    # =========================================================

    def test_storage_adapter_singleton(self):
        """シングルトンインスタンスが StorageAdapter であること"""
        assert isinstance(storage_adapter, StorageAdapter)

    # =========================================================
    # Integration: 各フォーマットの往復テスト
    # =========================================================

    @pytest.mark.parametrize("fmt,content", [
        ("csv",     pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})),
        ("parquet", pd.DataFrame({"x": [1.1, 2.2], "y": ["a", "b"]})),
        ("json",    pd.DataFrame({"col": [True, False]})),
        ("jsonl",   pd.DataFrame({"col": [True, False]})),
        ("xlsx",    pd.DataFrame({"num": [10, 20], "text": ["hello", "world"]})),
    ])
    def test_round_trip_all_formats(self, sa, tmp_path, fmt, content):
        """全フォーマットの書き込み→読み込み往復"""
        file_path = tmp_path / f"test.{fmt}"
        sa.write_df(content, str(file_path))
        df = sa.read_df(str(file_path))
        pd.testing.assert_frame_equal(df, content)

    def test_dataframe_with_null_values(self, sa, tmp_path):
        """null値を含むDataFrameの往復"""
        null_df = pd.DataFrame({"col1": [1, None, 3], "col2": [None, "b", "c"]})
        file_path = tmp_path / "null.parquet"
        sa.write_df(null_df, str(file_path))
        pd.testing.assert_frame_equal(sa.read_df(str(file_path)), null_df)

    def test_large_dataframe(self, sa, tmp_path):
        """大きなDataFrameの往復"""
        large_df = pd.DataFrame({
            "col1": range(10000),
            "col2": [f"text_{i}" for i in range(10000)],
        })
        file_path = tmp_path / "large.parquet"
        sa.write_df(large_df, str(file_path))
        pd.testing.assert_frame_equal(sa.read_df(str(file_path)), large_df)

    def test_empty_dataframe_with_columns(self, sa, tmp_path):
        """カラムはあるが行がないDataFrame"""
        df = pd.DataFrame(columns=["col1", "col2"])
        file_path = tmp_path / "empty.csv"
        sa.write_df(df, str(file_path))
        result = sa.read_df(str(file_path))
        assert list(result.columns) == ["col1", "col2"]
        assert len(result) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])