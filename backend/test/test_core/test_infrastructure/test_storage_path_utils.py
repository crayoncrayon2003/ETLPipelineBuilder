import pytest
from core.infrastructure.storage_path_utils import (
    get_scheme,
    is_remote_path,
    is_local_path,
    normalize_path,
    parse_s3_path,
)


class TestGetScheme:
    """
    get_scheme の MCDC分析:
        条件A: len(scheme) == 1
        条件B: scheme.isalpha()
        → A=True  AND B=True  → "" を返す  (Windowsドライブレター対策)
        → A=False OR  B=False → scheme をそのまま返す

    必要なMCDCケース:
        1. A=True,  B=True  → ""    (例: "C:/path" → scheme="c")
        2. A=False, B=True  → scheme (例: "s3://..." → scheme="s3")
        3. A=True,  B=False → scheme (urlparseの実装上 scheme は英字のみ返すため
                                       現実的なパスでは発生しない。注記として記載)
    """

    def test_windows_drive_letter_c(self):
        """A=True, B=True: Cドライブ → '' (ローカルパスとして扱う)"""
        assert get_scheme("C:/Users/foo/file.txt") == ""

    def test_windows_drive_letter_d(self):
        """A=True, B=True: Dドライブ → '' (大文字)"""
        assert get_scheme("D:/data/file.txt") == ""

    def test_windows_drive_letter_backslash(self):
        """A=True, B=True: バックスラッシュ区切り → ''"""
        assert get_scheme("C:\\Users\\foo\\file.txt") == ""

    def test_s3_scheme(self):
        """A=False, B=True: s3 (len=2 > 1) → 's3'"""
        assert get_scheme("s3://bucket/key") == "s3"

    def test_http_scheme(self):
        """A=False, B=True: http (len=4) → 'http'"""
        assert get_scheme("http://example.com/file.txt") == "http"

    def test_https_scheme(self):
        """A=False, B=True: https (len=5) → 'https'"""
        assert get_scheme("https://example.com/file.txt") == "https"

    def test_file_scheme(self):
        """A=False, B=True: file (len=4) → 'file'"""
        assert get_scheme("file:///tmp/foo.txt") == "file"

    def test_relative_path_no_scheme(self):
        """A=False, B=False: schemeなし (len=0) → ''"""
        assert get_scheme("relative/path/file.txt") == ""

    def test_empty_string(self):
        """A=False, B=False: 空文字 (len=0) → ''"""
        assert get_scheme("") == ""

    def test_dot_slash_path(self):
        """A=False, B=False: ./相対パス → ''"""
        assert get_scheme("./file.txt") == ""

    def test_absolute_unix_path(self):
        """A=False, B=False: Unix絶対パス (schemeなし) → ''"""
        assert get_scheme("/var/log/file.txt") == ""


class TestIsRemotePath:
    """
    is_remote_path の MCDC分析:
        条件: scheme in {"s3", "http", "https"}
        各要素が独立して True に寄与することを確認する。

    必要なMCDCケース:
        1. scheme="s3"    → True
        2. scheme="http"  → True
        3. scheme="https" → True
        4. scheme=""      → False  (ローカル相対パス)
        5. scheme="file"  → False  (ローカルファイルURI)
    """

    def test_s3_is_remote(self):
        """scheme=s3 → True"""
        assert is_remote_path("s3://bucket/key") is True

    def test_http_is_remote(self):
        """scheme=http → True"""
        assert is_remote_path("http://example.com/file.txt") is True

    def test_https_is_remote(self):
        """scheme=https → True"""
        assert is_remote_path("https://example.com/file.txt") is True

    def test_local_relative_is_not_remote(self):
        """scheme="" (相対パス) → False"""
        assert is_remote_path("relative/path/file.txt") is False

    def test_local_absolute_is_not_remote(self):
        """scheme="" (Unix絶対パス) → False"""
        assert is_remote_path("/var/log/file.txt") is False

    def test_file_scheme_is_not_remote(self):
        """scheme=file → False (ファイルURIはローカル扱い)"""
        assert is_remote_path("file:///tmp/foo.txt") is False

    def test_windows_drive_is_not_remote(self):
        """scheme="" (Windowsドライブレター → get_scheme が "" を返す) → False"""
        assert is_remote_path("C:/Users/foo/file.txt") is False

    def test_empty_string_is_not_remote(self):
        """scheme="" (空文字) → False"""
        assert is_remote_path("") is False


class TestIsLocalPath:
    """
    is_local_path の MCDC分析:
        条件: scheme in {"", "file"}
        各要素が独立して True に寄与することを確認する。

    必要なMCDCケース:
        1. scheme=""     → True  (相対パス / Unix絶対パス / Windowsパス)
        2. scheme="file" → True  (ファイルURI)
        3. scheme="s3"   → False
        4. scheme="http" → False
    """

    def test_relative_path_is_local(self):
        """scheme="" (相対パス) → True"""
        assert is_local_path("relative/path/file.txt") is True

    def test_absolute_unix_path_is_local(self):
        """scheme="" (Unix絶対パス) → True"""
        assert is_local_path("/var/log/file.txt") is True

    def test_windows_path_is_local(self):
        """scheme="" (Windowsパス → get_schemeが"" を返す) → True"""
        assert is_local_path("C:/Users/foo/file.txt") is True

    def test_dot_slash_path_is_local(self):
        """scheme="" (./相対) → True"""
        assert is_local_path("./file.txt") is True

    def test_file_uri_is_local(self):
        """scheme=file → True"""
        assert is_local_path("file:///tmp/foo.txt") is True

    def test_file_uri_relative_is_local(self):
        """scheme=file (netloc形式) → True"""
        assert is_local_path("file://data/foo.txt") is True

    def test_s3_is_not_local(self):
        """scheme=s3 → False"""
        assert is_local_path("s3://bucket/key") is False

    def test_http_is_not_local(self):
        """scheme=http → False"""
        assert is_local_path("http://example.com/file.txt") is False

    def test_https_is_not_local(self):
        """scheme=https → False"""
        assert is_local_path("https://example.com/file.txt") is False

    def test_empty_string_is_local(self):
        """scheme="" (空文字) → True"""
        assert is_local_path("") is True


class TestNormalizePath:
    """
    normalize_path の MCDC分析:

    【normalize_path 本体】
        条件A: scheme が SCHEME_NORMALIZERS に存在するか
            A=True  → 対応するノーマライザーに委譲
            A=False → ValueError

    【_normalize_local_path】
        条件B: preserve_trailing_slash  (path=="" or path.endswith("/"))
            B=True  → 末尾スラッシュを保持
            B=False → そのまま
        条件C: _is_absolute_path(path)
            C=True  → project_root と結合しない
            C=False → project_root と結合する
        条件D: re.match(Windowsパターン, path)  (C=True のときのみ評価)
            D=True  → 文字列のまま返す (Path()を使わない)
            D=False → str(Path(path)) で正規化

    【_normalize_file_path】
        条件E: bool(parsed.netloc)
            E=True  → netloc+path を相対パスとして project_root に結合
            E=False → url2pathname(parsed.path) を使用

    【_is_absolute_path】(normalize_path 経由で間接テスト)
        条件F: path.startswith("/")   → Unix絶対パス
        条件G: re.match(Windowsパターン) → Windows絶対パス
        F=True,  G=False → True  ("/foo")
        F=False, G=True  → True  ("C:/foo")
        F=False, G=False → False ("relative")
    """

    @pytest.fixture
    def root(self):
        return "/home/user/project"

    # --------------------------------------------------
    # リモートパス: そのまま返す
    # --------------------------------------------------

    def test_s3_path_passthrough(self, root):
        """s3://... はそのまま返す"""
        assert normalize_path("s3://bucket/key", root) == "s3://bucket/key"

    def test_http_path_passthrough(self, root):
        """http://... はそのまま返す"""
        assert normalize_path("http://example.com/file.txt", root) == "http://example.com/file.txt"

    def test_https_path_passthrough(self, root):
        """https://... はそのまま返す"""
        assert normalize_path("https://example.com/file.txt", root) == "https://example.com/file.txt"

    # --------------------------------------------------
    # file:// スキーム (_normalize_file_path)
    # E=False: netloc 空 → url2pathname(path) を使用
    # --------------------------------------------------

    def test_file_uri_absolute(self, root):
        """E=False: file:///tmp/foo.txt → /tmp/foo.txt (Unix絶対パス)"""
        assert normalize_path("file:///tmp/foo.txt", root) == "/tmp/foo.txt"

    def test_file_uri_empty(self, root):
        """E=False: file:// → project_root/ (path空, B=True でスラッシュ保持)"""
        assert normalize_path("file://", root) == "/home/user/project/"

    # E=True: netloc あり → 相対パスとして結合
    def test_file_uri_with_netloc_relative(self, root):
        """E=True: file://data/foo.txt → project_root/data/foo.txt"""
        assert normalize_path("file://data/foo.txt", root) == "/home/user/project/data/foo.txt"

    def test_file_uri_with_netloc_trailing_slash(self, root):
        """E=True: file://dir/ → project_root/dir/ (B=True でスラッシュ保持)"""
        assert normalize_path("file://dir/", root) == "/home/user/project/dir/"

    # --------------------------------------------------
    # ローカルパス (_normalize_local_path)
    # --------------------------------------------------

    # C=False (相対パス) × B=False → project_root と結合
    def test_local_relative_path(self, root):
        """C=False, B=False: 相対パス → project_root に結合"""
        assert normalize_path("data/foo.txt", root) == "/home/user/project/data/foo.txt"

    def test_local_dot_slash_path(self, root):
        """C=False, B=False: ./相対パス → project_root に結合"""
        assert normalize_path("./data/foo.txt", root) == "/home/user/project/data/foo.txt"

    # C=False, B=True → 末尾スラッシュ保持
    def test_local_empty_path(self, root):
        """C=False, B=True: 空文字 → project_root/"""
        assert normalize_path("", root) == "/home/user/project/"

    def test_local_relative_trailing_slash(self, root):
        """C=False, B=True: 末尾スラッシュあり相対パス → project_root/subdir/"""
        assert normalize_path("subdir/", root) == "/home/user/project/subdir/"

    # C=True, D=False (Unix絶対パス: F=True, G=False)
    def test_local_unix_absolute_path(self, root):
        """C=True, D=False, F=True, G=False: Unix絶対パス → そのまま"""
        assert normalize_path("/var/log/file.txt", root) == "/var/log/file.txt"

    def test_local_unix_absolute_trailing_slash(self, root):
        """C=True, D=False, B=True: Unix絶対パス末尾スラッシュ → 保持"""
        assert normalize_path("/var/log/", root) == "/var/log/"

    # C=True, D=True (Windowsパス: F=False, G=True)
    def test_local_windows_path_forward_slash(self, root):
        """C=True, D=True, F=False, G=True: Windowsパス(/) → 文字列のまま返す"""
        result = normalize_path("C:/Users/foo/file.txt", root)
        assert result == "C:/Users/foo/file.txt"
        assert "project" not in result  # project_root と結合していない

    def test_local_windows_path_backslash(self, root):
        """C=True, D=True: Windowsパス(\\) → 文字列のまま返す"""
        result = normalize_path("C:\\Users\\foo\\file.txt", root)
        assert result == "C:\\Users\\foo\\file.txt"
        assert "project" not in result

    def test_local_windows_other_drive(self, root):
        """C=True, D=True: D ドライブ → 文字列のまま返す"""
        result = normalize_path("D:/data/file.txt", root)
        assert result == "D:/data/file.txt"

    # --------------------------------------------------
    # 未知スキーム → ValueError (A=False)
    # --------------------------------------------------

    @pytest.mark.parametrize("path", [
        "ftp://example.com/file.txt",
        "gcs://bucket/key",
        "unknown://file.txt",
    ])
    def test_unknown_scheme_raises_value_error(self, root, path):
        """A=False: 未知スキーム → ValueError"""
        with pytest.raises(ValueError, match="Unknown scheme"):
            normalize_path(path, root)


class TestParseS3Path:
    """
    parse_s3_path の MCDC分析:
        条件A: parsed.scheme == "s3"
            A=False → ValueError
            A=True  → (bucket, key) を返す

    keyの派生ケース (A=True の内部):
        - key が空   (s3://bucket/)
        - key が単純 (s3://bucket/file.txt)
        - key がネスト (s3://bucket/a/b/c.txt)
        - key が末尾スラッシュ (s3://bucket/prefix/)
    """

    # A=True: 正常系
    def test_simple_key(self):
        """A=True: 通常のバケット+キー"""
        bucket, key = parse_s3_path("s3://bucket/key.txt")
        assert bucket == "bucket"
        assert key == "key.txt"

    def test_nested_key(self):
        """A=True: ネストしたキー"""
        bucket, key = parse_s3_path("s3://bucket/a/b/c.txt")
        assert bucket == "bucket"
        assert key == "a/b/c.txt"

    def test_empty_key(self):
        """A=True: キーが空 (バケットルート)"""
        bucket, key = parse_s3_path("s3://bucket/")
        assert bucket == "bucket"
        assert key == ""

    def test_key_with_trailing_slash(self):
        """A=True: キーが末尾スラッシュ (S3プレフィックス)"""
        bucket, key = parse_s3_path("s3://my-bucket/data/")
        assert bucket == "my-bucket"
        assert key == "data/"

    def test_hyphenated_bucket_name(self):
        """A=True: ハイフンを含むバケット名"""
        bucket, key = parse_s3_path("s3://my-bucket-name/file.parquet")
        assert bucket == "my-bucket-name"
        assert key == "file.parquet"

    # A=False: 異常系
    def test_http_scheme_raises(self):
        """A=False: http:// → ValueError"""
        with pytest.raises(ValueError, match="Not an S3 path"):
            parse_s3_path("http://bucket/key")

    def test_file_scheme_raises(self):
        """A=False: file:// → ValueError"""
        with pytest.raises(ValueError, match="Not an S3 path"):
            parse_s3_path("file:///tmp/foo.txt")

    def test_local_path_raises(self):
        """A=False: ローカルパス → ValueError"""
        with pytest.raises(ValueError, match="Not an S3 path"):
            parse_s3_path("/local/path/file.txt")

    def test_empty_string_raises(self):
        """A=False: 空文字 → ValueError"""
        with pytest.raises(ValueError, match="Not an S3 path"):
            parse_s3_path("")


class TestIsAbsolutePathViaNormalize:
    """
    _is_absolute_path の MCDC分析 (normalize_path 経由で間接的にテスト):
        条件F: path.startswith("/")        → Unix絶対パス
        条件G: re.match(Windowsパターン)  → Windows絶対パス
        → F or G

    必要なMCDCケース:
        1. F=True,  G=False → True  ("/foo/bar")
        2. F=False, G=True  → True  ("C:/foo" or "C:\\foo")
        3. F=False, G=False → False ("relative/foo")
    """

    @pytest.fixture
    def root(self):
        return "/project"

    def test_unix_absolute_not_joined_with_root(self, root):
        """F=True, G=False: Unix絶対パス → project_root と結合しない"""
        result = normalize_path("/var/data/file.txt", root)
        assert result == "/var/data/file.txt"
        assert result.startswith("/var")

    def test_windows_forward_slash_not_joined_with_root(self, root):
        """F=False, G=True: Windowsパス(/) → project_root と結合しない"""
        result = normalize_path("C:/Users/data.txt", root)
        assert "project" not in result
        assert result.startswith("C:")

    def test_windows_backslash_not_joined_with_root(self, root):
        """F=False, G=True: Windowsパス(\\) → project_root と結合しない"""
        result = normalize_path("C:\\Users\\data.txt", root)
        assert "project" not in result

    def test_relative_path_joined_with_root(self, root):
        """F=False, G=False: 相対パス → project_root と結合する"""
        result = normalize_path("data/file.txt", root)
        assert result == "/project/data/file.txt"

    def test_dot_relative_path_joined_with_root(self, root):
        """F=False, G=False: ./相対パス → project_root と結合する"""
        result = normalize_path("./data/file.txt", root)
        assert result == "/project/data/file.txt"


class TestTrailingSlashPreservation:
    """
    末尾スラッシュ保持ロジックの MCDC分析:
        条件B: preserve_trailing_slash (path=="" or path.endswith("/"))
        条件X: result.endswith("/") (Pathによって除去されたか)
        → B=True AND NOT X → スラッシュ付与

    必要なMCDCケース:
        1. B=True,  X=False → 付与する  (空文字, 末尾スラッシュ相対/絶対パス)
        2. B=False, X=False → 付与しない (通常パス)
        3. B=True,  X=True  → 付与しない (すでにスラッシュで終わっている場合。
                                           Path()は末尾スラッシュを除去するため
                                           実際にはX=Trueになる前に付与が走る)
    """

    @pytest.fixture
    def root(self):
        return "/home/user/project"

    def test_empty_path_adds_trailing_slash(self, root):
        """B=True (path==''): project_root/ になる"""
        assert normalize_path("", root) == "/home/user/project/"

    def test_relative_trailing_slash_preserved(self, root):
        """B=True (relative/): 末尾スラッシュを保持"""
        assert normalize_path("data/", root) == "/home/user/project/data/"

    def test_absolute_trailing_slash_preserved(self, root):
        """B=True (/absolute/): 末尾スラッシュを保持"""
        assert normalize_path("/var/log/", root) == "/var/log/"

    def test_file_uri_empty_trailing_slash(self, root):
        """B=True (file://): project_root/ になる"""
        assert normalize_path("file://", root) == "/home/user/project/"

    def test_normal_path_no_trailing_slash(self, root):
        """B=False: 通常パスは末尾スラッシュなし"""
        result = normalize_path("data/file.txt", root)
        assert not result.endswith("/")

    def test_absolute_path_no_trailing_slash(self, root):
        """B=False: Unix絶対パスは末尾スラッシュなし"""
        result = normalize_path("/var/log/file.txt", root)
        assert not result.endswith("/")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])