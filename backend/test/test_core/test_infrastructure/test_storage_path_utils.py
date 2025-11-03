import pytest
from scripts.core.infrastructure.storage_path_utils import (
    get_scheme,
    is_remote_path,
    is_local_path,
    normalize_path,
)

class TestStoragePathUtils:

    @pytest.fixture
    def project_root(self):
        return "/home/user/project"  # Arbitrary project root

    # -----------------------------
    # Tests for get_scheme
    # -----------------------------
    @pytest.mark.parametrize(
        "path,expected_scheme",
        [
            ("s3://bucket/key", "s3"),
            ("http://example.com/file.txt", "http"),
            ("https://example.com/file.txt", "https"),
            ("file:///tmp/foo.txt", "file"),
            ("relative/path/to/file", ""),
            ("", ""),  # Empty string
        ],
    )
    def test_get_scheme(self, path, expected_scheme):
        assert get_scheme(path) == expected_scheme

    # -----------------------------
    # Tests for is_remote_path / is_local_path
    # -----------------------------
    @pytest.mark.parametrize(
        "path,expected",
        [
            ("s3://bucket/key", True),
            ("http://example.com/file.txt", True),
            ("https://example.com/file.txt", True),
            ("file:///tmp/foo.txt", False),
            ("relative/path/to/file", False),
            ("", False),
        ],
    )
    def test_is_remote_path(self, path, expected):
        assert is_remote_path(path) is expected

    @pytest.mark.parametrize(
        "path,expected",
        [
            ("s3://bucket/key", False),
            ("http://example.com/file.txt", False),
            ("https://example.com/file.txt", False),
            ("file:///tmp/foo.txt", True),
            ("relative/path/to/file", True),
            ("", True),
        ],
    )
    def test_is_local_path(self, path, expected):
        assert is_local_path(path) is expected

    # -----------------------------
    # Tests for normalize_path (MC/DC coverage)
    # -----------------------------
    @pytest.mark.parametrize(
        "path,expected",
        [
            ("s3://bucket/key", "s3://bucket/key"),
            ("http://example.com/file.txt", "http://example.com/file.txt"),
            ("https://example.com/file.txt", "https://example.com/file.txt"),
            ("file:///tmp/foo.txt", "/tmp/foo.txt"),
            ("file://data/foo.txt", "/home/user/project/data/foo.txt"),
            ("data/foo.txt", "/home/user/project/data/foo.txt"),
            ("/var/log/file.txt", "/var/log/file.txt"),
            ("file://", "/home/user/project/"),
            ("", "/home/user/project/"),
        ],
    )
    def test_normalize_path_mcdc(self, path, expected, project_root):
        result = normalize_path(path, project_root)
        assert result == expected

    # -----------------------------
    # normalize_path raises ValueError for unsupported schemes
    # -----------------------------
    @pytest.mark.parametrize(
        "path",
        [
            "ftp://some/path",
            "gcs://bucket/key",
            "unknown://file.txt",
        ]
    )
    def test_normalize_path_unknown_scheme(self, path, project_root):
        with pytest.raises(ValueError):
            normalize_path(path, project_root)

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
