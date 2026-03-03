import os
import pathlib
import pytest
from core.data_container.formats import SupportedFormats


# ======================================================================
# from_string
#
# MCDC:
#   条件A: _value2member_map_.get(value.lower()) is not None
#     A=True  → メンバーを返す
#     A=False → ValueError を raise
# ======================================================================
class TestFromString:

    # ------------------------------------------------------------------
    # A=True: 全メンバーの網羅
    # (既存は csv/CSV/parquet の3件のみ)
    # ------------------------------------------------------------------
    @pytest.mark.parametrize("input_str, expected", [
        # 小文字
        ("csv",       SupportedFormats.CSV),
        ("parquet",   SupportedFormats.PARQUET),
        ("excel",     SupportedFormats.EXCEL),
        ("json",      SupportedFormats.JSON),
        ("jsonl",     SupportedFormats.JSONL),
        ("xml",       SupportedFormats.XML),
        ("shapefile", SupportedFormats.SHAPEFILE),
        ("geojson",   SupportedFormats.GEOJSON),
        ("gtfs",      SupportedFormats.GTFS),
        ("gtfs-rt",   SupportedFormats.GTFS_RT),   # ハイフン含む value
        ("zip",       SupportedFormats.ZIP),
        ("tar",       SupportedFormats.TAR),
        ("text",      SupportedFormats.TEXT),
        ("binary",    SupportedFormats.BINARY),
        ("unknown",   SupportedFormats.UNKNOWN),
        # 大文字 → value.lower() の正規化を確認
        ("CSV",       SupportedFormats.CSV),
        ("PARQUET",   SupportedFormats.PARQUET),
        ("EXCEL",     SupportedFormats.EXCEL),
        ("JSON",      SupportedFormats.JSON),
        ("JSONL",     SupportedFormats.JSONL),
        ("XML",       SupportedFormats.XML),
        ("SHAPEFILE", SupportedFormats.SHAPEFILE),
        ("GEOJSON",   SupportedFormats.GEOJSON),
        ("GTFS",      SupportedFormats.GTFS),
        ("GTFS-RT",   SupportedFormats.GTFS_RT),   # ハイフン含む大文字
        ("ZIP",       SupportedFormats.ZIP),
        ("TAR",       SupportedFormats.TAR),
        ("TEXT",      SupportedFormats.TEXT),
        ("BINARY",    SupportedFormats.BINARY),
        ("UNKNOWN",   SupportedFormats.UNKNOWN),
        # 混合ケース
        ("Csv",       SupportedFormats.CSV),
        ("GtFs-Rt",   SupportedFormats.GTFS_RT),
    ])
    def test_a_true_valid_values(self, input_str, expected):
        """A=True: 有効な値 → 対応するメンバーを返す"""
        assert SupportedFormats.from_string(input_str) == expected

    # ------------------------------------------------------------------
    # A=False: 無効な値 → ValueError
    # ------------------------------------------------------------------
    @pytest.mark.parametrize("input_str", [
        "unknown_format",   # 既存
        "",                 # 空文字
        "tsv",              # 類似するが未定義
        "xlsx",             # 拡張子名 (value は "excel")
        "shp",              # 拡張子名 (value は "shapefile")
        " csv",             # 前後スペース
    ])
    def test_a_false_invalid_values_raise_value_error(self, input_str):
        """A=False: 無効な値 → ValueError を raise する"""
        with pytest.raises(ValueError):
            SupportedFormats.from_string(input_str)


# ======================================================================
# from_path
#
# MCDC 条件:
#   B: suffix in ('gz', 'bz2', 'xz')
#   C: inner_suffix == 'tar'  (B=True のときのみ評価)
#   D: suffix == 'pb'
#   E: suffix == 'csv'
#   F: suffix == 'parquet'
#   G: suffix in ('xls', 'xlsx')
#   H: suffix == 'json'
#   I: suffix == 'jsonl'
#   J: suffix == 'xml'
#   K: suffix == 'shp'
#   L: suffix == 'geojson'
#   M: suffix == 'txt'
#   N: suffix == 'zip'
#   O: suffix == 'tar'
#   P: いずれにも該当しない → UNKNOWN
# ======================================================================
class TestFromPath:

    # ------------------------------------------------------------------
    # B=True, C=True: 複合圧縮拡張子 → TAR
    # ------------------------------------------------------------------
    @pytest.mark.parametrize("file_path", [
        "data/file.tar.gz",
        "data/file.tar.bz2",
        "data/file.tar.xz",
        "file.tar.gz",          # ディレクトリ成分なし
    ])
    def test_b_true_c_true_compound_tar_extension(self, file_path):
        """B=True, C=True: .tar.gz / .tar.bz2 / .tar.xz → TAR"""
        assert SupportedFormats.from_path(file_path) == SupportedFormats.TAR

    # ------------------------------------------------------------------
    # B=True, C=False: 圧縮ラッパーだが inner が tar でない → UNKNOWN
    # ------------------------------------------------------------------
    @pytest.mark.parametrize("file_path", [
        "data/file.gz",         # .tar なし
        "data/file.bz2",
        "data/file.xz",
        "data/file.json.gz",    # inner = .json (tar でない)
    ])
    def test_b_true_c_false_non_tar_compressed(self, file_path):
        """B=True, C=False: 圧縮ラッパーだが inner が tar でない → UNKNOWN"""
        assert SupportedFormats.from_path(file_path) == SupportedFormats.UNKNOWN

    # ------------------------------------------------------------------
    # B=False, D=True: .pb → GTFS_RT
    # ------------------------------------------------------------------
    @pytest.mark.parametrize("file_path", [
        "data/feed.pb",
        "realtime.pb",
    ])
    def test_b_false_d_true_pb_returns_gtfs_rt(self, file_path):
        """B=False, D=True: .pb → GTFS_RT"""
        assert SupportedFormats.from_path(file_path) == SupportedFormats.GTFS_RT

    # ------------------------------------------------------------------
    # B=False, D=False, 各条件 True → 対応メンバー
    # (既存テストを全メンバー網羅に拡張)
    # ------------------------------------------------------------------
    @pytest.mark.parametrize("file_path, expected", [
        # E=True
        ("data/file.csv",       SupportedFormats.CSV),
        # F=True
        ("data/file.parquet",   SupportedFormats.PARQUET),
        # G=True (xls と xlsx の両方)
        ("data/file.xls",       SupportedFormats.EXCEL),
        ("data/file.xlsx",      SupportedFormats.EXCEL),
        # H=True
        ("data/file.json",      SupportedFormats.JSON),
        # I=True
        ("data/file.jsonl",     SupportedFormats.JSONL),
        # J=True
        ("data/file.xml",       SupportedFormats.XML),
        # K=True
        ("data/file.shp",       SupportedFormats.SHAPEFILE),
        # L=True
        ("data/file.geojson",   SupportedFormats.GEOJSON),
        # M=True
        ("data/file.txt",       SupportedFormats.TEXT),
        # N=True
        ("data/file.zip",       SupportedFormats.ZIP),
        # O=True
        ("data/file.tar",       SupportedFormats.TAR),
        # P=True (いずれにも該当しない)
        ("data/file.unknownext", SupportedFormats.UNKNOWN),
        ("data/file",            SupportedFormats.UNKNOWN),  # 拡張子なし
    ])
    def test_known_extensions(self, file_path, expected):
        """B=False, D=False, 各条件True: 既知の拡張子 → 対応メンバーを返す"""
        assert SupportedFormats.from_path(file_path) == expected

    # ------------------------------------------------------------------
    # 大文字拡張子の正規化確認
    # ------------------------------------------------------------------
    @pytest.mark.parametrize("file_path, expected", [
        ("data/file.CSV",     SupportedFormats.CSV),
        ("data/file.PARQUET", SupportedFormats.PARQUET),
        ("data/file.XLSX",    SupportedFormats.EXCEL),
        ("data/file.JSON",    SupportedFormats.JSON),
        ("data/file.SHP",     SupportedFormats.SHAPEFILE),
        ("data/file.PB",      SupportedFormats.GTFS_RT),
    ])
    def test_uppercase_extension_normalized(self, file_path, expected):
        """拡張子の大文字 → .lower() で正規化されて正しく認識される"""
        assert SupportedFormats.from_path(file_path) == expected

    # ------------------------------------------------------------------
    # os.PathLike オブジェクトを受け付けるか
    # ------------------------------------------------------------------
    def test_accepts_pathlib_path(self):
        """引数型: pathlib.Path (os.PathLike) を渡せる"""
        assert SupportedFormats.from_path(pathlib.Path("data/file.csv")) == SupportedFormats.CSV

    def test_accepts_pathlib_path_compound_extension(self):
        """引数型: pathlib.Path で複合拡張子も正しく処理される"""
        assert SupportedFormats.from_path(pathlib.Path("data/file.tar.gz")) == SupportedFormats.TAR

    # ------------------------------------------------------------------
    # パスにディレクトリ成分がない場合
    # ------------------------------------------------------------------
    def test_filename_only_no_directory(self):
        """ディレクトリ成分なし: ファイル名のみでも正しく動作する"""
        assert SupportedFormats.from_path("file.json") == SupportedFormats.JSON

    # ------------------------------------------------------------------
    # 空文字パス
    # ------------------------------------------------------------------
    def test_empty_path_returns_unknown(self):
        """空文字パス → 拡張子なし → UNKNOWN"""
        assert SupportedFormats.from_path("") == SupportedFormats.UNKNOWN


if __name__ == "__main__":
    pytest.main([__file__, "-v"])