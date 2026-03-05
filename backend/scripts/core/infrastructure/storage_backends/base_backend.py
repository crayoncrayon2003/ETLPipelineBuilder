import abc
from typing import Any, Dict, List, Union
import os


class BaseStorageBackend(abc.ABC):
    """
    ストレージバックエンドの共通インターフェース。
    新しいストレージ（Azure Blob、GCS等）を追加する場合は
    このクラスを継承して各メソッドを実装する。
    """

    @abc.abstractmethod
    def read_bytes(self, path: str) -> bytes:
        """指定パスからバイト列を読み込む"""
        pass

    @abc.abstractmethod
    def write_bytes(self, path: str, data: bytes) -> None:
        """指定パスにバイト列を書き込む"""
        pass

    @abc.abstractmethod
    def read_text(self, path: str, encoding: str = 'utf-8') -> str:
        """指定パスからテキストを読み込む"""
        pass

    @abc.abstractmethod
    def write_text(self, path: str, text_content: str, encoding: str = 'utf-8') -> None:
        """指定パスにテキストを書き込む"""
        pass

    @abc.abstractmethod
    def exists(self, path: str) -> bool:
        """指定パスが存在するか確認する"""
        pass

    @abc.abstractmethod
    def delete(self, path: str) -> None:
        """指定パスのファイルを削除する"""
        pass

    @abc.abstractmethod
    def get_size(self, path: str) -> int:
        """指定パスのファイルサイズ（バイト）を返す"""
        pass

    @abc.abstractmethod
    def list_files(self, path: str) -> List[str]:
        """指定パス以下のファイル一覧を返す"""
        pass

    @abc.abstractmethod
    def mkdir(self, path: str, exist_ok: bool = True) -> None:
        """ディレクトリを作成する"""
        pass

    @abc.abstractmethod
    def is_dir(self, path: str) -> bool:
        """指定パスがディレクトリかどうかを返す"""
        pass

    @abc.abstractmethod
    def rename(self, old_path: str, new_path: str) -> None:
        """ファイルをリネーム/移動する"""
        pass

    @abc.abstractmethod
    def stat(self, path: str) -> Dict[str, Any]:
        """ファイルのメタデータを返す"""
        pass