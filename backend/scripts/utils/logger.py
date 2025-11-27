import logging
import sys
from typing import Optional

LOG_FORMAT = '[%(inputdataname)s][%(levelname)s][%(filename)s][%(funcName)s:%(lineno)d]\t%(message)s'
LOG_NAME2LEVEL = {
    "TRACE": logging.DEBUG,
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARN": logging.WARNING,
    "ERROR": logging.ERROR,
    "FATAL": logging.FATAL,
}


class CustomFormatter(logging.Formatter):
    def __init__(self, fmt: str, inputdataname: str = ""):
        super().__init__(fmt)
        self.inputdataname = inputdataname

    def format(self, record):
        record.inputdataname = self.inputdataname
        return super().format(record)

class AppLogger:
    def __init__(self, inputdataname: str = ""):
        self.inputdataname = inputdataname

    def get_logger(self, name: str):
        return logging.getLogger(name)

    def init_logger(self, level_str: str = "INFO") -> logging.Logger:
        root_logger = logging.getLogger()

        # デフォルトの handler を全部消す
        # ex. glueは、自動的にhandlerを追加する。この結果、意図しないログ出力や、ログ出力されないことがある
        for h in root_logger.handlers[:]:
            root_logger.removeHandler(h)

        # ハンドラ未設定なら初期化（root_logger の状態がグローバルの代替）
        if not root_logger.handlers:
            handler = logging.StreamHandler(stream=sys.stdout)
            formatter = CustomFormatter(LOG_FORMAT, self.inputdataname)
            handler.setFormatter(formatter)
            root_logger.addHandler(handler)

        # ログレベル設定
        level = LOG_NAME2LEVEL.get(level_str.upper(), logging.INFO)
        root_logger.setLevel(level)

        # ハンドラの formatter を最新 inputdataname に更新
        for handler in root_logger.handlers:
            if isinstance(handler.formatter, CustomFormatter):
                handler.setFormatter(CustomFormatter(LOG_FORMAT, self.inputdataname))
            handler.setLevel(level)

        # 伝播設定（子ロガー → root）
        root_logger.propagate = True

        root_logger.info(f"LOG_LEVEL set to {logging.getLevelName(level)}")
        if self.inputdataname:
            root_logger.info(f"INPUT_DATA_NAME: {self.inputdataname}")

        return root_logger


def setup_logger(name: str) -> logging.Logger:
    """
    Each module calls:
        logger = setup_logger(__name__)
    No global variables required.
    The logger will propagate to the configured root logger.
    """
    logger = logging.getLogger(name)
    logger.propagate = True
    logger.setLevel(logging.NOTSET)  # inherit from root

    # root が未初期化の可能性があるので、安全のため最低限の初期化を行う
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        # default init (INFO, no inputdataname)
        handler = logging.StreamHandler(stream=sys.stdout)
        formatter = CustomFormatter(LOG_FORMAT, inputdataname="")
        handler.setFormatter(formatter)
        handler.setLevel(logging.INFO)
        root_logger.addHandler(handler)
        root_logger.setLevel(logging.INFO)
        root_logger.propagate = True

    return logger
