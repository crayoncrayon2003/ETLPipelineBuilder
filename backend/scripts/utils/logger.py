import logging
import sys
from typing import Any

LOG_FORMAT = '[%(inputdataname)s][%(levelname)s][%(filename)s][%(funcName)s:%(lineno)d]\t%(message)s'
LOG_NAME2LEVEL = {
    "TRACE":    logging.DEBUG,
    "DEBUG":    logging.DEBUG,
    "INFO":     logging.INFO,
    "WARN":     logging.WARNING,
    "WARNING":  logging.WARNING,
    "ERROR":    logging.ERROR,
    "FATAL":    logging.FATAL,
    "CRITICAL": logging.CRITICAL,
}

REDACTED_VALUE = "***REDACTED***"
_SENSITIVE_KEY_PARTS = (
    "password",
    "passwd",
    "secret",
    "token",
    "api_key",
    "apikey",
    "authorization",
    "credential",
    "private_key",
)


def redact_sensitive_data(value: Any) -> Any:
    """Return a log-safe copy of nested configuration data."""
    if isinstance(value, dict):
        redacted = {}
        for key, item in value.items():
            normalized_key = str(key).lower().replace("-", "_")
            if any(part in normalized_key for part in _SENSITIVE_KEY_PARTS):
                redacted[key] = REDACTED_VALUE
            else:
                redacted[key] = redact_sensitive_data(item)
        return redacted
    if isinstance(value, list):
        return [redact_sensitive_data(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_sensitive_data(item) for item in value)
    return value


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
        self._handler_name = "etl_framework_backend_handler"

    def get_logger(self, name: str) -> logging.Logger:
        return logging.getLogger(name)

    def init_logger(self, level_str: str = "INFO") -> logging.Logger:
        root_logger = logging.getLogger()

        level = LOG_NAME2LEVEL.get(level_str.upper(), logging.INFO)
        root_logger.setLevel(level)

        # 既存 handler を尊重しつつ、自分の handler がなければ追加
        app_handler = None
        for h in root_logger.handlers:
            if getattr(h, "name", None) == self._handler_name:
                app_handler = h
                break

        if app_handler is None:
            app_handler = logging.StreamHandler(stream=sys.stdout)
            app_handler.name = self._handler_name
            root_logger.addHandler(app_handler)

        # formatter は自分の handler のみ更新
        formatter = CustomFormatter(LOG_FORMAT, self.inputdataname)
        app_handler.setFormatter(formatter)
        app_handler.setLevel(level)

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

    root_logger = logging.getLogger()
    if not root_logger.handlers:
        # default init (INFO, no inputdataname)
        handler = logging.StreamHandler(stream=sys.stdout)
        formatter = CustomFormatter(LOG_FORMAT, inputdataname="")
        handler.setFormatter(formatter)
        handler.setLevel(logging.INFO)
        root_logger.addHandler(handler)
        root_logger.setLevel(logging.INFO)

    return logger
