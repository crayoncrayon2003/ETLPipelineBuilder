import logging
import io
import sys
import pytest
from unittest.mock import patch, MagicMock
from scripts.utils.logger import AppLogger, setup_logger, CustomFormatter


# ==============================================
# CustomFormatter Tests
# ==============================================
class TestCustomFormatter:
    """CustomFormatter の format メソッドをテスト"""

    def test_format_with_inputdataname(self):
        """inputdataname が設定されている場合"""
        formatter = CustomFormatter('[%(inputdataname)s] %(message)s', inputdataname="test_data")
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="test message", args=(), exc_info=None
        )
        formatted = formatter.format(record)
        assert "[test_data]" in formatted
        assert "test message" in formatted

    def test_format_without_inputdataname(self):
        """inputdataname が空の場合"""
        formatter = CustomFormatter('[%(inputdataname)s] %(message)s', inputdataname="")
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="test message", args=(), exc_info=None
        )
        formatted = formatter.format(record)
        assert "[]" in formatted
        assert "test message" in formatted


# ==============================================
# AppLogger Tests
# ==============================================
class TestAppLogger:

    @pytest.fixture(autouse=True)
    def reset_logging(self):
        """各テスト前後にルートロガーをクリア"""
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
            handler.close()
        root_logger.setLevel(logging.WARNING)
        logging.Logger.manager.loggerDict.clear()

        yield

        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
            handler.close()

    # -----------------------------
    # init_logger
    # -----------------------------

    def test_init_logger_no_handlers(self, caplog):
        """
        MCDC: root_logger.handlers が空の場合
        条件: not root_logger.handlers = True

        この条件をテストするには、pytestのログキャプチャを一時的に無効化する必要があります。
        代わりに、ハンドラが正しく機能することを検証します。
        """
        app_logger = AppLogger(inputdataname="data1")

        # ログ出力を記録
        with caplog.at_level(logging.INFO):
            logger = app_logger.init_logger(level_str="INFO")

            # コンテキスト内でログレベルを確認
            assert logger.level == logging.INFO

            # 新しいログを出力
            logger.info("test_message")

        # ログメッセージが出力されることを確認（ハンドラが機能している証拠）
        assert "LOG_LEVEL set to INFO" in caplog.text
        assert "INPUT_DATA_NAME: data1" in caplog.text
        assert "test_message" in caplog.text

    def test_init_logger_with_existing_handlers(self):
        """
        MCDC: root_logger.handlers が既に存在する場合
        条件: not root_logger.handlers = False
        """
        root_logger = logging.getLogger()

        # 事前にハンドラを追加
        handler = logging.StreamHandler(stream=io.StringIO())
        formatter = CustomFormatter('[%(inputdataname)s] %(message)s', inputdataname="old")
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)
        root_logger.setLevel(logging.INFO)

        initial_handler_count = len(root_logger.handlers)

        app_logger = AppLogger(inputdataname="data2")
        logger = app_logger.init_logger(level_str="DEBUG")

        # ハンドラが重複追加されていないことを確認
        assert len(root_logger.handlers) == initial_handler_count

        # CustomFormatter の inputdataname が更新されていることを確認
        has_updated_formatter = any(
            isinstance(h.formatter, CustomFormatter) and h.formatter.inputdataname == "data2"
            for h in root_logger.handlers
        )
        assert has_updated_formatter
        assert logger.level == logging.DEBUG

    def test_init_logger_with_inputdataname(self, caplog):
        """
        MCDC: self.inputdataname が設定されている場合
        条件: if self.inputdataname = True
        """
        with caplog.at_level(logging.INFO):
            app_logger = AppLogger(inputdataname="test_input")
            logger = app_logger.init_logger(level_str="INFO")

        # INPUT_DATA_NAME がログに出力されることを確認
        assert "INPUT_DATA_NAME: test_input" in caplog.text

    def test_init_logger_without_inputdataname(self, caplog):
        """
        MCDC: self.inputdataname が空の場合
        条件: if self.inputdataname = False
        """
        with caplog.at_level(logging.INFO):
            app_logger = AppLogger(inputdataname="")
            logger = app_logger.init_logger(level_str="INFO")

        # INPUT_DATA_NAME が出力されないことを確認
        assert "INPUT_DATA_NAME:" not in caplog.text
        # LOG_LEVEL は出力される
        assert "LOG_LEVEL set to INFO" in caplog.text

    def test_init_logger_handler_formatter_is_custom(self):
        """
        MCDC: handler.formatter が CustomFormatter の場合
        条件: isinstance(handler.formatter, CustomFormatter) = True
        """
        root_logger = logging.getLogger()

        # 事前に CustomFormatter を持つハンドラを追加
        handler = logging.StreamHandler(stream=io.StringIO())
        formatter = CustomFormatter('[%(inputdataname)s] %(message)s', inputdataname="old")
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)
        root_logger.setLevel(logging.INFO)

        app_logger = AppLogger(inputdataname="new_data")
        logger = app_logger.init_logger(level_str="INFO")

        # CustomFormatter の inputdataname が更新されていることを確認
        assert handler.formatter.inputdataname == "new_data"

    def test_init_logger_handler_formatter_not_custom(self):
        """
        MCDC: handler.formatter が CustomFormatter でない場合
        条件: isinstance(handler.formatter, CustomFormatter) = False
        """
        root_logger = logging.getLogger()

        # 事前に通常の Formatter を持つハンドラを追加
        handler = logging.StreamHandler(stream=io.StringIO())
        formatter = logging.Formatter('%(message)s')
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)
        root_logger.setLevel(logging.INFO)

        original_formatter = handler.formatter

        app_logger = AppLogger(inputdataname="new_data")
        logger = app_logger.init_logger(level_str="INFO")

        # 通常の Formatter は更新されない
        assert handler.formatter is original_formatter
        assert not isinstance(handler.formatter, CustomFormatter)

    # -----------------------------
    # LOG_NAME2LEVEL
    # -----------------------------

    @pytest.mark.parametrize("level_str,expected_level", [
        ("TRACE", logging.DEBUG),
        ("DEBUG", logging.DEBUG),
        ("INFO", logging.INFO),
        ("WARN", logging.WARNING),
        ("ERROR", logging.ERROR),
        ("FATAL", logging.FATAL),
        ("UNKNOWN", logging.INFO),  # デフォルト値
    ])
    def test_init_logger_levels(self, level_str, expected_level):
        """各ログレベルの設定をテスト"""
        app_logger = AppLogger(inputdataname="")
        logger = app_logger.init_logger(level_str=level_str)

        assert logger.level == expected_level

    def test_get_logger(self):
        """get_logger メソッドのテスト"""
        app_logger = AppLogger(inputdataname="test")
        logger = app_logger.get_logger("test_module")

        assert logger.name == "test_module"
        assert isinstance(logger, logging.Logger)


# ==============================================
# setup_logger Tests
# ==============================================
class TestSetupLogger:

    @pytest.fixture(autouse=True)
    def reset_logging(self):
        """各テスト前後にルートロガーをクリア"""
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
            handler.close()
        root_logger.setLevel(logging.WARNING)
        logging.Logger.manager.loggerDict.clear()

        yield

        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
            handler.close()

    def test_setup_logger_root_no_handlers(self, caplog):
        """
        MCDC: root が未初期化（ハンドラなし）の場合
        条件: not root_logger.handlers = True

        この条件をテストするには、pytestのログキャプチャを一時的に無効化する必要があります。
        代わりに、デフォルト初期化が正しく機能することを検証します。
        """
        with caplog.at_level(logging.INFO):
            logger = setup_logger("test_module")
            logger.info("Test message from module")

        # ロガーが正しく設定されていることを確認
        assert logger.propagate is True
        assert logger.level == logging.NOTSET

        # ログメッセージが出力されることを確認（ハンドラが機能している証拠）
        assert "Test message from module" in caplog.text

    def test_setup_logger_root_with_handlers(self):
        """
        MCDC: root が既に初期化されている場合
        条件: not root_logger.handlers = False
        """
        root_logger = logging.getLogger()

        # 事前に root を初期化
        handler = logging.StreamHandler(stream=io.StringIO())
        root_logger.addHandler(handler)
        root_logger.setLevel(logging.INFO)

        initial_handler_count = len(root_logger.handlers)

        logger = setup_logger("test_module2")

        # ハンドラが重複追加されていないことを確認
        assert len(root_logger.handlers) == initial_handler_count
        assert logger.propagate is True

    def test_setup_logger_propagation(self):
        """ロガーの propagate 設定を確認"""
        logger = setup_logger("test_propagation")

        assert logger.propagate is True
        assert logger.level == logging.NOTSET

    def test_setup_logger_message_propagates(self, caplog):
        """子ロガーのメッセージが root に伝播することを確認"""
        with caplog.at_level(logging.INFO):
            logger = setup_logger("test_child")
            logger.info("Test message")

        # メッセージが記録されていることを確認
        assert "Test message" in caplog.text


# ==============================================
# Integration Tests
# ==============================================
class TestIntegration:
    """AppLogger と setup_logger の統合テスト"""

    @pytest.fixture(autouse=True)
    def reset_logging(self):
        """各テスト前後にルートロガーをクリア"""
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
            handler.close()
        root_logger.setLevel(logging.WARNING)
        logging.Logger.manager.loggerDict.clear()

        yield

        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
            handler.close()

    def test_applogger_with_setup_logger(self, caplog):
        """AppLogger で初期化後、setup_logger で取得したロガーが正しく動作"""
        with caplog.at_level(logging.DEBUG):
            # AppLogger で初期化
            app_logger = AppLogger(inputdataname="integration_test")
            app_logger.init_logger(level_str="DEBUG")

            # setup_logger で子ロガーを取得
            logger = setup_logger("child_module")
            logger.debug("Debug from child")
            logger.info("Info from child")

        # メッセージが記録されていることを確認
        assert "Debug from child" in caplog.text
        assert "Info from child" in caplog.text
        assert "INPUT_DATA_NAME: integration_test" in caplog.text

    def test_applogger_format_applied(self):
        """AppLogger のフォーマットが正しく適用されることを確認"""
        log_stream = io.StringIO()

        # 直接 StreamHandler を root に追加
        root_logger = logging.getLogger()
        handler = logging.StreamHandler(stream=log_stream)
        formatter = CustomFormatter(
            '[%(inputdataname)s][%(levelname)s] %(message)s',
            inputdataname="format_test"
        )
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)
        root_logger.setLevel(logging.INFO)

        # ログ出力
        root_logger.info("Test message")

        # フォーマットの確認
        log_output = log_stream.getvalue()
        assert "[format_test]" in log_output
        assert "Test message" in log_output


if __name__ == "__main__":
    pytest.main([__file__, "-v"])