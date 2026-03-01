import logging
import io
import sys
import pytest
from unittest.mock import patch, MagicMock
from scripts.utils.logger import AppLogger, setup_logger, CustomFormatter, LOG_NAME2LEVEL


# ======================================================================
# CustomFormatter
# ======================================================================
class TestCustomFormatter:

    def test_format_with_inputdataname(self):
        """inputdataname が設定されている場合 → フォーマットに含まれる"""
        formatter = CustomFormatter('[%(inputdataname)s] %(message)s', inputdataname="test_data")
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="test message", args=(), exc_info=None
        )
        assert "[test_data]" in formatter.format(record)
        assert "test message" in formatter.format(record)

    def test_format_without_inputdataname(self):
        """inputdataname が空の場合 → [] のまま"""
        formatter = CustomFormatter('[%(inputdataname)s] %(message)s', inputdataname="")
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="test message", args=(), exc_info=None
        )
        assert "[]" in formatter.format(record)
        assert "test message" in formatter.format(record)

    def test_format_does_not_affect_other_records(self):
        """format() が record に inputdataname を設定しても他の record に影響しない"""
        formatter = CustomFormatter('[%(inputdataname)s] %(message)s', inputdataname="x")
        r1 = logging.LogRecord("a", logging.INFO, "", 0, "msg1", (), None)
        r2 = logging.LogRecord("b", logging.INFO, "", 0, "msg2", (), None)
        formatter.format(r1)
        formatter.format(r2)
        assert r1.inputdataname == "x"
        assert r2.inputdataname == "x"


# ======================================================================
# AppLogger.init_logger
#
# MCDC:
#   条件A: etl_framework_backend_handler が未登録か (app_handler is None)
#     A=True  → 新規 StreamHandler を追加
#     A=False → 既存 handler を再利用 (重複追加しない)
#
#   条件B: self.inputdataname が truthy か
#     B=True  → INPUT_DATA_NAME をログ出力
#     B=False → 出力しない
#
#   条件C: level_str が LOG_NAME2LEVEL に存在するか
#     C=True  → 対応レベルを設定
#     C=False → デフォルト INFO
# ======================================================================
class TestAppLogger:

    @pytest.fixture(autouse=True)
    def reset_logging(self):
        root_logger = logging.getLogger()
        for h in root_logger.handlers[:]:
            root_logger.removeHandler(h)
            h.close()
        root_logger.setLevel(logging.WARNING)
        logging.Logger.manager.loggerDict.clear()
        yield
        root_logger = logging.getLogger()
        for h in root_logger.handlers[:]:
            root_logger.removeHandler(h)
            h.close()

    # ------------------------------------------------------------------
    # 条件A: app_handler が None か
    # ------------------------------------------------------------------

    def test_a_true_no_app_handler_adds_new_handler(self, caplog):
        """A=True: etl_framework_backend_handler 未登録 → 新規追加される"""
        with caplog.at_level(logging.INFO):
            logger = AppLogger(inputdataname="data1").init_logger(level_str="INFO")
            # caplog.at_level() は with ブロック終了後にレベルを元に戻すため
            # logger.level の確認は with ブロック内で行う
            assert logger.level == logging.INFO
            logger.info("test_message")

        assert "LOG_LEVEL set to INFO" in caplog.text
        assert "INPUT_DATA_NAME: data1" in caplog.text
        assert "test_message" in caplog.text

    def test_a_true_handler_name_is_etl_framework(self):
        """A=True: 新規追加された handler の name が 'etl_framework_backend_handler'"""
        AppLogger(inputdataname="").init_logger()
        root_logger = logging.getLogger()
        names = [getattr(h, "name", None) for h in root_logger.handlers]
        assert "etl_framework_backend_handler" in names

    def test_a_false_second_call_does_not_duplicate_handler(self):
        """A=False: init_logger を2回呼んでも etl_framework_backend_handler が重複しない"""
        AppLogger(inputdataname="first").init_logger(level_str="INFO")
        AppLogger(inputdataname="second").init_logger(level_str="DEBUG")

        root_logger = logging.getLogger()
        app_handlers = [
            h for h in root_logger.handlers
            if getattr(h, "name", None) == "etl_framework_backend_handler"
        ]
        assert len(app_handlers) == 1

    def test_a_false_second_call_updates_formatter_inputdataname(self):
        """A=False: 2回目の init_logger で formatter の inputdataname が更新される"""
        AppLogger(inputdataname="first").init_logger(level_str="INFO")
        AppLogger(inputdataname="second").init_logger(level_str="INFO")

        root_logger = logging.getLogger()
        app_handler = next(
            h for h in root_logger.handlers
            if getattr(h, "name", None) == "etl_framework_backend_handler"
        )
        assert app_handler.formatter.inputdataname == "second"

    def test_a_false_second_call_updates_level(self):
        """A=False: 2回目の init_logger でレベルが更新される"""
        AppLogger(inputdataname="").init_logger(level_str="INFO")
        AppLogger(inputdataname="").init_logger(level_str="DEBUG")

        root_logger = logging.getLogger()
        assert root_logger.level == logging.DEBUG

    def test_existing_non_app_handler_is_not_removed(self):
        """init_logger は自分の handler 以外の既存 handler を削除しない"""
        root_logger = logging.getLogger()
        external_handler = logging.StreamHandler(stream=io.StringIO())
        root_logger.addHandler(external_handler)

        AppLogger(inputdataname="x").init_logger()

        assert external_handler in root_logger.handlers

    def test_non_app_handler_formatter_is_not_changed(self):
        """init_logger は etl_framework_backend_handler 以外の formatter を変更しない"""
        root_logger = logging.getLogger()
        external_handler = logging.StreamHandler(stream=io.StringIO())
        original_formatter = logging.Formatter('%(message)s')
        external_handler.setFormatter(original_formatter)
        root_logger.addHandler(external_handler)

        AppLogger(inputdataname="new_data").init_logger(level_str="INFO")

        assert external_handler.formatter is original_formatter
        assert not isinstance(external_handler.formatter, CustomFormatter)

    def test_app_handler_has_custom_formatter(self):
        """etl_framework_backend_handler には CustomFormatter がセットされる"""
        AppLogger(inputdataname="fmt_test").init_logger()
        root_logger = logging.getLogger()
        app_handler = next(
            h for h in root_logger.handlers
            if getattr(h, "name", None) == "etl_framework_backend_handler"
        )
        assert isinstance(app_handler.formatter, CustomFormatter)
        assert app_handler.formatter.inputdataname == "fmt_test"

    # ------------------------------------------------------------------
    # 条件B: self.inputdataname が truthy か
    # ------------------------------------------------------------------

    def test_b_true_inputdataname_logged(self, caplog):
        """B=True: inputdataname あり → INPUT_DATA_NAME がログ出力される"""
        with caplog.at_level(logging.INFO):
            AppLogger(inputdataname="test_input").init_logger(level_str="INFO")
        assert "INPUT_DATA_NAME: test_input" in caplog.text

    def test_b_false_no_inputdataname_not_logged(self, caplog):
        """B=False: inputdataname 空 → INPUT_DATA_NAME はログ出力されない"""
        with caplog.at_level(logging.INFO):
            AppLogger(inputdataname="").init_logger(level_str="INFO")
        assert "INPUT_DATA_NAME:" not in caplog.text
        assert "LOG_LEVEL set to INFO" in caplog.text

    # ------------------------------------------------------------------
    # 条件C: level_str のマッピング
    # ------------------------------------------------------------------

    @pytest.mark.parametrize("level_str, expected_level", [
        ("TRACE",    logging.DEBUG),
        ("DEBUG",    logging.DEBUG),
        ("INFO",     logging.INFO),
        ("WARN",     logging.WARNING),
        ("WARNING",  logging.WARNING),
        ("ERROR",    logging.ERROR),
        ("FATAL",    logging.FATAL),
        ("CRITICAL", logging.CRITICAL),
        ("UNKNOWN",  logging.INFO),      # C=False: デフォルト INFO
        ("info",     logging.INFO),      # 大文字小文字を区別しない
    ])
    def test_c_all_level_strings(self, level_str, expected_level):
        """C=True/False: 全レベル文字列とデフォルトフォールバックを確認"""
        logger = AppLogger(inputdataname="").init_logger(level_str=level_str)
        assert logger.level == expected_level

    # ------------------------------------------------------------------
    # root_logger.propagate が変更されないこと
    # ------------------------------------------------------------------

    def test_init_logger_does_not_set_root_propagate(self):
        """init_logger は root_logger.propagate を変更しない
        ルートロガーに親はないため propagate の設定は無意味"""
        root_logger = logging.getLogger()
        original_propagate = root_logger.propagate
        AppLogger(inputdataname="").init_logger()
        assert root_logger.propagate == original_propagate

    # ------------------------------------------------------------------
    # LOG_NAME2LEVEL の定義確認
    # ------------------------------------------------------------------

    def test_log_name2level_contains_warning(self):
        """LOG_NAME2LEVEL に WARNING が含まれる"""
        assert "WARNING" in LOG_NAME2LEVEL
        assert LOG_NAME2LEVEL["WARNING"] == logging.WARNING

    def test_log_name2level_contains_critical(self):
        """LOG_NAME2LEVEL に CRITICAL が含まれる"""
        assert "CRITICAL" in LOG_NAME2LEVEL
        assert LOG_NAME2LEVEL["CRITICAL"] == logging.CRITICAL

    def test_log_name2level_fatal_equals_critical(self):
        """FATAL と CRITICAL は同じ値 (50) である"""
        assert LOG_NAME2LEVEL["FATAL"] == LOG_NAME2LEVEL["CRITICAL"]

    # ------------------------------------------------------------------
    # get_logger
    # ------------------------------------------------------------------

    def test_get_logger_returns_named_logger(self):
        """get_logger は指定した name の Logger を返す"""
        logger = AppLogger(inputdataname="test").get_logger("test_module")
        assert logger.name == "test_module"
        assert isinstance(logger, logging.Logger)


# ======================================================================
# setup_logger
#
# MCDC:
#   条件D: root_logger.handlers が空か
#     D=True  → デフォルトハンドラを追加
#     D=False → 何も追加しない
# ======================================================================
class TestSetupLogger:

    @pytest.fixture(autouse=True)
    def reset_logging(self):
        root_logger = logging.getLogger()
        for h in root_logger.handlers[:]:
            root_logger.removeHandler(h)
            h.close()
        root_logger.setLevel(logging.WARNING)
        logging.Logger.manager.loggerDict.clear()
        yield
        root_logger = logging.getLogger()
        for h in root_logger.handlers[:]:
            root_logger.removeHandler(h)
            h.close()

    def test_d_true_adds_default_handler(self, caplog):
        """D=True: root 未初期化 → デフォルトハンドラが追加され INFO が動作する"""
        with caplog.at_level(logging.INFO):
            logger = setup_logger("test_module")
            logger.info("Test message from module")

        assert logger.propagate is True
        assert logger.level == logging.NOTSET
        assert "Test message from module" in caplog.text

    def test_d_true_default_handler_has_custom_formatter(self):
        """D=True: デフォルト追加された handler が CustomFormatter を使う
        pytest が内部ハンドラを持つため、patch.object で handlers を空リストに制御し
        「ハンドラなし」状態を確実に再現する"""
        added_handlers = []

        def mock_add_handler(handler):
            added_handlers.append(handler)

        root_logger = logging.getLogger()
        with patch.object(logging.root, 'handlers', new=[]):
            with patch.object(root_logger, 'addHandler', side_effect=mock_add_handler):
                setup_logger("test_module")

        assert any(isinstance(h.formatter, CustomFormatter) for h in added_handlers)

    def test_d_false_existing_handler_not_duplicated(self):
        """D=False: root に既存 handler あり → ハンドラが重複追加されない"""
        root_logger = logging.getLogger()
        root_logger.addHandler(logging.StreamHandler(stream=io.StringIO()))
        root_logger.setLevel(logging.INFO)
        initial_count = len(root_logger.handlers)

        setup_logger("test_module2")

        assert len(root_logger.handlers) == initial_count

    def test_propagate_is_true(self):
        """setup_logger が返す logger の propagate が True"""
        assert setup_logger("test_propagation").propagate is True

    def test_level_is_notset(self):
        """setup_logger が返す logger の level が NOTSET (root から継承)"""
        assert setup_logger("test_level").level == logging.NOTSET

    def test_message_propagates_to_root(self, caplog):
        """子ロガーのメッセージが root に伝播する"""
        with caplog.at_level(logging.INFO):
            logger = setup_logger("test_child")
            logger.info("Test message")
        assert "Test message" in caplog.text

    def test_setup_logger_does_not_set_root_propagate(self):
        """setup_logger は root_logger.propagate を変更しない"""
        root_logger = logging.getLogger()
        original_propagate = root_logger.propagate
        setup_logger("test_no_propagate")
        assert root_logger.propagate == original_propagate


# ======================================================================
# Integration Tests
# ======================================================================
class TestIntegration:

    @pytest.fixture(autouse=True)
    def reset_logging(self):
        root_logger = logging.getLogger()
        for h in root_logger.handlers[:]:
            root_logger.removeHandler(h)
            h.close()
        root_logger.setLevel(logging.WARNING)
        logging.Logger.manager.loggerDict.clear()
        yield
        root_logger = logging.getLogger()
        for h in root_logger.handlers[:]:
            root_logger.removeHandler(h)
            h.close()

    def test_applogger_then_setup_logger(self, caplog):
        """AppLogger で初期化後、setup_logger の子ロガーが正しく動作する"""
        with caplog.at_level(logging.DEBUG):
            AppLogger(inputdataname="integration_test").init_logger(level_str="DEBUG")
            logger = setup_logger("child_module")
            logger.debug("Debug from child")
            logger.info("Info from child")

        assert "Debug from child" in caplog.text
        assert "Info from child" in caplog.text
        assert "INPUT_DATA_NAME: integration_test" in caplog.text

    def test_custom_formatter_output_format(self):
        """CustomFormatter のフォーマットが実際の出力に適用される"""
        log_stream = io.StringIO()
        root_logger = logging.getLogger()
        handler = logging.StreamHandler(stream=log_stream)
        handler.setFormatter(CustomFormatter(
            '[%(inputdataname)s][%(levelname)s] %(message)s',
            inputdataname="format_test"
        ))
        root_logger.addHandler(handler)
        root_logger.setLevel(logging.INFO)

        root_logger.info("Test message")

        output = log_stream.getvalue()
        assert "[format_test]" in output
        assert "Test message" in output


if __name__ == "__main__":
    pytest.main([__file__, "-v"])