import os
import logging
import tempfile
import pytest
from scripts.utils.logger import setup_logger

# ==============================================
# Logger Tests
# ==============================================
class TestLogger:

    # -----------------------------
    # Console Output
    # -----------------------------
    def test_console_only(self, capsys):
        logger = setup_logger("test_console", log_file=None, level="INFO")
        logger.info("Info message")
        logger.warning("Warning message")
        captured = capsys.readouterr()
        assert "Info message" in captured.out
        assert "Warning message" in captured.out

    # -----------------------------
    # File Output
    # -----------------------------
    def test_file_output(self, tmp_path):
        log_path = tmp_path / "test.log"
        logger = setup_logger("test_file", log_file=str(log_path), level="INFO")

        logger.info("Info log")
        logger.error("Error log")

        # flush handlers to ensure logs are written
        for handler in logger.handlers:
            if hasattr(handler, "flush"):
                handler.flush()

        assert log_path.exists()
        with open(log_path, "r", encoding="utf-8") as f:
            content = f.read()
            assert "Info log" in content
            assert "Error log" in content

    # -----------------------------
    # Log Levels
    # -----------------------------
    @pytest.mark.parametrize("level,should_log_debug", [
        ("DEBUG", True),
        ("INFO", False),
    ])
    def test_levels(self, capsys, level, should_log_debug):
        logger = setup_logger("test_level", log_file=None, level=level)
        logger.debug("Debug message")
        captured = capsys.readouterr()
        if should_log_debug:
            assert "Debug message" in captured.out
        else:
            assert "Debug message" not in captured.out

    # -----------------------------
    # Reuse Existing Handlers
    # -----------------------------
    def test_reuse_handlers(self, tmp_path):
        log_path = tmp_path / "test.log"
        logger1 = setup_logger("test_reuse", log_file=str(log_path))
        logger2 = setup_logger("test_reuse", log_file=str(log_path))
        # Retrieving a logger with the same name does not duplicate its handlers
        assert len(logger2.handlers) == len(logger1.handlers)

    # -----------------------------
    # Invalid File Paths
    # -----------------------------
    def test_invalid_file_path(self):
        invalid_path = "/root/forbidden_dir/test.log"
        with pytest.raises((PermissionError, OSError)):
            setup_logger("test_invalid", log_file=invalid_path)

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
