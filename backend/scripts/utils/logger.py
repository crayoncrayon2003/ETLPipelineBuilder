import logging
import sys
import os
from typing import Optional

def setup_logger(
    name: str = "etl_framework",
    level: str = "INFO",
    # log_file: Optional[str] = None
    log_file: Optional[str] = "/tmp/etl_framework.log"
) -> logging.Logger:
    """
    Sets up and configures a standardized logger that outputs to both
    console and an optional log file.

    Args:
        name (str): Logger name. Use `__name__` to keep module-specific loggers.
        level (str): Logging level (DEBUG, INFO, etc.).
        log_file (str, optional): File path for log output. If None, file logging is skipped.

    Returns:
        logging.Logger: Configured logger instance.
    """
    # Get the numeric logging level from the string
    log_level = getattr(logging, level.upper(), logging.INFO)

    # Get the logger. getLogger(name) ensures that we get the same logger
    # instance if called with the same name multiple times.
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    if logger.hasHandlers():
        return logger

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Optional file handler
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

# --- Example Usage ---
#
# A plugin or script would use this logger like so:
#
# from .logger import setup_logger
#
# # Get a logger specific to the current module
# log = setup_logger(__name__)
#
# def some_function():
#     log.debug("This is a detailed debug message.")
#     log.info("Starting an operation.")
#     log.warning("Something might be wrong here.")
#     try:
#         result = 1 / 0
#     except ZeroDivisionError:
#         log.error("An error occurred!", exc_info=True)
#
# if __name__ == '__main__':
#     # To see the debug message, you'd set the level:
#     # setup_logger(__name__, level="DEBUG")
#     some_function()