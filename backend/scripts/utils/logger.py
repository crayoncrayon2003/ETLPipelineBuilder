# backend/utils/logger.py

import logging
import sys
from typing import Optional

def setup_logger(
    name: str = "etl_framework",
    level: str = "INFO"
) -> logging.Logger:
    """
    Sets up and configures a standardized logger.

    This function creates a logger with a specified name and level,
    and attaches a handler that prints logs to the console with a
    consistent format.

    Args:
        name (str, optional): The name of the logger. It's good practice
            to use the module name (`__name__`) here. Defaults to "etl_framework".
        level (str, optional): The minimum logging level to output.
            Can be 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'.
            Defaults to "INFO".

    Returns:
        logging.Logger: A configured logger instance.
    """
    # Get the numeric logging level from the string
    log_level = getattr(logging, level.upper(), logging.INFO)

    # Get the logger. getLogger(name) ensures that we get the same logger
    # instance if called with the same name multiple times.
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    # Prevent adding multiple handlers if the logger is already configured
    if logger.hasHandlers():
        return logger

    # Create a handler to direct logs to the standard output (console)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)

    # Create a formatter to define the log message format
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Set the formatter for the handler
    handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(handler)

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