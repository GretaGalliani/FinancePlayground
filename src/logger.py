#!src/logger.py
"""
Logger module for the Finance Dashboard application.

This module provides a standardized logging mechanism for the application.
"""

import os
import logging
from datetime import datetime
from typing import Optional


def create_logger(
    name: str, level: int = logging.INFO, log_dir: str = "logs"
) -> logging.Logger:
    """
    Create a logger with file handler only (no console output).

    Args:
        name (str): Name of the logger.
        level (int, optional): Logging level. Defaults to logging.INFO.
        log_dir (str, optional): Directory to store log files. Defaults to "logs".

    Returns:
        logging.Logger: Configured logger instance.
    """
    # Ensure logs directory exists
    os.makedirs(log_dir, exist_ok=True)

    # Get or create logger
    logger = logging.getLogger(name)

    # Clear any existing handlers to prevent duplicate logging
    logger.handlers.clear()

    # Set logger level
    logger.setLevel(level)

    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Generate timestamp for log filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Create file handler
    log_filename = os.path.join(log_dir, f"{name}_{timestamp}.log")
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    # Add handler to logger
    logger.addHandler(file_handler)

    # Prevent log propagation
    logger.propagate = False

    return logger


def get_logger(name: str, level: Optional[int] = None) -> logging.Logger:
    """
    Retrieve an existing logger or create a new one.

    Args:
        name (str): Name of the logger.
        level (Optional[int], optional): Logging level. Defaults to None.

    Returns:
        logging.Logger: Logger instance.
    """
    logger = logging.getLogger(name)

    if level is not None:
        logger.setLevel(level)

    return logger


if __name__ == "__main__":
    # Example usage demonstrating logger creation and logging
    example_logger = create_logger("example_logger")
    example_logger.info("This is an informational log message")
    example_logger.warning("This is a warning message")
    example_logger.error("This is an error message")

    # Demonstrate getting an existing logger
    retrieved_logger = get_logger("example_logger")
    retrieved_logger.info("Logging from retrieved logger")
