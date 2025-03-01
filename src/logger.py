"""
Logger module for the Finance Dashboard application.

This module provides a standardized logging mechanism for the application.
"""

import os
import logging
from datetime import datetime
from pathlib import Path


def setup_logger(name="finance_dashboard", level=logging.INFO):
    """
    Set up a logger with file and console handlers.

    Args:
        name: Name of the logger
        level: Logging level

    Returns:
        logging.Logger: Configured logger
    """
    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)

    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Remove existing handlers if any
    if logger.handlers:
        logger.handlers.clear()

    # Create formatters
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_formatter = logging.Formatter("%(levelname)s: %(message)s")

    # Create file handler with timestamp in filename
    timestamp = datetime.now().strftime("%Y%m%d")
    file_handler = logging.FileHandler(f"logs/finance_dashboard_{timestamp}.log")
    file_handler.setLevel(level)
    file_handler.setFormatter(file_formatter)

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(console_formatter)

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# Create a default logger
logger = setup_logger()
