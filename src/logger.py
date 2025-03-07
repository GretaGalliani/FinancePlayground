"""
Logger module for the Finance Dashboard application.

This module provides a standardized logging mechanism for the application.
"""

import os
import logging
from datetime import datetime
from pathlib import Path


def create_logger(name, level=logging.INFO):
    """
    Create a logger with file handler only (no console output).

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

    # Create formatter
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Create file handler with timestamp in filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_handler = logging.FileHandler(f"logs/finance_{timestamp}.log")
    file_handler.setLevel(level)
    file_handler.setFormatter(file_formatter)

    # Add handler to logger
    logger.addHandler(file_handler)

    # Prevent propagation to parent loggers (including root logger)
    logger.propagate = False

    return logger
