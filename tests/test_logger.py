#!filepath: tests/test_logger.py
"""
Test suite for the logger module.

This module contains unit tests for the logger module to ensure
it correctly creates and configures loggers.
"""

import os
import sys
import logging
import tempfile
import unittest
from unittest.mock import patch, MagicMock

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Import the module under test
from src.logger import (  # pylint: disable=wrong-import-position,import-error
    create_logger,
)


class TestLogger(unittest.TestCase):
    """Test suite for the logger module."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary directory for log files
        with tempfile.TemporaryDirectory() as temp_dir:
            self.temp_dir = temp_dir

        # Patch os.makedirs to avoid creating real directories
        self.makedirs_patcher = patch("os.makedirs")
        self.mock_makedirs = self.makedirs_patcher.start()

    def tearDown(self):
        """Tear down test fixtures."""
        # Stop all patches
        self.makedirs_patcher.stop()

        # Remove any handlers from the root logger
        root_logger = logging.getLogger()
        while root_logger.handlers:
            root_logger.removeHandler(root_logger.handlers[0])

        # Reset the logging module
        logging.shutdown()

    @patch("logging.FileHandler")
    def test_logger_creation(self, mock_file_handler):
        """Test that a logger is created with the correct name and level."""
        # Set up the mock
        mock_file_handler.return_value = MagicMock()

        # Create a logger
        logger_name = "test_logger"
        created_logger = create_logger(logger_name)

        # Assert that the logger has the correct name
        self.assertEqual(created_logger.name, logger_name)

        # Assert that the logger has the correct level
        self.assertEqual(created_logger.level, logging.INFO)

        # Assert that os.makedirs was called to create the logs directory
        self.mock_makedirs.assert_called_once_with("logs", exist_ok=True)

        # Assert that FileHandler was created
        mock_file_handler.assert_called_once()

        # Assert that the handler was added to the logger
        self.assertEqual(len(created_logger.handlers), 1)

        # Assert that propagation is disabled
        self.assertFalse(created_logger.propagate)

    @patch("logging.FileHandler")
    def test_logger_custom_level(self, mock_file_handler):
        """Test that the logger can be created with a custom level."""
        # Set up the mock
        mock_file_handler.return_value = MagicMock()

        # Create a logger with a custom level
        created_logger = create_logger("test_logger", level=logging.DEBUG)

        # Assert that the logger has the correct level
        self.assertEqual(created_logger.level, logging.DEBUG)

        # Assert that the handler has the correct level
        mock_file_handler.return_value.setLevel.assert_called_once_with(logging.DEBUG)

    @patch("src.logger.datetime")
    @patch("logging.FileHandler")
    @staticmethod
    def test_timestamp_in_filename(mock_file_handler, mock_datetime):
        """Test that the timestamp is included in the log filename."""
        # Set up the mock datetime
        mock_datetime_instance = MagicMock()
        mock_datetime_instance.strftime.return_value = "20230101_120000"
        mock_datetime.now.return_value = mock_datetime_instance

        # Set up the mock file handler to capture the filename
        mock_file_handler.return_value = MagicMock()

        # Create a logger
        create_logger("test_logger")

        # Verify the FileHandler was called with the correct filename
        mock_file_handler.assert_called_once_with(
            os.path.join("logs", "test_logger_20230101_120000.log")
        )

    def test_handler_formatter(self):
        """Test that the handler has the correct formatter."""
        # Create a real logger but with a mock handler
        with patch("logging.FileHandler", autospec=True) as mock_file_handler:
            # Set up the mock handler
            mock_handler = MagicMock()
            mock_file_handler.return_value = mock_handler

            # Create a logger
            create_logger("test_logger")

            # Get the formatter
            mock_handler.setFormatter.assert_called_once()
            formatter = mock_handler.setFormatter.call_args[0][0]

            # Assert the format string
            expected_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            self.assertEqual(
                formatter._fmt, expected_format  # pylint: disable=protected-access
            )

    def test_existing_handlers_cleared(self):
        """Test that existing handlers are cleared when creating a logger."""
        # Create a logger and add a handler to it
        logger_name = "test_logger"
        existing_logger = logging.getLogger(logger_name)
        handler = logging.NullHandler()
        existing_logger.addHandler(handler)

        # Assert that the logger has a handler
        self.assertEqual(len(existing_logger.handlers), 1)

        # Create a new logger with the same name using our function
        with patch("logging.FileHandler", autospec=True):
            updated_logger = create_logger(logger_name)

            # Assert that it's the same logger object (since loggers are singletons)
            self.assertIs(updated_logger, existing_logger)

            # Assert that the original handler was removed
            self.assertNotIn(handler, updated_logger.handlers)

            # Assert that only one new handler was added
            self.assertEqual(len(updated_logger.handlers), 1)

    def test_real_logging(self):
        """Test that logs are actually written to a file."""
        # Create a temporary directory for logs
        with tempfile.TemporaryDirectory() as temp_logs_dir:
            # Create a logger with the temporary directory
            with patch("src.logger.datetime") as mock_datetime:
                # Set a fixed timestamp
                mock_datetime_instance = MagicMock()
                mock_datetime_instance.strftime.return_value = "20230101_120000"
                mock_datetime.now.return_value = mock_datetime_instance

                # Create a logger with the temporary log directory
                created_logger = create_logger("test_logger", log_dir=temp_logs_dir)

                # Write some log messages
                created_logger.info("Test info message")
                created_logger.warning("Test warning message")
                created_logger.error("Test error message")

                # Find the log file
                log_files = [
                    f for f in os.listdir(temp_logs_dir) if f.startswith("test_logger_")
                ]
                self.assertEqual(
                    len(log_files), 1, "Expected one log file to be created"
                )

                # Construct full path to the log file
                log_file_path = os.path.join(temp_logs_dir, log_files[0])

                # Read the log file contents
                with open(log_file_path, "r", encoding="utf-8") as log_file:
                    log_content = log_file.read()

                # Assert log messages are present
                self.assertIn("Test info message", log_content)
                self.assertIn("Test warning message", log_content)
                self.assertIn("Test error message", log_content)


if __name__ == "__main__":
    unittest.main()
