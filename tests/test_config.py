#!filepath: tests/test_config.py
"""
Test suite for the Config class.

This module contains unit tests for the Config class to ensure
proper loading and accessing of configuration values.
"""

import os
import sys
import tempfile
import unittest
from unittest.mock import MagicMock
import logging
import yaml
import pytest

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.config import Config  # pylint: disable=wrong-import-position,import-error


class TestConfig(unittest.TestCase):
    """Test suite for the Config class."""

    def setUp(self):
        """Set up test fixtures, if any."""
        # Create a logger that doesn't actually log anything
        self.logger = logging.getLogger("test_logger")
        self.logger.setLevel(logging.CRITICAL + 1)  # Higher than any real level

        # Sample configuration
        self.sample_config = {
            "app_name": "Finance Dashboard",
            "version": "1.0.0",
            "environment": "test",
            "database": {
                "host": "localhost",
                "port": 5432,
                "username": "test_user",
                "password": "test_password",
            },
            "paths": {"input": "input", "output": "output"},
        }

        # Create a temporary file with the sample configuration
        self.temp_dir = (
            tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        )
        self.config_path = os.path.join(self.temp_dir.name, "test_config.yaml")

        with open(self.config_path, "w", encoding="utf-8") as config_file:
            yaml.dump(self.sample_config, config_file)

    def tearDown(self):
        """Tear down test fixtures, if any."""
        # Clean up the temporary directory
        self.temp_dir.cleanup()

    def test_init_with_valid_file(self):
        """Test initializing Config with a valid file."""
        config = Config(self.config_path, self.logger)
        self.assertTrue(config.is_valid())

    def test_init_with_nonexistent_file(self):
        """Test initializing Config with a nonexistent file."""
        nonexistent_path = os.path.join(self.temp_dir.name, "nonexistent.yaml")
        with self.assertRaises(FileNotFoundError):
            Config(nonexistent_path, self.logger)

    def test_init_with_invalid_yaml(self):
        """Test initializing Config with invalid YAML."""
        invalid_yaml_path = os.path.join(self.temp_dir.name, "invalid.yaml")
        with open(invalid_yaml_path, "w", encoding="utf-8") as config_file:
            config_file.write("This is not valid YAML: {unclosed")

        with self.assertRaises(ValueError):
            Config(invalid_yaml_path, self.logger)

    def test_init_with_non_dict_yaml(self):
        """Test initializing Config with YAML that doesn't produce a dict."""
        non_dict_yaml_path = os.path.join(self.temp_dir.name, "non_dict.yaml")
        with open(non_dict_yaml_path, "w", encoding="utf-8") as config_file:
            config_file.write("- just\n- a\n- list")

        with self.assertRaises(ValueError):
            Config(non_dict_yaml_path, self.logger)

    def test_get_top_level_key(self):
        """Test getting a top-level configuration key."""
        config = Config(self.config_path, self.logger)
        self.assertEqual(config.get("app_name"), "Finance Dashboard")
        self.assertEqual(config.get("version"), "1.0.0")

    def test_get_nested_key(self):
        """Test getting a nested configuration key."""
        config = Config(self.config_path, self.logger)
        self.assertEqual(config.get("database.host"), "localhost")
        self.assertEqual(config.get("database.port"), 5432)
        self.assertEqual(config.get("paths.input"), "input")

    def test_get_nonexistent_key(self):
        """Test getting a nonexistent configuration key."""
        config = Config(self.config_path, self.logger)
        with self.assertRaises(KeyError):
            config.get("nonexistent_key")

    def test_get_nonexistent_nested_key(self):
        """Test getting a nonexistent nested configuration key."""
        config = Config(self.config_path, self.logger)
        with self.assertRaises(KeyError):
            config.get("database.nonexistent")

    def test_get_empty_key(self):
        """Test getting an empty key."""
        config = Config(self.config_path, self.logger)
        with self.assertRaises(KeyError):
            config.get("")

    def test_logger_integration(self):
        """Test that the logger is properly integrated."""
        mock_logger = MagicMock(spec=logging.Logger)
        config = Config(self.config_path, mock_logger)

        # Verify info log was called during initialization
        mock_logger.info.assert_called_with(
            f"Configuration loaded from {self.config_path}"
        )

        # Test error logging for missing key
        with self.assertRaises(KeyError):
            config.get("nonexistent_key")

        # Verify error log was called
        mock_logger.error.assert_called_with(
            "Configuration key not found: nonexistent_key"
        )


# For running with pytest
def test_config_creation():
    """Test creating a Config instance using pytest."""
    with tempfile.TemporaryDirectory() as temp_dir:
        config_path = os.path.join(temp_dir, "test_config.yaml")
        sample_config = {"test_key": "test_value"}

        with open(config_path, "w", encoding="utf-8") as config_file:
            yaml.dump(sample_config, config_file)

        config = Config(config_path)
        assert config.get("test_key") == "test_value"


def test_nested_key_access():
    """Test accessing nested keys using pytest."""
    with tempfile.TemporaryDirectory() as temp_dir:
        config_path = os.path.join(temp_dir, "test_config.yaml")
        sample_config = {"nested": {"key1": "value1", "key2": {"subkey": "subvalue"}}}

        with open(config_path, "w", encoding="utf-8") as config_file:
            yaml.dump(sample_config, config_file)

        config = Config(config_path)
        assert config.get("nested.key1") == "value1"
        assert config.get("nested.key2.subkey") == "subvalue"


def test_error_handling():
    """Test error handling with pytest."""
    with tempfile.TemporaryDirectory() as temp_dir:
        config_path = os.path.join(temp_dir, "test_config.yaml")
        sample_config = {"test_key": "test_value"}

        with open(config_path, "w", encoding="utf-8") as config_file:
            yaml.dump(sample_config, config_file)

        config = Config(config_path)

        with pytest.raises(KeyError):
            config.get("missing_key")


if __name__ == "__main__":
    unittest.main()
