#!filepath: src/config.py
"""
Configuration module for the Finance Dashboard application.

This module provides a configuration system that loads settings
from YAML files with required keys and logging support.
"""

import os
import logging
from typing import Any, Dict
import yaml


class Config:
    """
    Configuration management class for the Finance Dashboard application.

    This class loads and manages configuration settings from a YAML file,
    providing access to configuration values with support for nested
    keys and strict validation.

    Attributes:
        _config: Dictionary containing the loaded configuration
        _config_path: Path to the configuration file
        logger: Logger instance
    """

    def __init__(self, source: str, logger: logging.Logger = None):
        """
        Initialize the Config object by loading configuration from a file.

        Args:
            source: Path to the configuration file
            logger: Logger instance from the main application

        Raises:
            FileNotFoundError: If the configuration file doesn't exist
            ValueError: If the configuration file is invalid
        """
        self.logger = logger or logging.getLogger(__name__)
        self._config_path = source
        self._config = self._load_config(source)
        self.logger.info(f"Configuration loaded from {source}")

    def _load_config(self, file_path: str) -> Dict[str, Any]:
        """
        Load configuration from a YAML file.

        Args:
            file_path: Path to the configuration file

        Returns:
            Dict[str, Any]: Dictionary containing the loaded configuration

        Raises:
            FileNotFoundError: If the configuration file doesn't exist
            ValueError: If the configuration file is invalid
        """
        if not os.path.exists(file_path):
            error_msg = f"Configuration file not found: {file_path}"
            self.logger.error(error_msg)
            raise FileNotFoundError(error_msg)

        try:
            with open(file_path, "r", encoding="utf-8") as file:
                config = yaml.safe_load(file)

            if not isinstance(config, dict):
                error_msg = f"Invalid configuration format in {file_path}"
                self.logger.error(error_msg)
                raise ValueError(error_msg)

            return config
        except yaml.YAMLError as e:
            error_msg = f"Error parsing configuration file {file_path}: {str(e)}"
            self.logger.error(error_msg)
            raise ValueError(error_msg) from e

    def get(self, key: str) -> Any:
        """
        Get a configuration value by key with support for nested keys.

        Args:
            key: Configuration key (can be dot-separated for nested access)

        Returns:
            Any: The configuration value

        Raises:
            KeyError: If the key doesn't exist in the configuration
        """
        if not key:
            error_msg = "Empty key provided"
            self.logger.error(error_msg)
            raise KeyError(error_msg)

        # Split the key into parts for nested access
        keys = key.split(".")
        value = self._config

        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError) as exc:
            error_msg = f"Configuration key not found: {key}"
            self.logger.error(error_msg)
            raise KeyError(error_msg) from exc

    def is_valid(self) -> bool:
        """
        Check if the configuration is valid.

        Returns:
            bool: True if the configuration is valid
        """
        return bool(self._config)
