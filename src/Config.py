#!/filepath: config.py
"""
Configuration module for the finance application.

This module handles loading configuration from YAML files and
providing access to configuration values throughout the application.
"""

import logging
import os
from typing import Any, Dict, Optional, TypeVar, cast

# Add type ignore for yaml module until stubs are installed
import yaml  # type: ignore

T = TypeVar("T")


class Config:
    """
    Configuration handler class for the finance application.

    Loads configuration values from a YAML file and provides
    methods to access those values.
    """

    def __init__(
        self, config_path: str = "config.yaml", logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the configuration.

        Args:
            config_path: Path to the YAML configuration file
            logger: Logger instance for logging configuration events
        """
        self.config_path = config_path
        self.logger = logger or logging.getLogger(__name__)
        self._config_data: Dict[str, Any] = {}
        self._load_config()

    def _load_config(self) -> None:
        """
        Load configuration from the YAML file.

        Raises:
            FileNotFoundError: If the configuration file doesn't exist
            yaml.YAMLError: If the YAML file is invalid
        """
        if not os.path.exists(self.config_path):
            self.logger.error(f"Config file not found: {self.config_path}")
            raise FileNotFoundError(f"Config file not found: {self.config_path}")

        self.logger.info(f"Loading configuration from {self.config_path}")
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                self._config_data = yaml.safe_load(f)
            self.logger.info("Configuration loaded successfully")
        except yaml.YAMLError as e:
            self.logger.error(f"Error parsing YAML configuration: {str(e)}")
            raise

    def get(self, key: str, default: Optional[T] = None) -> Any:
        """
        Get a configuration value by key.

        Args:
            key: The configuration key to look up
            default: Default value to return if key is not found

        Returns:
            The configuration value or the default if not found
        """
        value = self._config_data.get(key, default)
        return cast(T, value) if value is not None else default

    def set(self, key: str, value: Any) -> None:
        """
        Set a configuration value.

        This changes the value in memory but does not update the YAML file.

        Args:
            key: The configuration key to set
            value: The value to set
        """
        self._config_data[key] = value
        self.logger.debug(f"Set configuration {key}={value}")

    def save(self) -> None:
        """
        Save the current configuration to the YAML file.

        Raises:
            IOError: If the file cannot be written
        """
        self.logger.info(f"Saving configuration to {self.config_path}")
        try:
            with open(self.config_path, "w", encoding="utf-8") as f:
                yaml.dump(self._config_data, f, default_flow_style=False)
            self.logger.info("Configuration saved successfully")
        except Exception as e:
            self.logger.error(f"Error saving configuration: {str(e)}")
            raise IOError(f"Error saving configuration: {str(e)}") from e
