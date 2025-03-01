"""
Configuration module for the Finance Dashboard application.

This module provides a flexible configuration system that can load settings
from YAML files and provide typed access to configuration values.
"""

import os
import yaml
from typing import Any, Dict, Optional, Union, List


class Config:
    """
    Configuration management class for the Finance Dashboard application.

    This class loads and manages configuration settings from a YAML file,
    providing typed access to configuration values with support for nested
    keys and default values.

    Attributes:
        _config: Dictionary containing the loaded configuration
    """

    def __init__(self, source: str):
        """
        Initialize the Config object by loading configuration from a file.

        Args:
            source: Path to the configuration file

        Raises:
            FileNotFoundError: If the configuration file doesn't exist
            ValueError: If the configuration file is invalid
        """
        self._config = self._load_config(source)
        self._config_path = source

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
            raise FileNotFoundError(f"Configuration file not found: {file_path}")

        try:
            with open(file_path, "r") as file:
                config = yaml.safe_load(file)

            if not isinstance(config, dict):
                raise ValueError(f"Invalid configuration format in {file_path}")

            return config
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing configuration file {file_path}: {str(e)}")

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value by key with support for nested keys.

        Args:
            key: Configuration key (can be dot-separated for nested access)
            default: Default value to return if the key doesn't exist

        Returns:
            Any: The configuration value or the default if not found
        """
        if not key:
            return default

        # Split the key into parts for nested access
        keys = key.split(".")
        value = self._config

        try:
            for k in keys:
                value = value[k]
        except (KeyError, TypeError):
            return default

        return value

    def get_path(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """
        Get a file path from configuration, resolving relative paths.

        Args:
            key: Configuration key for the path
            default: Default path to return if the key doesn't exist

        Returns:
            Optional[str]: Absolute path or the default if not found
        """
        path = self.get(key, default)

        if not path:
            return default

        # If the path is relative, resolve it relative to the config file
        if not os.path.isabs(path):
            config_dir = os.path.dirname(os.path.abspath(self._config_path))
            path = os.path.join(config_dir, path)

        return path

    def get_list(self, key: str, default: Optional[List[Any]] = None) -> List[Any]:
        """
        Get a list from configuration.

        Args:
            key: Configuration key for the list
            default: Default list to return if the key doesn't exist

        Returns:
            List[Any]: The list from configuration or the default if not found
        """
        value = self.get(key, default)

        if value is None:
            return default or []

        if not isinstance(value, list):
            return [value]

        return value

    def get_dict(
        self, key: str, default: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Get a dictionary from configuration.

        Args:
            key: Configuration key for the dictionary
            default: Default dictionary to return if the key doesn't exist

        Returns:
            Dict[str, Any]: The dictionary from configuration or the default if not found
        """
        value = self.get(key, default)

        if value is None:
            return default or {}

        if not isinstance(value, dict):
            return default or {}

        return value

    def set(self, key: str, value: Any) -> None:
        """
        Set a configuration value.

        Args:
            key: Configuration key (can be dot-separated for nested access)
            value: Value to set
        """
        if not key:
            return

        keys = key.split(".")
        d = self._config

        for k in keys[:-1]:
            d = d.setdefault(k, {})

        d[keys[-1]] = value

    def save(self, file_path: Optional[str] = None) -> None:
        """
        Save the current configuration to a file.

        Args:
            file_path: Path to save the configuration (defaults to original path)

        Raises:
            ValueError: If the file can't be written
        """
        save_path = file_path or self._config_path

        try:
            os.makedirs(os.path.dirname(save_path), exist_ok=True)

            with open(save_path, "w") as file:
                yaml.dump(self._config, file, default_flow_style=False)
        except Exception as e:
            raise ValueError(f"Error saving configuration to {save_path}: {str(e)}")
