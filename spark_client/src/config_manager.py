"""
Configuration management module for the CDC pipeline.

This module provides functionality to load, cache, and access configuration
settings from a JSON file with automatic refresh capabilities.
"""

import os
import json
import time
import logging
from typing import Dict, Any, Optional, Union


class ConfigError(Exception):
    """Exception raised for configuration-related errors."""
    pass


class ConfigManager:
    """
    Manages configuration loading and reloading from JSON file.
    
    This class provides a centralized way to access configuration values
    with automatic periodic refreshes to detect changes in the config file.
    It supports fallback to default values and handles file I/O exceptions.
    """
    
    def __init__(
        self, 
        config_path: str = "/opt/src/config.json", 
        refresh_interval: int = 60,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the configuration manager.
        
        Args:
            config_path: Path to the JSON configuration file
            refresh_interval: Time in seconds between configuration reloads
            logger: Optional logger instance; if not provided, a new one is created
            
        Raises:
            ConfigError: If critical configuration errors occur during initialization
        """
        self.config_path = config_path
        self.refresh_interval = refresh_interval
        self.last_load_time = 0
        self.config: Dict[str, Any] = {}
        
        # Set up logging
        self.logger = logger or self._setup_logger()
        
        # Initial configuration load
        self._load_config()
    
    def _setup_logger(self) -> logging.Logger:
        """
        Set up and configure a logger for this class.
        
        Returns:
            logging.Logger: Configured logger instance
        """
        logger = logging.getLogger("ConfigManager")
        
        # Only add handler if not already configured
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        
        return logger
    
    def _load_config(self) -> None:
        """
        Load configuration from JSON file.
        
        If the file doesn't exist or can't be parsed, default configuration is used.
        """
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r') as f:
                    config_data = json.load(f)
                
                # Validate the loaded config
                if self._validate_config(config_data):
                    self.config = config_data
                    self.last_load_time = time.time()
                    self.logger.info(f"Configuration loaded from {self.config_path}")
                else:
                    self.logger.warning("Invalid configuration format. Using defaults.")
                    self._create_default_config()
            else:
                self.logger.warning(f"Config file not found at {self.config_path}. Using defaults.")
                self._create_default_config()
        except json.JSONDecodeError:
            self.logger.error(f"Invalid JSON in config file {self.config_path}. Using defaults.")
            self._create_default_config()
        except Exception as e:
            self.logger.error(f"Error loading config: {str(e)}. Using defaults.")
            self._create_default_config()
    
    def _validate_config(self, config: Dict[str, Any]) -> bool:
        """
        Validate the structure and required fields in the configuration.
        
        Args:
            config: The configuration dictionary to validate
            
        Returns:
            bool: True if configuration is valid, False otherwise
        """
        required_sections = [
            "s3_config", "delta_config", "kafka_config", 
            "cache_config", "processing_config"
        ]
        
        # Check if all required sections exist
        for section in required_sections:
            if section not in config:
                self.logger.error(f"Missing required section: {section}")
                return False
        
        return True
    
    def _create_default_config(self) -> None:
        """
        Create and save default configuration.
        
        This method creates a default configuration and attempts to save it to
        the configured file path.
        """
        self.config = {
            "s3_config": {
                "access_key_id": "12345678",
                "secret_access_key": "12345678",
                "endpoint": "http://minio:9000",
                "path_style_access": True,
                "ssl_enabled": False
            },
            "delta_config": {
                "output_path": "s3a://change-data-capture/customers-delta",
                "checkpoint_dir": "s3a://change-data-capture/checkpoint"
            },
            "kafka_config": {
                "bootstrap_servers": "kafka:9092",
                "topic": "dbserver2.public.links",
                "fail_on_data_loss": False
            },
            "cache_config": {
                "schema_path": "/opt/src/schema.json",
                "field_info_path": "/opt/src/field_info.json"
            },
            "processing_config": {
                "key_column": "customerId",
                "process_time": "1 minute",
                "batch_size": 1000
            }
        }
        
        try:
            os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
            with open(self.config_path, 'w') as f:
                json.dump(self.config, f, indent=2)
            self.logger.info(f"Default configuration saved to {self.config_path}")
        except Exception as e:
            self.logger.error(f"Error saving default config: {str(e)}")
    
    def get_config(self) -> Dict[str, Any]:
        """
        Get current configuration, reloading if refresh interval has passed.
        
        Returns:
            Dict[str, Any]: Complete configuration dictionary
        """
        if time.time() - self.last_load_time > self.refresh_interval:
            self._load_config()
        return self.config
    
    def get(self, section: str, key: str, default: Any = None) -> Any:
        """
        Get a specific configuration value with fallback to default.
        
        Args:
            section: Configuration section name
            key: Configuration key within the section
            default: Default value if section or key doesn't exist
            
        Returns:
            Any: The configuration value or default
        """
        self.get_config()  # Check if reload needed
        
        if section not in self.config:
            self.logger.warning(f"Configuration section '{section}' not found. Using default.")
            return default
            
        if key not in self.config[section]:
            self.logger.warning(f"Key '{key}' not found in section '{section}'. Using default.")
            return default
            
        return self.config[section][key]
    
    def set(self, section: str, key: str, value: Any) -> bool:
        """
        Set a configuration value and save to file.
        
        Args:
            section: Configuration section name
            key: Configuration key within the section
            value: Value to set
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if section not in self.config:
                self.config[section] = {}
            
            self.config[section][key] = value
            
            with open(self.config_path, 'w') as f:
                json.dump(self.config, f, indent=2)
                
            self.last_load_time = time.time()
            self.logger.info(f"Updated configuration: {section}.{key}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to update configuration: {str(e)}")
            return False