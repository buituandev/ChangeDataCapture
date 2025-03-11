import os
import json
import time

class ConfigManager:
    """Manages configuration loading and reloading from JSON file."""
    
    def __init__(self, config_path="/opt/src/config.json", refresh_interval=60):
        """Initialize config manager with path to config file and refresh interval."""
        self.config_path = config_path
        self.refresh_interval = refresh_interval  # seconds
        self.last_load_time = 0
        self.config = {}
        self._load_config()
    
    def _load_config(self):
        """Load configuration from JSON file."""
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r') as f:
                    self.config = json.load(f)
                self.last_load_time = time.time()
                print(f"Configuration loaded from {self.config_path}")
            else:
                print(f"Config file not found at {self.config_path}. Using defaults.")
                self._create_default_config()
        except Exception as e:
            print(f"Error loading config: {e}. Using defaults.")
            self._create_default_config()
    
    def _create_default_config(self):
        """Create and save default configuration."""
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
                "field_info_path": "/opt/src/field_info.json",
                "sql_history_path": "/opt/src/sql_history.csv"
            },
            "processing_config": {
                "key_column": "customerId",
                "process_time": "1 minute",
                "batch_size": 1000
            }
        }
        
        try:
            with open(self.config_path, 'w') as f:
                json.dump(self.config, f, indent=2)
            print(f"Default configuration saved to {self.config_path}")
        except Exception as e:
            print(f"Error saving default config: {e}")
    
    def get_config(self):
        """Get current configuration, reloading if refresh interval has passed."""
        if time.time() - self.last_load_time > self.refresh_interval:
            self._load_config()
        return self.config
    
    def get(self, section, key, default=None):
        """Get a specific configuration value."""
        self.get_config()  # Check if reload needed
        return self.config.get(section, {}).get(key, default)