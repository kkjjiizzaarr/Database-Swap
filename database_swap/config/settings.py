"""Configuration management for database swap operations."""

import yaml
import os
from typing import Dict, Any, Optional
from pathlib import Path


class Config:
    """Configuration manager for database swap operations."""
    
    def __init__(self, config_file: Optional[str] = None):
        """Initialize configuration with optional config file."""
        self.config_file = config_file
        self.config = self._load_default_config()
        
        if config_file and os.path.exists(config_file):
            self._load_config_file(config_file)
    
    def _load_default_config(self) -> Dict[str, Any]:
        """Load default configuration."""
        return {
            'source': {
                'type': 'sqlite',
                'connection': {
                    'host': 'localhost',
                    'port': None,
                    'database': 'source.db',
                    'username': None,
                    'password': None
                }
            },
            'target': {
                'type': 'sqlite',
                'connection': {
                    'host': 'localhost',
                    'port': None,
                    'database': 'target.db',
                    'username': None,
                    'password': None
                }
            },
            'migration': {
                'batch_size': 1000,
                'rate_limit_delay': 0.1,
                'max_retries': 3,
                'timeout': 30,
                'tables': None  # None means all tables
            },
            'validation': {
                'strict_mode': True,
                'data_type_validation': True,
                'foreign_key_validation': False
            },
            'logging': {
                'level': 'INFO',
                'file': 'database_swap.log',
                'console': True
            }
        }
    
    def _load_config_file(self, config_file: str) -> None:
        """Load configuration from YAML file."""
        try:
            with open(config_file, 'r') as f:
                file_config = yaml.safe_load(f)
                self._merge_config(file_config)
        except Exception as e:
            raise ValueError(f"Failed to load config file {config_file}: {e}")
    
    def _merge_config(self, new_config: Dict[str, Any]) -> None:
        """Merge new configuration with existing configuration."""
        def merge_dict(base: Dict[str, Any], update: Dict[str, Any]) -> Dict[str, Any]:
            """Recursively merge dictionaries."""
            for key, value in update.items():
                if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                    base[key] = merge_dict(base[key], value)
                else:
                    base[key] = value
            return base
        
        self.config = merge_dict(self.config, new_config)
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by dot notation key."""
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def set(self, key: str, value: Any) -> None:
        """Set configuration value by dot notation key."""
        keys = key.split('.')
        config = self.config
        
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        config[keys[-1]] = value
    
    def get_source_config(self) -> Dict[str, Any]:
        """Get source database configuration."""
        return self.config.get('source', {})
    
    def get_target_config(self) -> Dict[str, Any]:
        """Get target database configuration."""
        return self.config.get('target', {})
    
    def get_migration_config(self) -> Dict[str, Any]:
        """Get migration configuration."""
        return self.config.get('migration', {})
    
    def get_validation_config(self) -> Dict[str, Any]:
        """Get validation configuration."""
        return self.config.get('validation', {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration."""
        return self.config.get('logging', {})
    
    def save_config(self, file_path: str) -> None:
        """Save current configuration to file."""
        try:
            with open(file_path, 'w') as f:
                yaml.dump(self.config, f, default_flow_style=False, indent=2)
        except Exception as e:
            raise ValueError(f"Failed to save config to {file_path}: {e}")
    
    def update_from_args(self, args: Dict[str, Any]) -> None:
        """Update configuration from command line arguments."""
        # Map command line arguments to config keys
        arg_mapping = {
            'source_type': 'source.type',
            'source_host': 'source.connection.host',
            'source_port': 'source.connection.port',
            'source_database': 'source.connection.database',
            'source_username': 'source.connection.username',
            'source_password': 'source.connection.password',
            'target_type': 'target.type',
            'target_host': 'target.connection.host',
            'target_port': 'target.connection.port',
            'target_database': 'target.connection.database',
            'target_username': 'target.connection.username',
            'target_password': 'target.connection.password',
            'batch_size': 'migration.batch_size',
            'rate_limit_delay': 'migration.rate_limit_delay',
            'max_retries': 'migration.max_retries',
            'timeout': 'migration.timeout',
            'tables': 'migration.tables',
            'log_level': 'logging.level',
            'log_file': 'logging.file'
        }
        
        for arg_key, config_key in arg_mapping.items():
            if arg_key in args and args[arg_key] is not None:
                self.set(config_key, args[arg_key])


def load_config(config_file: Optional[str] = None) -> Config:
    """Load configuration from file or use defaults."""
    # Look for config file in standard locations if not specified
    if not config_file:
        search_paths = [
            'config.yaml',
            'config.yml',
            os.path.expanduser('~/.database_swap.yaml'),
            '/etc/database_swap.yaml'
        ]
        
        for path in search_paths:
            if os.path.exists(path):
                config_file = path
                break
    
    return Config(config_file)