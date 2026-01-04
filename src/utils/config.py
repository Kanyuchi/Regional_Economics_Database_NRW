"""
Configuration Management Module
Regional Economics Database for NRW

Handles loading and managing configuration from YAML files and environment variables.
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv


class ConfigManager:
    """Manages application configuration from YAML files and environment variables."""

    def __init__(self, config_dir: Optional[str] = None):
        """
        Initialize the configuration manager.

        Args:
            config_dir: Directory containing configuration files.
                       Defaults to project_root/config
        """
        # Load environment variables from .env file
        load_dotenv()

        # Determine config directory
        if config_dir is None:
            project_root = Path(__file__).parent.parent.parent
            self.config_dir = project_root / "config"
        else:
            self.config_dir = Path(config_dir)

        # Load all configurations
        self._database_config = None
        self._sources_config = None
        self._logging_config = None

    def _load_yaml(self, filename: str) -> Dict[str, Any]:
        """
        Load YAML configuration file.

        Args:
            filename: Name of the YAML file to load

        Returns:
            Dictionary containing configuration
        """
        filepath = self.config_dir / filename

        if not filepath.exists():
            raise FileNotFoundError(f"Configuration file not found: {filepath}")

        with open(filepath, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        # Substitute environment variables
        return self._substitute_env_vars(config)

    def _substitute_env_vars(self, config: Any) -> Any:
        """
        Recursively substitute environment variables in configuration.

        Replaces ${VAR_NAME} with the value of environment variable VAR_NAME.

        Args:
            config: Configuration dictionary or value

        Returns:
            Configuration with environment variables substituted
        """
        if isinstance(config, dict):
            return {key: self._substitute_env_vars(value)
                   for key, value in config.items()}
        elif isinstance(config, list):
            return [self._substitute_env_vars(item) for item in config]
        elif isinstance(config, str):
            # Replace ${VAR_NAME} with environment variable value
            if config.startswith('${') and config.endswith('}'):
                env_var = config[2:-1]
                return os.getenv(env_var, config)
            return config
        else:
            return config

    @property
    def database(self) -> Dict[str, Any]:
        """Get database configuration."""
        if self._database_config is None:
            self._database_config = self._load_yaml('database.yaml')
        return self._database_config

    @property
    def sources(self) -> Dict[str, Any]:
        """Get data sources configuration."""
        if self._sources_config is None:
            self._sources_config = self._load_yaml('sources.yaml')
        return self._sources_config

    @property
    def logging(self) -> Dict[str, Any]:
        """Get logging configuration."""
        if self._logging_config is None:
            self._logging_config = self._load_yaml('logging.yaml')
        return self._logging_config

    def get_db_connection_string(self, db_name: str = 'regional_economics') -> str:
        """
        Get database connection string.

        Args:
            db_name: Name of the database configuration to use

        Returns:
            PostgreSQL connection string
        """
        db_config = self.database['database'][db_name]

        return (
            f"postgresql://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )

    def get_db_params(self, db_name: str = 'regional_economics') -> Dict[str, Any]:
        """
        Get database connection parameters as dictionary.

        Args:
            db_name: Name of the database configuration to use

        Returns:
            Dictionary with connection parameters
        """
        return self.database['database'][db_name]

    def get_source_config(self, source_name: str) -> Dict[str, Any]:
        """
        Get configuration for a specific data source.

        Args:
            source_name: Name of the data source (regional_db, state_db, ba)

        Returns:
            Dictionary with source configuration
        """
        sources = self.sources['data_sources']

        if source_name not in sources:
            raise ValueError(f"Unknown data source: {source_name}")

        return sources[source_name]


# Global configuration instance
_config_instance = None


def get_config() -> ConfigManager:
    """
    Get global configuration instance (singleton).

    Returns:
        ConfigManager instance
    """
    global _config_instance

    if _config_instance is None:
        _config_instance = ConfigManager()

    return _config_instance
