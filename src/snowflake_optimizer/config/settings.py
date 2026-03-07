"""
Configuration settings for Snowflake Cost Optimizer.
"""

import os
import yaml
from pathlib import Path
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
from loguru import logger


class SnowflakeSettings(BaseModel):
    """Snowflake connection settings."""
    account: str = Field(..., description="Snowflake account identifier")
    user: str = Field(..., description="Snowflake username")
    password: Optional[str] = Field(None, description="Snowflake password")
    warehouse: str = Field(..., description="Default warehouse")
    database: str = Field(..., description="Default database")
    schema_name: str = Field(..., description="Default schema")
    role: Optional[str] = Field(None, description="Snowflake role")
    private_key_path: Optional[str] = Field(None, description="Path to private key file")
    private_key_passphrase: Optional[str] = Field(None, description="Private key passphrase")


class AnalysisSettings(BaseModel):
    """Analysis configuration settings."""
    lookback_days: int = Field(30, description="Days of historical data to analyze")
    min_warehouse_spend: float = Field(10.0, description="Minimum spend threshold for analysis")
    slow_query_threshold_ms: int = Field(10000, description="Threshold for slow queries in milliseconds")
    frequent_query_min_executions: int = Field(100, description="Minimum executions for frequent query analysis")
    high_spend_threshold: float = Field(1000.0, description="High spend alert threshold")
    unexpected_spike_percentage: float = Field(50.0, description="Unexpected spike percentage threshold")


class OptimizationSettings(BaseModel):
    """Optimization recommendation settings."""
    warehouse_utilization_threshold: float = Field(0.8, description="Warehouse utilization threshold")
    idle_time_threshold_minutes: int = Field(30, description="Idle time threshold in minutes")
    table_scan_threshold: int = Field(1000000, description="Table scan threshold in rows")
    unused_table_days: int = Field(90, description="Days before table considered unused")
    min_execution_count: int = Field(10, description="Minimum execution count for query optimization")
    performance_improvement_threshold: float = Field(0.3, description="Performance improvement threshold")


class CacheSettings(BaseModel):
    """Cache configuration settings."""
    ttl_hours: int = Field(1, description="Cache TTL in hours")
    max_entries: int = Field(1000, description="Maximum cache entries")


class Settings(BaseSettings):
    """Main application settings."""
    
    # Application settings
    app_name: str = Field("Snowflake Cost Optimizer", description="Application name")
    app_version: str = Field("1.0.0", description="Application version")
    debug: bool = Field(False, description="Debug mode")
    log_level: str = Field("INFO", description="Logging level")
    
    # Snowflake settings
    snowflake: SnowflakeSettings
    
    # Analysis settings
    analysis: AnalysisSettings = Field(default_factory=AnalysisSettings)
    
    # Optimization settings  
    optimization: OptimizationSettings = Field(default_factory=OptimizationSettings)
    
    # Cache settings
    cache: CacheSettings = Field(default_factory=CacheSettings)
    
    class Config:
        env_file = ".env"
        env_nested_delimiter = "__"
        case_sensitive = False


def load_config_from_yaml(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load configuration from YAML file."""
    if config_path is None:
        # Look for config.yaml in the project root
        project_root = Path(__file__).parent.parent.parent.parent
        config_path = project_root / "config" / "config.yaml"
    
    config_file = Path(config_path)
    if not config_file.exists():
        logger.warning(f"Config file not found: {config_path}")
        return {}
    
    try:
        with open(config_file, 'r') as f:
            config_data = yaml.safe_load(f)
        logger.info(f"Loaded configuration from {config_path}")
        return config_data or {}
    except Exception as e:
        logger.error(f"Error loading config file {config_path}: {e}")
        return {}


def create_settings() -> Settings:
    """Create settings instance with configuration from YAML and environment variables."""
    # Load YAML configuration
    yaml_config = load_config_from_yaml()
    
    # Extract nested configurations
    analysis_config = yaml_config.get('analysis', {})
    optimization_config = yaml_config.get('optimization', {})
    cache_config = yaml_config.get('cache', {})
    app_config = yaml_config.get('app', {})
    
    # Get Snowflake config from YAML (with defaults)
    snowflake_config = yaml_config.get('snowflake', {})
    
    # Create Snowflake settings with YAML defaults, but environment variables take precedence
    snowflake_settings = SnowflakeSettings(
        account=os.getenv('SNOWFLAKE_ACCOUNT') or snowflake_config.get('account', ''),
        user=os.getenv('SNOWFLAKE_USER') or snowflake_config.get('user', ''),
        password=os.getenv('SNOWFLAKE_PASSWORD') or snowflake_config.get('password'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE') or snowflake_config.get('warehouse', ''),
        database=os.getenv('SNOWFLAKE_DATABASE') or snowflake_config.get('database', ''),
        schema_name=os.getenv('SNOWFLAKE_SCHEMA') or snowflake_config.get('schema', ''),
        role=os.getenv('SNOWFLAKE_ROLE') or snowflake_config.get('role'),
        private_key_path=os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH') or snowflake_config.get('private_key_path'),
        private_key_passphrase=os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE') or snowflake_config.get('private_key_passphrase')
    )
    
    # Merge YAML and environment configurations
    settings_data = {
        'app_name': app_config.get('name', 'Snowflake Cost Optimizer'),
        'app_version': app_config.get('version', '1.0.0'),
        'debug': app_config.get('debug', False),
        'log_level': app_config.get('log_level', 'INFO'),
        'snowflake': snowflake_settings,
        'analysis': AnalysisSettings(**analysis_config),
        'optimization': OptimizationSettings(**optimization_config),
        'cache': CacheSettings(**cache_config)
    }
    
    return Settings(**settings_data)


# Global settings instance
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Get global settings instance."""
    global _settings
    if _settings is None:
        _settings = create_settings()
    return _settings 