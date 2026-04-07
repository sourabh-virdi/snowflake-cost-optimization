"""
Unit tests for configuration and settings management.
"""

import pytest
import os
import tempfile
import yaml
from pathlib import Path
from unittest.mock import patch, mock_open

import sys
sys.path.append(str(Path(__file__).parent.parent.parent / "src"))

from snowflake_optimizer.config.settings import (
    SnowflakeSettings, 
    DatabaseSettings, 
    UISettings, 
    CacheSettings,
    Settings,
    get_settings,
    create_settings
)


class TestSnowflakeSettings:
    """Test Snowflake connection settings."""
    
    def test_snowflake_settings_creation_with_password(self):
        """Test creating Snowflake settings with password authentication."""
        settings = SnowflakeSettings(
            account="test_account",
            user="test_user", 
            password="test_password",
            warehouse="test_warehouse",
            database="test_database",
            schema_name="test_schema"
        )
        
        assert settings.account == "test_account"
        assert settings.user == "test_user"
        assert settings.password == "test_password"
        assert settings.warehouse == "test_warehouse"
        assert settings.database == "test_database"
        assert settings.schema_name == "test_schema"
        assert settings.role is None
        assert settings.private_key_path is None
    
    def test_snowflake_settings_creation_with_key_pair(self):
        """Test creating Snowflake settings with key pair authentication."""
        settings = SnowflakeSettings(
            account="test_account",
            user="test_user",
            private_key_path="/path/to/key.p8",
            private_key_passphrase="passphrase",
            warehouse="test_warehouse", 
            database="test_database",
            schema_name="test_schema"
        )
        
        assert settings.account == "test_account"
        assert settings.user == "test_user"
        assert settings.password is None
        assert settings.private_key_path == "/path/to/key.p8"
        assert settings.private_key_passphrase == "passphrase"
    
    def test_snowflake_settings_missing_required_fields(self):
        """Test that missing required fields raise validation errors."""
        with pytest.raises(Exception):  # Pydantic validation error
            SnowflakeSettings(
                account="test_account",
                # Missing user
                password="test_password",
                warehouse="test_warehouse",
                database="test_database",
                schema_name="test_schema"
            )


class TestDatabaseSettings:
    """Test database configuration settings."""
    
    def test_database_settings_creation(self):
        """Test creating database settings with defaults."""
        settings = DatabaseSettings()
        
        assert settings.connection_timeout == 60
        assert settings.query_timeout == 300
        assert settings.max_retries == 3
    
    def test_database_settings_custom_values(self):
        """Test creating database settings with custom values."""
        settings = DatabaseSettings(
            connection_timeout=120,
            query_timeout=600,
            max_retries=5
        )
        
        assert settings.connection_timeout == 120
        assert settings.query_timeout == 600
        assert settings.max_retries == 5


class TestUISettings:
    """Test UI configuration settings."""
    
    def test_ui_settings_creation(self):
        """Test creating UI settings with defaults."""
        settings = UISettings()
        
        assert settings.page_title == "Snowflake Cost Optimizer"
        assert settings.page_icon == "SF"
        assert settings.layout == "wide"
        assert settings.theme == "light"
    
    def test_ui_settings_custom_values(self):
        """Test creating UI settings with custom values."""
        settings = UISettings(
            page_title="Custom Title",
            page_icon="🔥",
            layout="centered",
            theme="dark"
        )
        
        assert settings.page_title == "Custom Title"
        assert settings.page_icon == "🔥"
        assert settings.layout == "centered"
        assert settings.theme == "dark"


class TestCacheSettings:
    """Test cache configuration settings."""
    
    def test_cache_settings_creation(self):
        """Test creating cache settings with defaults."""
        settings = CacheSettings()
        
        assert settings.enabled is True
        assert settings.default_ttl_minutes == 60
        assert settings.max_size_mb == 100
        assert settings.cleanup_interval_minutes == 30
    
    def test_cache_settings_custom_values(self):
        """Test creating cache settings with custom values."""
        settings = CacheSettings(
            enabled=False,
            default_ttl_minutes=120,
            max_size_mb=500,
            cleanup_interval_minutes=60
        )
        
        assert settings.enabled is False
        assert settings.default_ttl_minutes == 120
        assert settings.max_size_mb == 500
        assert settings.cleanup_interval_minutes == 60


class TestSettings:
    """Test main Settings class."""
    
    def test_settings_creation_with_defaults(self):
        """Test creating main settings with default values."""
        snowflake_config = SnowflakeSettings(
            account="test_account",
            user="test_user",
            password="test_password",
            warehouse="test_warehouse",
            database="test_database", 
            schema_name="test_schema"
        )
        
        settings = Settings(snowflake=snowflake_config)
        
        assert settings.snowflake == snowflake_config
        assert isinstance(settings.database, DatabaseSettings)
        assert isinstance(settings.ui, UISettings)
        assert isinstance(settings.cache, CacheSettings)
    
    def test_settings_creation_with_custom_values(self):
        """Test creating main settings with custom values."""
        snowflake_config = SnowflakeSettings(
            account="test_account",
            user="test_user",
            password="test_password",
            warehouse="test_warehouse",
            database="test_database",
            schema_name="test_schema"
        )
        
        database_config = DatabaseSettings(connection_timeout=120)
        ui_config = UISettings(page_title="Custom Title")
        cache_config = CacheSettings(enabled=False)
        
        settings = Settings(
            snowflake=snowflake_config,
            database=database_config,
            ui=ui_config,
            cache=cache_config
        )
        
        assert settings.snowflake == snowflake_config
        assert settings.database == database_config
        assert settings.ui == ui_config
        assert settings.cache == cache_config


class TestCreateSettings:
    """Test settings creation from various sources."""
    
    def test_create_settings_from_env_vars(self):
        """Test creating settings from environment variables."""
        env_vars = {
            'SNOWFLAKE_ACCOUNT': 'env_account',
            'SNOWFLAKE_USER': 'env_user',
            'SNOWFLAKE_PASSWORD': 'env_password',
            'SNOWFLAKE_WAREHOUSE': 'env_warehouse',
            'SNOWFLAKE_DATABASE': 'env_database',
            'SNOWFLAKE_SCHEMA': 'env_schema',
            'SNOWFLAKE_ROLE': 'env_role'
        }
        
        with patch.dict(os.environ, env_vars):
            settings = create_settings()
            
            assert settings.snowflake.account == 'env_account'
            assert settings.snowflake.user == 'env_user'
            assert settings.snowflake.password == 'env_password'
            assert settings.snowflake.warehouse == 'env_warehouse'
            assert settings.snowflake.database == 'env_database'
            assert settings.snowflake.schema_name == 'env_schema'
            assert settings.snowflake.role == 'env_role'
    
    def test_create_settings_from_config_file(self):
        """Test creating settings from YAML config file."""
        config_data = {
            'snowflake': {
                'account': 'yaml_account',
                'user': 'yaml_user',
                'password': 'yaml_password',
                'warehouse': 'yaml_warehouse',
                'database': 'yaml_database',
                'schema': 'yaml_schema'
            },
            'ui': {
                'page_title': 'YAML Title'
            },
            'cache': {
                'enabled': False
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp_file:
            yaml.dump(config_data, tmp_file)
            tmp_file_path = tmp_file.name
        
        try:
            with patch('snowflake_optimizer.config.settings.Path') as mock_path:
                mock_path.return_value.exists.return_value = True
                with patch('builtins.open', mock_open(read_data=yaml.dump(config_data))):
                    settings = create_settings()
                    
                    # Environment variables should take precedence
                    assert settings.ui.page_title == 'YAML Title'
                    assert settings.cache.enabled is False
        finally:
            os.unlink(tmp_file_path)
    
    def test_create_settings_env_override_config(self):
        """Test that environment variables override config file values."""
        config_data = {
            'snowflake': {
                'account': 'yaml_account',
                'user': 'yaml_user',
                'password': 'yaml_password',
                'warehouse': 'yaml_warehouse',
                'database': 'yaml_database',
                'schema': 'yaml_schema'
            }
        }
        
        env_vars = {
            'SNOWFLAKE_ACCOUNT': 'env_account_override',
            'SNOWFLAKE_USER': 'env_user_override'
        }
        
        with patch.dict(os.environ, env_vars):
            with patch('snowflake_optimizer.config.settings.Path') as mock_path:
                mock_path.return_value.exists.return_value = True
                with patch('builtins.open', mock_open(read_data=yaml.dump(config_data))):
                    settings = create_settings()
                    
                    # Environment variables should override config file
                    assert settings.snowflake.account == 'env_account_override'
                    assert settings.snowflake.user == 'env_user_override'
    
    def test_create_settings_missing_config_file(self):
        """Test creating settings when config file doesn't exist."""
        env_vars = {
            'SNOWFLAKE_ACCOUNT': 'env_account',
            'SNOWFLAKE_USER': 'env_user',
            'SNOWFLAKE_PASSWORD': 'env_password',
            'SNOWFLAKE_WAREHOUSE': 'env_warehouse',
            'SNOWFLAKE_DATABASE': 'env_database',
            'SNOWFLAKE_SCHEMA': 'env_schema'
        }
        
        with patch.dict(os.environ, env_vars):
            with patch('snowflake_optimizer.config.settings.Path') as mock_path:
                mock_path.return_value.exists.return_value = False
                settings = create_settings()
                
                assert settings.snowflake.account == 'env_account'
                assert settings.snowflake.user == 'env_user'


class TestGetSettings:
    """Test settings singleton pattern."""
    
    def test_get_settings_singleton(self):
        """Test that get_settings returns the same instance."""
        env_vars = {
            'SNOWFLAKE_ACCOUNT': 'test_account',
            'SNOWFLAKE_USER': 'test_user',
            'SNOWFLAKE_PASSWORD': 'test_password',
            'SNOWFLAKE_WAREHOUSE': 'test_warehouse',
            'SNOWFLAKE_DATABASE': 'test_database',
            'SNOWFLAKE_SCHEMA': 'test_schema'
        }
        
        with patch.dict(os.environ, env_vars):
            settings1 = get_settings()
            settings2 = get_settings()
            
            assert settings1 is settings2
    
    def test_get_settings_force_reload(self):
        """Test forcing reload of settings."""
        env_vars = {
            'SNOWFLAKE_ACCOUNT': 'test_account',
            'SNOWFLAKE_USER': 'test_user',
            'SNOWFLAKE_PASSWORD': 'test_password',
            'SNOWFLAKE_WAREHOUSE': 'test_warehouse',
            'SNOWFLAKE_DATABASE': 'test_database',
            'SNOWFLAKE_SCHEMA': 'test_schema'
        }
        
        with patch.dict(os.environ, env_vars):
            settings1 = get_settings()
            settings2 = get_settings(force_reload=True)
            
            # Should be different instances after force reload
            assert settings1 is not settings2
            # But should have same values
            assert settings1.snowflake.account == settings2.snowflake.account


if __name__ == "__main__":
    pytest.main([__file__]) 