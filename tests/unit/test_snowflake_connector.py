"""
Unit tests for Snowflake connector and caching functionality.
"""

import pytest
import pandas as pd
import tempfile
import os
import json
import pickle
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

import sys
sys.path.append(str(Path(__file__).parent.parent.parent / "src"))

from snowflake_optimizer.connectors.snowflake_connector import (
    SnowflakeConnector, 
    QueryCache,
    QUERY_CACHE_TTL,
    DEFAULT_QUERY_LIMIT,
    DEFAULT_ANALYSIS_DAYS,
    UNKNOWN_VALUE
)
from snowflake_optimizer.config.settings import SnowflakeSettings


class TestQueryCache:
    """Test the QueryCache functionality."""
    
    @pytest.fixture
    def temp_cache_dir(self):
        """Create a temporary directory for cache testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)
    
    @pytest.fixture
    def cache(self, temp_cache_dir):
        """Create a QueryCache instance for testing."""
        return QueryCache(cache_dir=temp_cache_dir)
    
    def test_cache_initialization(self, temp_cache_dir):
        """Test cache initialization."""
        cache = QueryCache(cache_dir=temp_cache_dir)
        
        assert cache.cache_dir == temp_cache_dir
        assert cache.cache_index_file == temp_cache_dir / "cache_index.json"
        assert cache.cache_index == {}
    
    def test_generate_cache_key(self, cache):
        """Test cache key generation."""
        query = "SELECT * FROM table WHERE date > '2023-01-01'"
        params = {'days': 30, 'warehouse': 'COMPUTE_WH'}
        
        key1 = cache._generate_cache_key(query, params)
        key2 = cache._generate_cache_key(query, params)
        
        # Same inputs should generate same key
        assert key1 == key2
        assert len(key1) == 32  # MD5 hash length
        
        # Different inputs should generate different keys
        key3 = cache._generate_cache_key(query + " LIMIT 10", params)
        assert key1 != key3
    
    def test_cache_set_and_get(self, cache):
        """Test setting and getting cached data."""
        query = "SELECT * FROM test_table"
        params = {'test': 'value'}
        data = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
        cache_type = 'test_query'
        
        # Set cache
        cache.set(query, params, data, cache_type)
        
        # Get cache
        result = cache.get(query, params, cache_type)
        
        assert result is not None
        pd.testing.assert_frame_equal(result, data)
    
    def test_cache_expiration(self, cache):
        """Test cache expiration functionality."""
        query = "SELECT * FROM test_table"
        params = {'test': 'value'}
        data = pd.DataFrame({'col1': [1, 2, 3]})
        cache_type = 'test_query'
        
        # Set cache with very short TTL
        with patch.dict(QUERY_CACHE_TTL, {'test_query': 0.001}):  # 0.001 minutes
            cache.set(query, params, data, cache_type)
            
            # Should get data immediately
            result = cache.get(query, params, cache_type)
            assert result is not None
            
            # Wait for expiration (simulate)
            import time
            time.sleep(0.1)
            
            # Should return None after expiration
            result = cache.get(query, params, cache_type)
            assert result is None
    
    def test_cache_clear_expired(self, cache):
        """Test clearing expired cache entries."""
        # Add some test data
        for i in range(3):
            query = f"SELECT * FROM table_{i}"
            data = pd.DataFrame({'col': [i]})
            cache.set(query, {}, data, 'test_query')
        
        # Manually expire one entry
        cache_key = list(cache.cache_index.keys())[0]
        cache.cache_index[cache_key]['expiry'] = datetime.now() - timedelta(hours=1)
        cache._save_cache_index()
        
        # Clear expired
        removed_count = cache.clear_expired()
        
        assert removed_count == 1
        assert len(cache.cache_index) == 2
    
    def test_cache_clear_all(self, cache):
        """Test clearing all cache entries."""
        # Add some test data
        for i in range(3):
            query = f"SELECT * FROM table_{i}"
            data = pd.DataFrame({'col': [i]})
            cache.set(query, {}, data, 'test_query')
        
        assert len(cache.cache_index) == 3
        
        # Clear all
        cache.clear_all()
        
        assert len(cache.cache_index) == 0
    
    def test_cache_stats(self, cache):
        """Test cache statistics."""
        # Initially empty
        stats = cache.get_cache_stats()
        assert stats['total_entries'] == 0
        assert stats['valid_entries'] == 0
        assert stats['expired_entries'] == 0
        
        # Add some entries
        for i in range(3):
            query = f"SELECT * FROM table_{i}"
            data = pd.DataFrame({'col': [i]})
            cache.set(query, {}, data, 'test_query')
        
        # Expire one entry manually
        cache_key = list(cache.cache_index.keys())[0]
        cache.cache_index[cache_key]['expiry'] = datetime.now() - timedelta(hours=1)
        
        stats = cache.get_cache_stats()
        assert stats['total_entries'] == 3
        assert stats['valid_entries'] == 2
        assert stats['expired_entries'] == 1


class TestSnowflakeConnector:
    """Test the SnowflakeConnector class."""
    
    @pytest.fixture
    def mock_settings(self):
        """Create mock Snowflake settings."""
        return SnowflakeSettings(
            account="test_account",
            user="test_user",
            password="test_password",
            warehouse="test_warehouse",
            database="test_database",
            schema_name="test_schema"
        )
    
    @pytest.fixture
    def temp_cache_dir(self):
        """Create a temporary directory for cache testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)
    
    @pytest.fixture
    def connector(self, mock_settings, temp_cache_dir):
        """Create a SnowflakeConnector instance for testing."""
        with patch('snowflake_optimizer.connectors.snowflake_connector.snowflake.connector.connect'):
            connector = SnowflakeConnector(mock_settings, cache_dir=temp_cache_dir)
            return connector
    
    def test_connector_initialization(self, mock_settings, temp_cache_dir):
        """Test connector initialization."""
        with patch('snowflake_optimizer.connectors.snowflake_connector.snowflake.connector.connect'):
            connector = SnowflakeConnector(mock_settings, cache_dir=temp_cache_dir)
            
            assert connector.settings == mock_settings
            assert connector.enable_cache is True
            assert isinstance(connector.cache, QueryCache)
    
    def test_connector_initialization_no_cache(self, mock_settings):
        """Test connector initialization with cache disabled."""
        with patch('snowflake_optimizer.connectors.snowflake_connector.snowflake.connector.connect'):
            connector = SnowflakeConnector(mock_settings, enable_cache=False)
            
            assert connector.enable_cache is False
            assert connector.cache is None
    
    @patch('snowflake_optimizer.connectors.snowflake_connector.snowflake.connector.connect')
    def test_test_connection_success(self, mock_connect, connector):
        """Test successful connection test."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value = None
        mock_cursor.fetchone.return_value = [1]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        result = connector.test_connection()
        
        assert result is True
        mock_cursor.execute.assert_called_once_with("SELECT 1")
    
    @patch('snowflake_optimizer.connectors.snowflake_connector.snowflake.connector.connect')
    def test_test_connection_failure(self, mock_connect, connector):
        """Test connection test failure."""
        mock_connect.side_effect = Exception("Connection failed")
        
        result = connector.test_connection()
        
        assert result is False
    
    def test_execute_query_with_cache_hit(self, connector):
        """Test query execution with cache hit."""
        query = "SELECT * FROM test_table"
        expected_data = pd.DataFrame({'col1': [1, 2, 3]})
        
        # Mock cache hit
        connector.cache.get = Mock(return_value=expected_data)
        
        result = connector.execute_query(query, cache_key_type='test_query')
        
        pd.testing.assert_frame_equal(result, expected_data)
        connector.cache.get.assert_called_once()
    
    def test_execute_query_with_cache_miss(self, connector):
        """Test query execution with cache miss."""
        query = "SELECT * FROM test_table"
        expected_data = pd.DataFrame({'col1': [1, 2, 3]})
        
        # Mock cache miss and database execution
        connector.cache.get = Mock(return_value=None)
        connector.cache.set = Mock()
        
        mock_cursor = MagicMock()
        mock_cursor.fetch_pandas_all.return_value = expected_data
        
        with patch.object(connector, '_get_cursor', return_value=mock_cursor):
            result = connector.execute_query(query, cache_key_type='test_query')
        
        pd.testing.assert_frame_equal(result, expected_data)
        connector.cache.get.assert_called_once()
        connector.cache.set.assert_called_once()
    
    def test_execute_query_no_cache(self, mock_settings):
        """Test query execution without cache."""
        with patch('snowflake_optimizer.connectors.snowflake_connector.snowflake.connector.connect'):
            connector = SnowflakeConnector(mock_settings, enable_cache=False)
            
            query = "SELECT * FROM test_table"
            expected_data = pd.DataFrame({'col1': [1, 2, 3]})
            
            mock_cursor = MagicMock()
            mock_cursor.fetch_pandas_all.return_value = expected_data
            
            with patch.object(connector, '_get_cursor', return_value=mock_cursor):
                result = connector.execute_query(query)
            
            pd.testing.assert_frame_equal(result, expected_data)
    
    def test_column_name_normalization(self, connector):
        """Test that column names are normalized to lowercase."""
        query = "SELECT * FROM test_table"
        raw_data = pd.DataFrame({'COL1': [1, 2, 3], 'Col2': ['a', 'b', 'c']})
        expected_data = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
        
        connector.cache.get = Mock(return_value=None)
        connector.cache.set = Mock()
        
        mock_cursor = MagicMock()
        mock_cursor.fetch_pandas_all.return_value = raw_data
        
        with patch.object(connector, '_get_cursor', return_value=mock_cursor):
            result = connector.execute_query(query, cache_key_type='test_query')
        
        pd.testing.assert_frame_equal(result, expected_data)
    
    def test_check_table_columns(self, connector):
        """Test table column detection."""
        table_name = "test_table"
        expected_columns = ['col1', 'col2', 'col3']
        mock_data = pd.DataFrame({col: [1] for col in expected_columns})
        
        with patch.object(connector, 'execute_query', return_value=mock_data) as mock_execute:
            result = connector._check_table_columns(table_name)
        
        assert result == expected_columns
        mock_execute.assert_called_once()
    
    def test_find_column_mapping(self, connector):
        """Test column mapping functionality."""
        available_cols = ['database_name', 'table_catalog', 'row_count', 'active_bytes']
        column_mappings = {
            'database': ['database_name', 'table_catalog'],
            'rows': ['row_count', 'table_rows'],
            'storage': ['active_bytes', 'storage_bytes']
        }
        
        result = connector._find_column_mapping(available_cols, column_mappings)
        
        expected = {
            'database': 'database_name',
            'rows': 'row_count', 
            'storage': 'active_bytes'
        }
        
        assert result == expected
    
    def test_build_dynamic_select(self, connector):
        """Test dynamic SELECT clause building."""
        columns_config = [
            {'column': 'existing_col', 'alias': 'alias1', 'default': "'default1'", 'coalesce': False},
            {'column': None, 'alias': 'alias2', 'default': '0', 'coalesce': False},
            {'column': 'another_col', 'alias': 'alias3', 'default': "'fallback'", 'coalesce': True}
        ]
        
        result = connector._build_dynamic_select(columns_config)
        
        expected = [
            "existing_col as alias1",
            "0 as alias2", 
            "COALESCE(another_col, 'fallback') as alias3"
        ]
        
        assert result == expected
    
    def test_get_warehouse_usage(self, connector):
        """Test warehouse usage data retrieval."""
        mock_data = pd.DataFrame({
            'warehouse_name': ['WH1', 'WH2'],
            'total_credits': [100.0, 200.0],
            'usage_count': [50, 75],
            'total_runtime_minutes': [1200, 1800]
        })
        
        with patch.object(connector, 'execute_query', return_value=mock_data) as mock_execute:
            result = connector.get_warehouse_usage(days=30)
        
        pd.testing.assert_frame_equal(result, mock_data)
        mock_execute.assert_called_once()
        
        # Check that the query contains expected elements
        call_args = mock_execute.call_args
        query = call_args[0][0]
        assert 'warehouse_metering_history' in query
        assert 'DATEDIFF' in query
    
    def test_get_storage_usage_dynamic_columns(self, connector):
        """Test storage usage with dynamic column detection."""
        # Mock available columns
        available_columns = ['table_name', 'database_name', 'active_bytes', 'row_count']
        mock_schema_data = pd.DataFrame({col: [1] for col in available_columns})
        
        # Mock final storage data
        mock_storage_data = pd.DataFrame({
            'table_name': ['table1', 'table2'],
            'database_name': ['db1', 'db2'],
            'active_bytes': [1000000, 2000000],
            'row_count': [100, 200]
        })
        
        with patch.object(connector, 'execute_query') as mock_execute:
            # First call for schema check
            # Second call for actual data
            mock_execute.side_effect = [mock_schema_data, mock_storage_data]
            
            result = connector.get_storage_usage()
        
        assert len(mock_execute.call_args_list) == 2
        pd.testing.assert_frame_equal(result, mock_storage_data)
    
    def test_clear_cache(self, connector):
        """Test cache clearing functionality."""
        connector.cache.clear_all = Mock()
        
        connector.clear_cache()
        
        connector.cache.clear_all.assert_called_once()
    
    def test_get_cache_stats(self, connector):
        """Test cache statistics retrieval."""
        expected_stats = {'total_entries': 5, 'valid_entries': 3}
        connector.cache.get_cache_stats = Mock(return_value=expected_stats)
        
        result = connector.get_cache_stats()
        
        assert result == expected_stats
        connector.cache.get_cache_stats.assert_called_once()
    
    def test_close_connection(self, connector):
        """Test connection closing."""
        mock_conn = MagicMock()
        connector.connection = mock_conn
        
        connector.close()
        
        mock_conn.close.assert_called_once()
        assert connector.connection is None


if __name__ == "__main__":
    pytest.main([__file__]) 