"""
Snowflake connector for cost optimization analysis.
"""

import os
import hashlib
import pickle
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
from pathlib import Path
import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.connector import connect, SnowflakeConnection
from snowflake.connector.errors import Error as SnowflakeError
from loguru import logger
from ..config.settings import get_settings, SnowflakeSettings

# Constants for commonly used values
DEFAULT_QUERY_LIMIT = 10000
DEFAULT_ANALYSIS_DAYS = 30
UNKNOWN_VALUE = 'unknown'

# Cache configuration
DEFAULT_CACHE_TTL_HOURS = 1  # Default cache TTL in hours
CACHE_DIR = Path(".cache/query_cache")
CACHE_INDEX_FILE = CACHE_DIR / "cache_index.json"

# Different TTL for different query types
QUERY_CACHE_TTL = {
    'warehouse_usage': 0.5,      # 30 minutes - changes frequently
    'query_history': 1.0,        # 1 hour - moderately dynamic
    'storage_usage': 6.0,        # 6 hours - changes slowly
    'user_access': 24.0,         # 24 hours - changes very slowly
    'cost_analysis': 2.0,        # 2 hours - moderately important
    'default': 1.0               # 1 hour default
}

# Column mapping configurations
STORAGE_COLUMN_MAPPINGS = {
    'database': ['database_name', 'table_catalog'],
    'schema': ['schema_name', 'table_schema'],
    'created': ['created', 'table_created'],
    'altered': ['last_altered', 'table_last_altered', 'last_ddl'],
    'comment': ['comment', 'table_comment']
}

STORAGE_DATA_COLUMNS = [
    'table_name', 'active_bytes', 'time_travel_bytes', 
    'failsafe_bytes', 'retained_for_clone_bytes', 'row_count'
]

WAREHOUSE_CREDIT_COLUMNS = ['credits_used', 'credits_used_compute', 'credit_used']


class QueryCache:
    """
    File-based query cache with TTL support for Snowflake queries.
    """
    
    def __init__(self, cache_dir: Path = CACHE_DIR):
        """Initialize query cache."""
        self.cache_dir = cache_dir
        self.index_file = cache_dir / "cache_index.json"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._load_cache_index()
    
    def _load_cache_index(self):
        """Load cache index from file."""
        try:
            if self.index_file.exists():
                with open(self.index_file, 'r') as f:
                    self.cache_index = json.load(f)
            else:
                self.cache_index = {}
        except Exception as e:
            logger.warning(f"Failed to load cache index: {e}")
            self.cache_index = {}
    
    def _save_cache_index(self):
        """Save cache index to file."""
        try:
            with open(self.index_file, 'w') as f:
                json.dump(self.cache_index, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save cache index: {e}")
    
    def _generate_cache_key(self, query: str, params: Dict[str, Any] = None) -> str:
        """Generate a unique cache key for a query and its parameters."""
        # Normalize query (remove extra whitespace, convert to lowercase)
        normalized_query = ' '.join(query.lower().strip().split())
        
        # Include parameters in the key
        params_str = json.dumps(params or {}, sort_keys=True)
        
        # Create hash
        cache_content = f"{normalized_query}:{params_str}"
        return hashlib.md5(cache_content.encode()).hexdigest()
    
    def _get_cache_file_path(self, cache_key: str) -> Path:
        """Get the file path for a cache key."""
        return self.cache_dir / f"{cache_key}.pkl"
    
    def _is_cache_valid(self, cache_key: str, ttl_hours: float) -> bool:
        """Check if cache entry is still valid."""
        if cache_key not in self.cache_index:
            return False
        
        cache_info = self.cache_index[cache_key]
        cached_time = datetime.fromisoformat(cache_info['timestamp'])
        expiry_time = cached_time + timedelta(hours=ttl_hours)
        
        return datetime.now() < expiry_time
    
    def get(self, query: str, params: Dict[str, Any] = None, ttl_hours: float = DEFAULT_CACHE_TTL_HOURS) -> Optional[pd.DataFrame]:
        """Get cached query result if valid."""
        cache_key = self._generate_cache_key(query, params)
        
        if not self._is_cache_valid(cache_key, ttl_hours):
            return None
        
        try:
            cache_file = self._get_cache_file_path(cache_key)
            if cache_file.exists():
                with open(cache_file, 'rb') as f:
                    cached_result = pickle.load(f)
                
                logger.debug(f"Cache hit for query (key: {cache_key[:8]}...)")
                return cached_result
        except Exception as e:
            logger.warning(f"Failed to load cached result: {e}")
            # Remove invalid cache entry
            self._remove_cache_entry(cache_key)
        
        return None
    
    def set(self, query: str, result: pd.DataFrame, params: Dict[str, Any] = None, ttl_hours: float = DEFAULT_CACHE_TTL_HOURS):
        """Cache query result."""
        cache_key = self._generate_cache_key(query, params)
        
        try:
            # Save the result
            cache_file = self._get_cache_file_path(cache_key)
            with open(cache_file, 'wb') as f:
                pickle.dump(result, f)
            
            # Update index
            self.cache_index[cache_key] = {
                'timestamp': datetime.now().isoformat(),
                'ttl_hours': ttl_hours,
                'query_hash': cache_key[:16],  # First 16 chars for identification
                'result_size': len(result),
                'columns': len(result.columns) if not result.empty else 0
            }
            
            self._save_cache_index()
            logger.debug(f"Cached query result (key: {cache_key[:8]}..., size: {len(result)} rows)")
            
        except Exception as e:
            logger.error(f"Failed to cache query result: {e}")
    
    def _remove_cache_entry(self, cache_key: str):
        """Remove a cache entry."""
        try:
            # Remove file
            cache_file = self._get_cache_file_path(cache_key)
            if cache_file.exists():
                cache_file.unlink()
            
            # Remove from index
            if cache_key in self.cache_index:
                del self.cache_index[cache_key]
                self._save_cache_index()
                
        except Exception as e:
            logger.warning(f"Failed to remove cache entry: {e}")
    
    def clear_expired(self):
        """Clear all expired cache entries."""
        expired_keys = []
        
        for cache_key, cache_info in self.cache_index.items():
            cached_time = datetime.fromisoformat(cache_info['timestamp'])
            ttl_hours = cache_info.get('ttl_hours', DEFAULT_CACHE_TTL_HOURS)
            expiry_time = cached_time + timedelta(hours=ttl_hours)
            
            if datetime.now() >= expiry_time:
                expired_keys.append(cache_key)
        
        for key in expired_keys:
            self._remove_cache_entry(key)
        
        if expired_keys:
            logger.info(f"Cleared {len(expired_keys)} expired cache entries")
    
    def clear_all(self):
        """Clear all cache entries."""
        try:
            # Remove all cache files
            for cache_file in self.cache_dir.glob("*.pkl"):
                cache_file.unlink()
            
            # Clear index
            self.cache_index = {}
            self._save_cache_index()
            
            logger.info("Cleared all cache entries")
            
        except Exception as e:
            logger.error(f"Failed to clear cache: {e}")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_entries = len(self.cache_index)
        valid_entries = 0
        total_size_mb = 0
        
        for cache_key, cache_info in self.cache_index.items():
            cached_time = datetime.fromisoformat(cache_info['timestamp'])
            ttl_hours = cache_info.get('ttl_hours', DEFAULT_CACHE_TTL_HOURS)
            expiry_time = cached_time + timedelta(hours=ttl_hours)
            
            if datetime.now() < expiry_time:
                valid_entries += 1
            
            # Calculate file size
            cache_file = self._get_cache_file_path(cache_key)
            if cache_file.exists():
                total_size_mb += cache_file.stat().st_size / (1024 * 1024)
        
        return {
            'total_entries': total_entries,
            'valid_entries': valid_entries,
            'expired_entries': total_entries - valid_entries,
            'total_size_mb': round(total_size_mb, 2),
            'cache_directory': str(self.cache_dir)
        }


class SnowflakeConnector:
    """
    Snowflake connector with session management, query execution, and caching capabilities.
    """
    
    def __init__(self, settings: Optional[SnowflakeSettings] = None, enable_cache: bool = True):
        """Initialize Snowflake connector with configuration."""
        self.settings = settings or get_settings().snowflake
        self._session: Optional[Session] = None
        self._connection: Optional[SnowflakeConnection] = None
        
        # Initialize cache
        self.enable_cache = enable_cache
        if self.enable_cache:
            self.cache = QueryCache()
            # Clean up expired entries on initialization
            self.cache.clear_expired()
        else:
            self.cache = None
        
    def _get_connection_params(self) -> Dict[str, Any]:
        """Get connection parameters for Snowflake."""
        params = {
            'account': self.settings.account,
            'user': self.settings.user,
            'warehouse': self.settings.warehouse,
            'database': self.settings.database,
            'schema': self.settings.schema_name,
        }
        
        if self.settings.role:
            params['role'] = self.settings.role
            
        # Authentication method
        if self.settings.private_key_path:
            # Key pair authentication
            key_path = Path(self.settings.private_key_path)
            if not key_path.exists():
                raise FileNotFoundError(f"Private key file not found: {key_path}")
                
            with open(key_path, 'rb') as key_file:
                private_key = key_file.read()
                
            params['private_key'] = private_key
            if self.settings.private_key_passphrase:
                params['private_key_passphrase'] = self.settings.private_key_passphrase
        else:
            # Password authentication
            params['password'] = self.settings.password
            
        return params

    def create_session(self) -> Session:
        """Create a new Snowpark session."""
        if self._session is None:
            try:
                connection_params = self._get_connection_params()
                self._session = Session.builder.configs(connection_params).create()
                logger.debug("Snowpark session created successfully")
            except Exception as e:
                logger.error(f"Failed to create Snowpark session: {e}")
                raise
        return self._session

    def create_connection(self) -> SnowflakeConnection:
        """Create a new Snowflake connection."""
        if self._connection is None:
            try:
                connection_params = self._get_connection_params()
                self._connection = connect(**connection_params)
                logger.debug("Snowflake connection created successfully")
            except Exception as e:
                logger.error(f"Failed to create Snowflake connection: {e}")
                raise
        return self._connection

    def test_connection(self) -> bool:
        """Test the Snowflake connection."""
        try:
            session = self.create_session()
            session.sql("SELECT 1").collect()
            logger.info("Snowflake connection test successful")
            return True
        except Exception as e:
            logger.error(f"Snowflake connection test failed: {e}")
            return False

    def close(self):
        """Close the Snowflake connection and session."""
        try:
            if self._session:
                self._session.close()
                self._session = None
            if self._connection:
                self._connection.close()
                self._connection = None
            logger.debug("Snowflake connections closed")
        except Exception as e:
            logger.error(f"Error closing connections: {e}")

    def execute_query(self, query: str, use_session: bool = True, cache_key_type: str = 'default', 
                     cache_params: Dict[str, Any] = None, force_refresh: bool = False) -> pd.DataFrame:
        """
        Execute a SQL query and return results as a pandas DataFrame with caching support.
        
        Args:
            query: SQL query to execute
            use_session: Whether to use Snowpark session (True) or direct connection (False)
            cache_key_type: Type of query for TTL determination (warehouse_usage, storage_usage, etc.)
            cache_params: Additional parameters to include in cache key
            force_refresh: If True, bypass cache and force query execution
            
        Returns:
            Query results as pandas DataFrame
        """
        # Determine cache TTL based on query type
        ttl_hours = QUERY_CACHE_TTL.get(cache_key_type, QUERY_CACHE_TTL['default'])
        
        # Try to get from cache first (if enabled and not forcing refresh)
        if self.enable_cache and not force_refresh:
            cached_result = self.cache.get(query, cache_params, ttl_hours)
            if cached_result is not None:
                return cached_result
        
        # Execute query if not in cache or cache disabled
        try:
            start_time = datetime.now()
            
            if use_session:
                session = self.create_session()
                result = session.sql(query).to_pandas()
            else:
                connection = self.create_connection()
                result = pd.read_sql(query, connection)
            
            execution_time = (datetime.now() - start_time).total_seconds()
            logger.debug(f"Query executed in {execution_time:.2f}s, returned {len(result)} rows")
            
            if not result.empty:
                logger.debug(f"Columns returned: {list(result.columns)}")
                # Normalize column names to lowercase for consistency
                result.columns = [col.lower() for col in result.columns]
                logger.debug(f"Normalized columns: {list(result.columns)}")
            
            # Cache the result if caching is enabled
            if self.enable_cache:
                self.cache.set(query, result, cache_params, ttl_hours)
            
            return result
            
        except (SnowparkSQLException, SnowflakeError) as e:
            logger.error(f"Query execution failed: {e}")
            logger.debug(f"Failed query: {query}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during query execution: {e}")
            raise

    def _check_table_columns(self, table_name: str) -> List[str]:
        """Check what columns are available in a given table."""
        try:
            query = f"SELECT * FROM {table_name} LIMIT 1"
            # Use shorter cache for schema queries
            result = self.execute_query(query, cache_key_type='default', 
                                      cache_params={'operation': 'schema_check', 'table': table_name})
            return list(result.columns) if not result.empty else []
        except Exception as e:
            logger.error(f"Failed to check columns for {table_name}: {e}")
            return []

    def _find_column_mapping(self, available_cols: List[str], column_mappings: Dict[str, List[str]]) -> Dict[str, Optional[str]]:
        """Find the best column mapping from available columns."""
        mapping = {}
        for key, candidates in column_mappings.items():
            mapping[key] = next((col for col in candidates if col in available_cols), None)
        return mapping

    def _build_dynamic_select(self, columns_config: List[Dict[str, Any]]) -> List[str]:
        """Build a dynamic SELECT clause based on column configuration."""
        select_columns = []
        for config in columns_config:
            column_name = config['column']
            alias = config['alias']
            default_value = config['default']
            coalesce = config.get('coalesce', False)
            
            if column_name:
                if coalesce:
                    select_columns.append(f"COALESCE({column_name}, {default_value}) as {alias}")
                else:
                    select_columns.append(f"{column_name} as {alias}")
            else:
                select_columns.append(f"{default_value} as {alias}")
        
        return select_columns

    def get_warehouse_usage(self, days: int = DEFAULT_ANALYSIS_DAYS) -> pd.DataFrame:
        """Get warehouse usage data for the specified number of days."""
        # Check available columns
        available_cols = self._check_table_columns("snowflake.account_usage.warehouse_metering_history")
        
        # Find credits column
        credits_col = next((col for col in WAREHOUSE_CREDIT_COLUMNS if col in available_cols), '1')
        if credits_col == '1':
            logger.warning("No credits column found in warehouse_metering_history, using literal value")
        
        query = f"""
        SELECT 
            warehouse_name,
            DATE(start_time) as usage_date,
            SUM({credits_col}) as total_credits,
            AVG({credits_col}) as avg_credits_per_hour,
            COUNT(*) as usage_count,
            SUM(DATEDIFF(second, start_time, end_time)) / 60.0 as total_runtime_minutes,
            AVG(DATEDIFF(second, start_time, end_time)) as avg_runtime_seconds
        FROM snowflake.account_usage.warehouse_metering_history 
        WHERE start_time >= CURRENT_DATE - {days}
        GROUP BY warehouse_name, DATE(start_time)
        ORDER BY usage_date DESC, total_credits DESC
        """
        
        return self.execute_query(
            query, 
            cache_key_type='warehouse_usage',
            cache_params={'days': days, 'credits_col': credits_col}
        )

    def get_query_history(self, days: int = 7, limit: int = DEFAULT_QUERY_LIMIT) -> pd.DataFrame:
        """Get query history with performance metrics."""
        query = f"""
        SELECT 
            query_id,
            query_text,
            database_name,
            schema_name,
            user_name,
            warehouse_name,
            warehouse_size,
            start_time,
            end_time,
            total_elapsed_time,
            execution_time,
            compilation_time,
            bytes_scanned,
            rows_produced,
            credits_used_cloud_services,
            query_type,
            execution_status
        FROM snowflake.account_usage.query_history
        WHERE start_time >= CURRENT_DATE - {days}
        AND execution_status = 'SUCCESS'
        ORDER BY start_time DESC
        LIMIT {limit}
        """
        
        return self.execute_query(
            query,
            cache_key_type='query_history',
            cache_params={'days': days, 'limit': limit}
        )
    
    def get_storage_usage(self) -> pd.DataFrame:
        """Get storage usage by database and table."""
        # Check available columns
        available_cols = self._check_table_columns("snowflake.account_usage.table_storage_metrics")
        logger.debug(f"Available columns in table_storage_metrics: {available_cols}")
        
        # Find column mappings
        col_mapping = self._find_column_mapping(available_cols, STORAGE_COLUMN_MAPPINGS)
        
        # Build column configuration for dynamic SELECT
        columns_config = [
            {'column': 'table_name' if 'table_name' in available_cols else None, 'alias': 'table_name', 'default': f"'{UNKNOWN_VALUE}'", 'coalesce': True},
            {'column': col_mapping['database'], 'alias': 'database_name', 'default': f"'{UNKNOWN_VALUE}'"},
            {'column': col_mapping['schema'], 'alias': 'schema_name', 'default': f"'{UNKNOWN_VALUE}'"},
            {'column': 'active_bytes' if 'active_bytes' in available_cols else None, 'alias': 'active_bytes', 'default': '0', 'coalesce': True},
            {'column': 'time_travel_bytes' if 'time_travel_bytes' in available_cols else None, 'alias': 'time_travel_bytes', 'default': '0', 'coalesce': True},
            {'column': 'failsafe_bytes' if 'failsafe_bytes' in available_cols else None, 'alias': 'failsafe_bytes', 'default': '0', 'coalesce': True},
            {'column': 'retained_for_clone_bytes' if 'retained_for_clone_bytes' in available_cols else None, 'alias': 'retained_for_clone_bytes', 'default': '0', 'coalesce': True},
            {'column': 'row_count' if 'row_count' in available_cols else None, 'alias': 'row_count', 'default': '0', 'coalesce': True},
            {'column': col_mapping['database'], 'alias': 'table_catalog', 'default': f"'{UNKNOWN_VALUE}'"},
            {'column': col_mapping['schema'], 'alias': 'table_schema', 'default': f"'{UNKNOWN_VALUE}'"},
            {'column': col_mapping['created'], 'alias': 'table_created', 'default': 'NULL'},
            {'column': col_mapping['altered'], 'alias': 'table_last_altered', 'default': 'NULL'},
            {'column': col_mapping['comment'], 'alias': 'table_comment', 'default': 'NULL'}
        ]
        
        # Build dynamic SELECT clause
        select_columns = self._build_dynamic_select(columns_config)
        
        query = f"""
        SELECT 
            {', '.join(select_columns)}
        FROM snowflake.account_usage.table_storage_metrics
        WHERE 1=1  
        ORDER BY active_bytes DESC
        """
        
        return self.execute_query(
            query,
            cache_key_type='storage_usage',
            cache_params={'columns_mapping': col_mapping}
        )

    def get_user_access_patterns(self, days: int = DEFAULT_ANALYSIS_DAYS) -> pd.DataFrame:
        """Get user access patterns and permissions."""
        # Check available columns first
        available_cols = self._check_table_columns("snowflake.account_usage.grants_to_users")
        logger.debug(f"Available columns in grants_to_users: {available_cols}")
        
        # Map common column variations
        column_mappings = {
            'user': ['user_name', 'grantee_name', 'grantee'],
            'database': ['database_name', 'granted_database'],
            'schema': ['schema_name', 'granted_schema'],
            'object': ['object_name', 'granted_object_name'],
            'type': ['object_type', 'granted_object_type'],
            'privilege': ['privilege', 'privilege_type'],
            'granted_on': ['granted_on', 'granted_on_type'],
            'granted_to': ['granted_to', 'granted_to_type'], 
            'granted_by': ['granted_by', 'grantor'],
            'created': ['created_on', 'granted_on_timestamp', 'created_date']
        }
        
        # Find the best column mapping
        col_mapping = self._find_column_mapping(available_cols, column_mappings)
        
        # Build column configuration for dynamic SELECT
        columns_config = [
            {'column': col_mapping['user'], 'alias': 'user_name', 'default': f"'{UNKNOWN_VALUE}'"},
            {'column': col_mapping['database'], 'alias': 'database_name', 'default': f"'{UNKNOWN_VALUE}'"},
            {'column': col_mapping['schema'], 'alias': 'schema_name', 'default': f"'{UNKNOWN_VALUE}'"},
            {'column': col_mapping['object'], 'alias': 'object_name', 'default': f"'{UNKNOWN_VALUE}'"},
            {'column': col_mapping['type'], 'alias': 'object_type', 'default': f"'{UNKNOWN_VALUE}'"},
            {'column': col_mapping['privilege'], 'alias': 'privilege', 'default': f"'{UNKNOWN_VALUE}'"},
            {'column': col_mapping['granted_on'], 'alias': 'granted_on', 'default': f"'{UNKNOWN_VALUE}'"},
            {'column': col_mapping['granted_to'], 'alias': 'granted_to', 'default': f"'{UNKNOWN_VALUE}'"},
            {'column': col_mapping['granted_by'], 'alias': 'granted_by', 'default': f"'{UNKNOWN_VALUE}'"},
            {'column': col_mapping['created'], 'alias': 'created_on', 'default': 'CURRENT_DATE'},
        ]
        
        # Add computed columns
        if col_mapping['created']:
            columns_config.append({
                'column': f"DATE({col_mapping['created']})", 
                'alias': 'access_date', 
                'default': 'CURRENT_DATE'
            })
        else:
            columns_config.append({
                'column': None, 
                'alias': 'access_date', 
                'default': 'CURRENT_DATE'
            })
        
        # Build dynamic SELECT clause
        select_columns = self._build_dynamic_select(columns_config)
        
        # Add COUNT(*) for aggregation
        select_columns.append("COUNT(*) as access_count")
        
        # Build GROUP BY clause (exclude the COUNT column)
        group_by_columns = [config['alias'] for config in columns_config if config['alias'] != 'access_count']
        
        # Construct the query with available columns
        if col_mapping['created']:
            where_clause = f"WHERE {col_mapping['created']} >= CURRENT_DATE - {days}"
        else:
            where_clause = "WHERE 1=1"  # Fallback if no date column
        
        query = f"""
        SELECT 
            {', '.join(select_columns)}
        FROM snowflake.account_usage.grants_to_users
        {where_clause}
        GROUP BY {', '.join(group_by_columns)}
        ORDER BY access_date DESC, access_count DESC
        LIMIT 1000
        """
        
        return self.execute_query(
            query,
            cache_key_type='user_access',
            cache_params={'days': days, 'columns_mapping': col_mapping}
        )
    
    def get_cost_analysis_data(self, days: int = DEFAULT_ANALYSIS_DAYS) -> Dict[str, pd.DataFrame]:
        """Get comprehensive cost analysis data."""
        data = {}
        
        try:
            # Warehouse costs
            data['warehouse_usage'] = self.get_warehouse_usage(days)
            
            # Storage costs
            data['storage_usage'] = self.get_storage_usage()
            
            # Query performance data
            data['query_history'] = self.get_query_history(days)
            
            # User access patterns
            data['user_access'] = self.get_user_access_patterns(days)
            
        except Exception as e:
            logger.error(f"Error getting cost analysis data: {e}")
            
        return data

    def clear_cache(self, query_type: Optional[str] = None):
        """Clear cache entries. If query_type is specified, clear only that type."""
        if not self.enable_cache:
            logger.warning("Caching is disabled")
            return
        
        if query_type:
            # Clear specific type (this would require more sophisticated cache key management)
            # For now, we'll clear all cache
            logger.info(f"Clearing cache for query type: {query_type}")
            self.cache.clear_expired()
        else:
            logger.info("Clearing all cache")
            self.cache.clear_all()

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        if not self.enable_cache:
            return {'status': 'disabled'}
        
        return self.cache.get_cache_stats() 