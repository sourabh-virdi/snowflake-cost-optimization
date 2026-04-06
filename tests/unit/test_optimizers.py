"""
Unit tests for optimizer classes.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch
from pathlib import Path

import sys
sys.path.append(str(Path(__file__).parent.parent.parent / "src"))

from snowflake_optimizer.optimizers.warehouse_optimizer import (
    WarehouseOptimizer, 
    WarehouseRecommendation
)
from snowflake_optimizer.optimizers.query_optimizer import (
    QueryOptimizer,
    QueryRecommendation
)
from snowflake_optimizer.optimizers.storage_optimizer import (
    StorageOptimizer,
    StorageRecommendation
)


class TestWarehouseOptimizer:
    """Test the WarehouseOptimizer class."""
    
    @pytest.fixture
    def mock_connector(self):
        """Create a mock SnowflakeConnector."""
        return Mock()
    
    @pytest.fixture
    def warehouse_optimizer(self, mock_connector):
        """Create a WarehouseOptimizer instance for testing."""
        return WarehouseOptimizer(mock_connector)
    
    def test_analyze_warehouse_optimization_opportunities(self, warehouse_optimizer, mock_connector):
        """Test warehouse optimization analysis."""
        warehouse_data = pd.DataFrame({
            'warehouse_name': ['WH1', 'WH2'],
            'total_credits': [100.0, 200.0],
            'usage_count': [10, 100],  # WH1 low usage, WH2 high usage
            'total_runtime_minutes': [300, 1800]
        })
        
        query_data = pd.DataFrame({
            'warehouse_name': ['WH1', 'WH1', 'WH2', 'WH2'],
            'execution_time': [1000, 2000, 500, 600],
            'queued_time': [100, 200, 10, 20],
            'query_id': ['q1', 'q2', 'q3', 'q4']
        })
        
        mock_connector.get_warehouse_usage.return_value = warehouse_data
        mock_connector.get_query_history.return_value = query_data
        
        recommendations = warehouse_optimizer.analyze_warehouse_optimization_opportunities(days=30)
        
        assert isinstance(recommendations, list)
        assert len(recommendations) > 0
        
        # Check recommendation structure
        for rec in recommendations:
            assert isinstance(rec, WarehouseRecommendation)
            assert hasattr(rec, 'warehouse_name')
            assert hasattr(rec, 'recommendation_type')
            assert hasattr(rec, 'current_config')
            assert hasattr(rec, 'recommended_config')
            assert hasattr(rec, 'estimated_savings')
            assert hasattr(rec, 'confidence_score')
            assert hasattr(rec, 'implementation_effort')
            assert rec.recommendation_type in ['resize', 'auto_suspend', 'schedule', 'cluster']
    
    def test_analyze_single_warehouse(self, warehouse_optimizer):
        """Test single warehouse analysis."""
        warehouse_data = pd.DataFrame({
            'warehouse_name': ['WH1'],
            'total_credits': [50.0],
            'usage_count': [5],  # Very low usage
            'total_runtime_minutes': [150]
        })
        
        query_data = pd.DataFrame({
            'warehouse_name': ['WH1', 'WH1'],
            'execution_time': [10000, 15000],  # Slow queries
            'queued_time': [100, 200],
            'query_id': ['q1', 'q2']
        })
        
        recommendations = warehouse_optimizer._analyze_single_warehouse(
            'WH1', warehouse_data, query_data, analysis_days=30
        )
        
        assert isinstance(recommendations, list)
        assert len(recommendations) > 0
        
        # Should generate recommendations for low usage warehouse
        rec_types = [rec.recommendation_type for rec in recommendations]
        assert any(rec_type in ['resize', 'auto_suspend'] for rec_type in rec_types)
    
    def test_analyze_utilization_patterns(self, warehouse_optimizer):
        """Test utilization pattern analysis."""
        warehouse_data = pd.DataFrame({
            'warehouse_name': ['WH1'],
            'total_credits': [100.0],
            'usage_count': [50],
            'total_runtime_minutes': [1200]
        })
        
        query_data = pd.DataFrame({
            'warehouse_name': ['WH1'] * 10,
            'execution_time': [1000, 2000, 1500, 3000, 1200, 1800, 2200, 1100, 1400, 1600],
            'queued_time': [50, 100, 75, 200, 60, 90, 110, 40, 70, 80],
            'query_id': [f'q{i}' for i in range(1, 11)]
        })
        
        analysis = warehouse_optimizer._analyze_utilization_patterns('WH1', warehouse_data, query_data)
        
        assert 'avg_utilization' in analysis
        assert 'peak_utilization' in analysis
        assert 'avg_execution_time' in analysis
        assert 'avg_queue_time' in analysis
        assert 'cost_per_query' in analysis
        assert 'efficiency_score' in analysis
        
        # Check value ranges
        assert 0 <= analysis['avg_utilization'] <= 100
        assert analysis['peak_utilization'] >= analysis['avg_utilization']
        assert analysis['avg_execution_time'] > 0
        assert analysis['efficiency_score'] >= 0
    
    def test_generate_sizing_recommendation(self, warehouse_optimizer):
        """Test warehouse sizing recommendation generation."""
        utilization_analysis = {
            'avg_utilization': 20.0,  # Low utilization
            'peak_utilization': 40.0,
            'efficiency_score': 30.0,
            'avg_execution_time': 5000
        }
        
        recommendation = warehouse_optimizer._generate_sizing_recommendation(
            'WH1', utilization_analysis, avg_daily_credits=50.0, total_queries=100
        )
        
        assert recommendation is not None
        assert isinstance(recommendation, WarehouseRecommendation)
        assert recommendation.warehouse_name == 'WH1'
        assert recommendation.recommendation_type == 'resize'
        assert recommendation.estimated_savings > 0
        assert 0 <= recommendation.confidence_score <= 1
    
    def test_generate_auto_suspend_recommendation(self, warehouse_optimizer):
        """Test auto-suspend recommendation generation."""
        warehouse_data = pd.DataFrame({
            'warehouse_name': ['WH1'],
            'total_credits': [100.0],
            'usage_count': [10],  # Infrequent usage
            'total_runtime_minutes': [300]
        })
        
        query_data = pd.DataFrame({
            'warehouse_name': ['WH1', 'WH1'],
            'execution_time': [1000, 2000],
            'queued_time': [50, 100],
            'query_id': ['q1', 'q2']
        })
        
        utilization_analysis = {
            'avg_utilization': 15.0,
            'usage_frequency': 0.33,  # Low frequency
            'idle_time_percentage': 85.0
        }
        
        recommendation = warehouse_optimizer._generate_auto_suspend_recommendation(
            'WH1', warehouse_data, query_data, utilization_analysis
        )
        
        assert recommendation is not None
        assert isinstance(recommendation, WarehouseRecommendation)
        assert recommendation.recommendation_type == 'auto_suspend'
        assert recommendation.estimated_savings > 0
    
    def test_generate_scheduling_recommendation(self, warehouse_optimizer):
        """Test scheduling recommendation generation."""
        warehouse_data = pd.DataFrame({
            'warehouse_name': ['WH1'],
            'total_credits': [200.0],
            'usage_count': [100],
            'total_runtime_minutes': [1800]
        })
        
        utilization_analysis = {
            'peak_hours': [9, 10, 11, 14, 15, 16],  # Business hours pattern
            'usage_pattern': 'business_hours',
            'peak_utilization': 90.0,
            'off_peak_utilization': 10.0
        }
        
        recommendation = warehouse_optimizer._generate_scheduling_recommendation(
            'WH1', warehouse_data, utilization_analysis
        )
        
        if recommendation:  # May be None if pattern doesn't warrant scheduling
            assert isinstance(recommendation, WarehouseRecommendation)
            assert recommendation.recommendation_type == 'schedule'
            assert recommendation.estimated_savings >= 0


class TestQueryOptimizer:
    """Test the QueryOptimizer class."""
    
    @pytest.fixture
    def mock_connector(self):
        """Create a mock SnowflakeConnector."""
        return Mock()
    
    @pytest.fixture
    def query_optimizer(self, mock_connector):
        """Create a QueryOptimizer instance for testing."""
        return QueryOptimizer(mock_connector)
    
    def test_analyze_query_optimization_opportunities(self, query_optimizer, mock_connector):
        """Test query optimization analysis."""
        query_data = pd.DataFrame({
            'query_id': ['q1', 'q2', 'q3'],
            'execution_time': [10000, 5000, 15000],  # q3 and q1 are slow
            'compilation_time': [1000, 500, 2000],
            'queued_time': [500, 100, 1000],
            'bytes_scanned': [1000000000, 500000000, 2000000000],  # Large scans
            'rows_produced': [1000, 500, 2000],
            'query_text': [
                'SELECT * FROM large_table WHERE date > 2023-01-01',
                'SELECT count(*) FROM small_table',
                'SELECT * FROM huge_table ORDER BY id'
            ]
        })
        
        mock_connector.get_query_history.return_value = query_data
        
        recommendations = query_optimizer.analyze_query_optimization_opportunities(days=30)
        
        assert isinstance(recommendations, list)
        assert len(recommendations) > 0
        
        # Check recommendation structure
        for rec in recommendations:
            assert isinstance(rec, QueryRecommendation)
            assert hasattr(rec, 'query_id')
            assert hasattr(rec, 'recommendation_type')
            assert hasattr(rec, 'current_performance')
            assert hasattr(rec, 'recommended_changes')
            assert hasattr(rec, 'estimated_improvement')
            assert hasattr(rec, 'confidence_score')
            assert rec.recommendation_type in ['index', 'rewrite', 'partition', 'materialize']
    
    def test_identify_slow_queries(self, query_optimizer):
        """Test slow query identification."""
        query_data = pd.DataFrame({
            'query_id': ['q1', 'q2', 'q3', 'q4'],
            'execution_time': [1000, 15000, 5000, 20000],  # q4 slowest, then q2
            'compilation_time': [100, 1500, 500, 2000],
            'bytes_scanned': [100000, 1500000, 500000, 2000000]
        })
        
        slow_queries = query_optimizer._identify_slow_queries(query_data, top_n=2)
        
        assert len(slow_queries) == 2
        assert slow_queries[0]['query_id'] == 'q4'  # Slowest first
        assert slow_queries[1]['query_id'] == 'q2'  # Second slowest
        
        # Check performance metrics are included
        for query in slow_queries:
            assert 'execution_time' in query
            assert 'compilation_time' in query
            assert 'bytes_scanned' in query
    
    def test_generate_optimization_recommendations(self, query_optimizer):
        """Test optimization recommendation generation."""
        slow_queries = [
            {
                'query_id': 'q1',
                'execution_time': 15000,
                'compilation_time': 2000,
                'bytes_scanned': 2000000000,
                'rows_produced': 1000,
                'query_text': 'SELECT * FROM large_table WHERE date > 2023-01-01 ORDER BY id'
            },
            {
                'query_id': 'q2',
                'execution_time': 10000,
                'compilation_time': 1000,
                'bytes_scanned': 1000000000,
                'rows_produced': 500,
                'query_text': 'SELECT col1, col2 FROM table WHERE complex_condition = true'
            }
        ]
        
        recommendations = query_optimizer._generate_optimization_recommendations(slow_queries)
        
        assert len(recommendations) > 0
        
        for rec in recommendations:
            assert isinstance(rec, QueryRecommendation)
            assert rec.estimated_improvement > 0
            assert 0 <= rec.confidence_score <= 1
            assert rec.implementation_effort in ['low', 'medium', 'high']
            assert len(rec.description) > 0
    
    def test_analyze_query_patterns(self, query_optimizer):
        """Test query pattern analysis."""
        query_text = 'SELECT * FROM large_table WHERE date > 2023-01-01 ORDER BY id LIMIT 1000'
        performance_metrics = {
            'execution_time': 15000,
            'compilation_time': 2000,
            'bytes_scanned': 2000000000,
            'rows_produced': 1000
        }
        
        recommendation = query_optimizer._analyze_query_patterns(query_text, performance_metrics)
        
        assert recommendation is not None
        assert isinstance(recommendation, dict)
        assert 'type' in recommendation
        assert 'description' in recommendation
        assert 'estimated_improvement' in recommendation
        assert 'confidence' in recommendation
        
        # Should identify optimization opportunities
        assert recommendation['type'] in ['index', 'rewrite', 'partition', 'materialize']
        assert recommendation['estimated_improvement'] > 0


class TestStorageOptimizer:
    """Test the StorageOptimizer class."""
    
    @pytest.fixture
    def mock_connector(self):
        """Create a mock SnowflakeConnector."""
        return Mock()
    
    @pytest.fixture
    def storage_optimizer(self, mock_connector):
        """Create a StorageOptimizer instance for testing."""
        return StorageOptimizer(mock_connector)
    
    def test_analyze_storage_optimization_opportunities(self, storage_optimizer, mock_connector):
        """Test storage optimization analysis."""
        storage_data = pd.DataFrame({
            'table_name': ['table1', 'table2', 'table3'],
            'database_name': ['db1', 'db1', 'db2'],
            'schema_name': ['schema1', 'schema1', 'schema2'],
            'active_bytes': [10737418240, 1073741824, 5368709120],  # 10GB, 1GB, 5GB
            'time_travel_bytes': [2147483648, 107374182, 1073741824],  # 2GB, 0.1GB, 1GB
            'failsafe_bytes': [1073741824, 53687091, 536870912],  # 1GB, 0.05GB, 0.5GB
            'row_count': [1000000, 100000, 500000],
            'table_created': ['2023-01-01', '2024-01-01', '2023-06-01'],
            'table_last_altered': ['2023-12-01', '2024-01-15', '2023-06-01']
        })
        
        query_data = pd.DataFrame({
            'database_name': ['db1', 'db1', 'db2'],
            'table_name': ['table1', 'table2', 'table3'],
            'query_count': [10, 0, 5],  # table2 unused
            'last_access': ['2024-01-15', '2023-06-01', '2024-01-10']
        })
        
        mock_connector.get_storage_usage.return_value = storage_data
        mock_connector.get_query_history.return_value = query_data
        
        recommendations = storage_optimizer.analyze_storage_optimization_opportunities(days=30)
        
        assert isinstance(recommendations, list)
        assert len(recommendations) > 0
        
        # Check recommendation structure
        for rec in recommendations:
            assert isinstance(rec, StorageRecommendation)
            assert hasattr(rec, 'object_name')
            assert hasattr(rec, 'object_type')
            assert hasattr(rec, 'recommendation_type')
            assert hasattr(rec, 'current_storage')
            assert hasattr(rec, 'recommended_changes')
            assert hasattr(rec, 'estimated_savings')
            assert rec.recommendation_type in ['cleanup', 'compress', 'partition', 'lifecycle']
    
    def test_identify_large_tables(self, storage_optimizer):
        """Test large table identification."""
        storage_data = pd.DataFrame({
            'table_name': ['small_table', 'large_table', 'huge_table'],
            'database_name': ['db1', 'db1', 'db2'],
            'active_bytes': [107374182, 10737418240, 53687091200],  # 0.1GB, 10GB, 50GB
            'time_travel_bytes': [10737418, 1073741824, 5368709120],
            'row_count': [1000, 1000000, 10000000]
        })
        
        large_tables = storage_optimizer._identify_large_tables(storage_data, size_threshold_gb=5.0)
        
        assert len(large_tables) == 2  # large_table and huge_table
        assert large_tables[0]['table_name'] == 'huge_table'  # Largest first
        assert large_tables[1]['table_name'] == 'large_table'
        
        # Check size calculations
        for table in large_tables:
            assert table['size_gb'] >= 5.0
            assert 'time_travel_gb' in table
            assert 'total_storage_gb' in table
    
    def test_analyze_time_travel_waste(self, storage_optimizer):
        """Test time travel waste analysis."""
        storage_data = pd.DataFrame({
            'table_name': ['table1', 'table2', 'table3'],
            'active_bytes': [1073741824, 2147483648, 536870912],  # 1GB, 2GB, 0.5GB
            'time_travel_bytes': [2147483648, 214748365, 1073741824],  # 2GB, 0.2GB, 1GB
            'table_last_altered': ['2023-01-01', '2024-01-15', '2023-06-01']
        })
        
        waste_analysis = storage_optimizer._analyze_time_travel_waste(storage_data)
        
        assert len(waste_analysis) > 0
        
        # Should identify table1 and table3 as having excessive time travel
        table_names = [item['table_name'] for item in waste_analysis]
        assert 'table1' in table_names  # 200% time travel overhead
        assert 'table3' in table_names  # 200% time travel overhead
        
        # Check waste calculations
        for item in waste_analysis:
            assert item['time_travel_percentage'] > 50  # Over 50% threshold
            assert item['potential_savings_gb'] > 0
    
    def test_identify_unused_tables(self, storage_optimizer):
        """Test unused table identification."""
        storage_data = pd.DataFrame({
            'table_name': ['active_table', 'unused_table', 'old_table'],
            'database_name': ['db1', 'db1', 'db2'],
            'active_bytes': [1073741824, 2147483648, 536870912],
            'table_created': ['2024-01-01', '2023-01-01', '2022-01-01'],
            'table_last_altered': ['2024-01-15', '2023-06-01', '2022-03-01']
        })
        
        query_data = pd.DataFrame({
            'table_name': ['active_table', 'active_table'],
            'database_name': ['db1', 'db1'],
            'query_count': [10, 5],
            'last_access': ['2024-01-15', '2024-01-10']
        })
        
        unused_tables = storage_optimizer._identify_unused_tables(
            storage_data, query_data, days_threshold=90
        )
        
        assert len(unused_tables) == 2  # unused_table and old_table
        
        table_names = [item['table_name'] for item in unused_tables]
        assert 'unused_table' in table_names
        assert 'old_table' in table_names
        
        # Check that active_table is not in the list
        assert 'active_table' not in table_names
    
    def test_analyze_compression_opportunities(self, storage_optimizer):
        """Test compression opportunity analysis."""
        storage_data = pd.DataFrame({
            'table_name': ['table1', 'table2', 'table3'],
            'active_bytes': [10737418240, 1073741824, 5368709120],  # 10GB, 1GB, 5GB
            'row_count': [10000000, 1000000, 5000000],
            'table_comment': ['Regular table', 'Compressed table', None]
        })
        
        compression_opportunities = storage_optimizer._analyze_compression_opportunities(storage_data)
        
        assert len(compression_opportunities) > 0
        
        # Should suggest compression for large tables
        table_names = [item['table_name'] for item in compression_opportunities]
        assert 'table1' in table_names  # Largest table
        
        # Check compression estimations
        for item in compression_opportunities:
            assert item['estimated_compression_ratio'] > 0
            assert item['potential_savings_gb'] > 0
            assert item['current_size_gb'] > 0


if __name__ == "__main__":
    pytest.main([__file__]) 