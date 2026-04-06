"""
Unit tests for analyzer classes.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
from pathlib import Path

import sys
sys.path.append(str(Path(__file__).parent.parent.parent / "src"))

from snowflake_optimizer.analyzers.cost_analyzer import CostAnalyzer
from snowflake_optimizer.analyzers.usage_analyzer import UsageAnalyzer, UtilizationMetrics
from snowflake_optimizer.analyzers.performance_analyzer import PerformanceAnalyzer
from snowflake_optimizer.analyzers.access_analyzer import AccessAnalyzer


class TestCostAnalyzer:
    """Test the CostAnalyzer class."""
    
    @pytest.fixture
    def mock_connector(self):
        """Create a mock SnowflakeConnector."""
        return Mock()
    
    @pytest.fixture
    def cost_analyzer(self, mock_connector):
        """Create a CostAnalyzer instance for testing."""
        return CostAnalyzer(mock_connector)
    
    def test_analyze_overall_costs(self, cost_analyzer, mock_connector):
        """Test overall cost analysis."""
        # Mock data for different cost components
        warehouse_data = pd.DataFrame({
            'warehouse_name': ['WH1', 'WH2'],
            'total_credits': [100.0, 200.0],
            'usage_count': [50, 75],
            'total_runtime_minutes': [1200, 1800]
        })
        
        storage_data = pd.DataFrame({
            'table_name': ['table1', 'table2'],
            'database_name': ['db1', 'db2'],
            'active_bytes': [1073741824, 2147483648],  # 1GB, 2GB
            'time_travel_bytes': [107374182, 214748365],
            'failsafe_bytes': [53687091, 107374182]
        })
        
        cost_data = pd.DataFrame({
            'service_type': ['WAREHOUSE', 'STORAGE'],
            'credits_used': [300.0, 50.0],
            'cost_amount': [600.0, 100.0]
        })
        
        # Configure mock connector
        mock_connector.get_warehouse_usage.return_value = warehouse_data
        mock_connector.get_storage_usage.return_value = storage_data
        mock_connector.get_cost_analysis_data.return_value = cost_data
        
        # Run analysis
        result = cost_analyzer.analyze_overall_costs(days=30)
        
        # Verify results structure
        assert 'warehouse_analysis' in result
        assert 'storage_analysis' in result
        assert 'cost_breakdown' in result
        assert 'estimated_monthly_cost' in result
        assert 'recommendations' in result
        assert 'priority_alerts' in result
        
        # Verify warehouse analysis
        warehouse_analysis = result['warehouse_analysis']
        assert warehouse_analysis['total_warehouses'] == 2
        assert warehouse_analysis['total_cost'] == 300.0
        assert warehouse_analysis['avg_cost_per_warehouse'] == 150.0
        
        # Verify storage analysis
        storage_analysis = result['storage_analysis']
        assert storage_analysis['total_tables'] == 2
        assert storage_analysis['total_storage_gb'] == pytest.approx(3.0, rel=1e-1)  # 3GB total
        
        # Verify cost breakdown
        cost_breakdown = result['cost_breakdown']
        assert 'warehouse_percentage' in cost_breakdown
        assert 'storage_percentage' in cost_breakdown
    
    def test_analyze_warehouse_costs(self, cost_analyzer, mock_connector):
        """Test warehouse cost analysis."""
        warehouse_data = pd.DataFrame({
            'warehouse_name': ['WH1', 'WH2', 'WH3'],
            'total_credits': [100.0, 200.0, 50.0],
            'usage_count': [50, 75, 25],
            'total_runtime_minutes': [1200, 1800, 600]
        })
        
        mock_connector.get_warehouse_usage.return_value = warehouse_data
        
        result = cost_analyzer._analyze_warehouse_costs(warehouse_data)
        
        assert result['total_warehouses'] == 3
        assert result['total_cost'] == 350.0
        assert result['avg_cost_per_warehouse'] == pytest.approx(116.67, rel=1e-2)
        assert result['most_expensive_warehouse'] == 'WH2'
        assert result['cost_distribution']
        
        # Check that cost distribution is properly sorted
        cost_dist = result['cost_distribution']
        assert cost_dist[0]['warehouse'] == 'WH2'  # Most expensive first
        assert cost_dist[0]['credits'] == 200.0
    
    def test_analyze_storage_costs(self, cost_analyzer):
        """Test storage cost analysis."""
        storage_data = pd.DataFrame({
            'table_name': ['table1', 'table2', 'table3'],
            'database_name': ['db1', 'db1', 'db2'],
            'active_bytes': [1073741824, 2147483648, 536870912],  # 1GB, 2GB, 0.5GB
            'time_travel_bytes': [107374182, 214748365, 53687091],
            'failsafe_bytes': [53687091, 107374182, 26843546]
        })
        
        result = cost_analyzer._analyze_storage_costs(storage_data)
        
        assert result['total_tables'] == 3
        assert result['total_storage_gb'] == pytest.approx(3.5, rel=1e-1)
        assert result['databases'] == 2
        assert 'storage_by_database' in result
        assert 'largest_tables' in result
        
        # Check largest tables sorting
        largest_tables = result['largest_tables']
        assert largest_tables[0]['table_name'] == 'table2'  # Largest first
        assert largest_tables[0]['size_gb'] == pytest.approx(2.0, rel=1e-1)
    
    def test_generate_cost_recommendations(self, cost_analyzer):
        """Test cost recommendation generation."""
        warehouse_analysis = {
            'total_cost': 1000.0,
            'cost_distribution': [
                {'warehouse': 'WH1', 'credits': 500.0, 'utilization': 30.0},
                {'warehouse': 'WH2', 'credits': 300.0, 'utilization': 80.0},
                {'warehouse': 'WH3', 'credits': 200.0, 'utilization': 10.0}
            ]
        }
        
        storage_analysis = {
            'total_storage_gb': 1000.0,
            'largest_tables': [
                {'table_name': 'large_table', 'size_gb': 200.0, 'time_travel_gb': 50.0}
            ]
        }
        
        recommendations = cost_analyzer._generate_cost_recommendations(
            warehouse_analysis, storage_analysis
        )
        
        assert len(recommendations) > 0
        assert any('underutilized' in rec.lower() for rec in recommendations)
    
    def test_generate_priority_alerts(self, cost_analyzer):
        """Test priority alert generation."""
        warehouse_analysis = {
            'total_cost': 2000.0,  # High cost
            'cost_distribution': [
                {'warehouse': 'WH1', 'credits': 1500.0, 'utilization': 20.0}  # Low utilization
            ]
        }
        
        storage_analysis = {
            'total_storage_gb': 5000.0,  # Large storage
            'largest_tables': [
                {'table_name': 'huge_table', 'size_gb': 1000.0}
            ]
        }
        
        alerts = cost_analyzer._generate_priority_alerts(warehouse_analysis, storage_analysis)
        
        assert len(alerts) > 0
        
        # Check alert structure
        for alert in alerts:
            assert 'severity' in alert
            assert 'message' in alert
            assert 'cost_impact' in alert
            assert alert['severity'] in ['low', 'medium', 'high', 'critical']


class TestUsageAnalyzer:
    """Test the UsageAnalyzer class."""
    
    @pytest.fixture
    def mock_connector(self):
        """Create a mock SnowflakeConnector."""
        return Mock()
    
    @pytest.fixture
    def usage_analyzer(self, mock_connector):
        """Create a UsageAnalyzer instance for testing."""
        return UsageAnalyzer(mock_connector)
    
    def test_analyze_warehouse_usage_patterns(self, usage_analyzer, mock_connector):
        """Test warehouse usage pattern analysis."""
        warehouse_data = pd.DataFrame({
            'warehouse_name': ['WH1', 'WH2'],
            'total_credits': [100.0, 200.0],
            'usage_count': [50, 75],
            'total_runtime_minutes': [1200, 1800]
        })
        
        query_data = pd.DataFrame({
            'warehouse_name': ['WH1', 'WH1', 'WH2'],
            'execution_time': [1000, 2000, 3000],
            'queued_time': [100, 200, 50],
            'compilation_time': [50, 75, 100]
        })
        
        mock_connector.get_warehouse_usage.return_value = warehouse_data
        mock_connector.get_query_history.return_value = query_data
        
        result = usage_analyzer.analyze_warehouse_usage_patterns(days=30)
        
        assert 'summary' in result
        assert 'utilization_metrics' in result
        assert 'usage_trends' in result
        assert 'recommendations' in result
        
        # Check summary
        summary = result['summary']
        assert summary['total_warehouses'] == 2
        assert summary['total_credits'] == 300.0
        
        # Check utilization metrics
        utilization_metrics = result['utilization_metrics']
        assert 'WH1' in utilization_metrics
        assert 'WH2' in utilization_metrics
        
        for warehouse, metrics in utilization_metrics.items():
            assert isinstance(metrics, UtilizationMetrics)
            assert hasattr(metrics, 'avg_utilization')
            assert hasattr(metrics, 'peak_utilization')
            assert hasattr(metrics, 'cost_efficiency_score')
    
    def test_calculate_utilization_metrics(self, usage_analyzer):
        """Test utilization metrics calculation."""
        warehouse_data = pd.DataFrame({
            'warehouse_name': ['WH1'],
            'total_credits': [100.0],
            'usage_count': [50],
            'total_runtime_minutes': [1200]
        })
        
        query_data = pd.DataFrame({
            'warehouse_name': ['WH1', 'WH1', 'WH1'],
            'execution_time': [1000, 2000, 1500],
            'queued_time': [100, 200, 150],
            'compilation_time': [50, 75, 60]
        })
        
        metrics = usage_analyzer._calculate_utilization_metrics('WH1', warehouse_data, query_data)
        
        assert isinstance(metrics, UtilizationMetrics)
        assert metrics.avg_utilization >= 0
        assert metrics.peak_utilization >= metrics.avg_utilization
        assert 0 <= metrics.cost_efficiency_score <= 100
        assert metrics.query_count == 3
        assert metrics.avg_execution_time > 0
    
    def test_identify_usage_trends(self, usage_analyzer):
        """Test usage trend identification."""
        usage_data = pd.DataFrame({
            'warehouse_name': ['WH1', 'WH1', 'WH2', 'WH2'],
            'date': ['2023-01-01', '2023-01-02', '2023-01-01', '2023-01-02'],
            'total_credits': [10, 15, 20, 25],
            'usage_count': [5, 8, 10, 12]
        })
        
        trends = usage_analyzer._identify_usage_trends(usage_data)
        
        assert 'daily_trends' in trends
        assert 'warehouse_growth' in trends
        
        # Check that we have trend data
        daily_trends = trends['daily_trends']
        assert len(daily_trends) > 0
    
    def test_generate_usage_recommendations(self, usage_analyzer):
        """Test usage recommendation generation."""
        utilization_metrics = {
            'WH1': UtilizationMetrics(
                avg_utilization=20.0,  # Low utilization
                peak_utilization=40.0,
                cost_efficiency_score=30.0,  # Poor efficiency
                query_count=100,
                avg_execution_time=5000,
                avg_queue_time=1000
            ),
            'WH2': UtilizationMetrics(
                avg_utilization=90.0,  # High utilization
                peak_utilization=95.0,
                cost_efficiency_score=85.0,
                query_count=500,
                avg_execution_time=2000,
                avg_queue_time=100
            )
        }
        
        recommendations = usage_analyzer._generate_usage_recommendations(utilization_metrics)
        
        assert len(recommendations) > 0
        
        # Should recommend downsizing for WH1 and possibly upsizing for WH2
        rec_text = ' '.join(recommendations).lower()
        assert any(keyword in rec_text for keyword in ['downsize', 'resize', 'optimize'])


class TestPerformanceAnalyzer:
    """Test the PerformanceAnalyzer class."""
    
    @pytest.fixture
    def mock_connector(self):
        """Create a mock SnowflakeConnector."""
        return Mock()
    
    @pytest.fixture
    def performance_analyzer(self, mock_connector):
        """Create a PerformanceAnalyzer instance for testing."""
        return PerformanceAnalyzer(mock_connector)
    
    def test_analyze_query_performance(self, performance_analyzer, mock_connector):
        """Test query performance analysis."""
        query_data = pd.DataFrame({
            'query_id': ['q1', 'q2', 'q3'],
            'warehouse_name': ['WH1', 'WH1', 'WH2'],
            'execution_time': [1000, 5000, 2000],
            'queued_time': [100, 500, 200],
            'compilation_time': [50, 100, 75],
            'bytes_scanned': [1000000, 5000000, 2000000],
            'rows_produced': [100, 1000, 500]
        })
        
        mock_connector.get_query_history.return_value = query_data
        
        result = performance_analyzer.analyze_query_performance(days=30)
        
        assert 'summary' in result
        assert 'slow_queries' in result
        assert 'performance_trends' in result
        assert 'recommendations' in result
        
        # Check summary
        summary = result['summary']
        assert summary['total_queries'] == 3
        assert summary['avg_execution_time'] > 0
        
        # Check slow queries identification
        slow_queries = result['slow_queries']
        assert len(slow_queries) > 0
        # Should identify q2 as slowest
        assert slow_queries[0]['query_id'] == 'q2'
    
    def test_identify_slow_queries(self, performance_analyzer):
        """Test slow query identification."""
        query_data = pd.DataFrame({
            'query_id': ['q1', 'q2', 'q3', 'q4'],
            'execution_time': [1000, 10000, 2000, 15000],  # q4 and q2 are slow
            'queued_time': [100, 500, 200, 300],
            'bytes_scanned': [1000000, 5000000, 2000000, 8000000]
        })
        
        slow_queries = performance_analyzer._identify_slow_queries(query_data)
        
        # Should return top slow queries, sorted by execution time
        assert len(slow_queries) <= 10  # Limited to top 10
        assert slow_queries[0]['query_id'] == 'q4'  # Slowest first
        assert slow_queries[1]['query_id'] == 'q2'  # Second slowest
    
    def test_calculate_performance_trends(self, performance_analyzer):
        """Test performance trend calculation."""
        query_data = pd.DataFrame({
            'query_id': ['q1', 'q2', 'q3'],
            'start_time': [
                datetime.now() - timedelta(days=2),
                datetime.now() - timedelta(days=1),
                datetime.now()
            ],
            'execution_time': [1000, 1200, 1100],
            'queued_time': [100, 150, 120]
        })
        
        trends = performance_analyzer._calculate_performance_trends(query_data)
        
        assert 'avg_execution_time_trend' in trends
        assert 'query_volume_trend' in trends
        assert isinstance(trends['avg_execution_time_trend'], (int, float))


class TestAccessAnalyzer:
    """Test the AccessAnalyzer class."""
    
    @pytest.fixture
    def mock_connector(self):
        """Create a mock SnowflakeConnector."""
        return Mock()
    
    @pytest.fixture
    def access_analyzer(self, mock_connector):
        """Create an AccessAnalyzer instance for testing."""
        return AccessAnalyzer(mock_connector)
    
    def test_analyze_data_access_patterns(self, access_analyzer, mock_connector):
        """Test data access pattern analysis."""
        access_data = pd.DataFrame({
            'user_name': ['user1', 'user2', 'user1'],
            'database_name': ['db1', 'db1', 'db2'],
            'schema_name': ['schema1', 'schema1', 'schema2'],
            'object_name': ['table1', 'table2', 'table3'],
            'privilege': ['SELECT', 'INSERT', 'SELECT'],
            'granted_on': ['TABLE', 'TABLE', 'TABLE']
        })
        
        query_data = pd.DataFrame({
            'user_name': ['user1', 'user2', 'user1'],
            'database_name': ['db1', 'db1', 'db2'],
            'query_text': ['SELECT * FROM table1', 'INSERT INTO table2', 'SELECT * FROM table3']
        })
        
        mock_connector.get_user_access_patterns.return_value = access_data
        mock_connector.get_query_history.return_value = query_data
        
        result = access_analyzer.analyze_data_access_patterns(days=30)
        
        assert 'user_access_summary' in result
        assert 'database_usage' in result
        assert 'privilege_analysis' in result
        assert 'compliance_alerts' in result
        assert 'recommendations' in result
        
        # Check user access summary
        user_summary = result['user_access_summary']
        assert 'user1' in user_summary
        assert 'user2' in user_summary
        
        # Check database usage
        db_usage = result['database_usage']
        assert 'db1' in db_usage
        assert 'db2' in db_usage
    
    def test_analyze_user_access(self, access_analyzer):
        """Test user access analysis."""
        access_data = pd.DataFrame({
            'user_name': ['user1', 'user1', 'user2'],
            'database_name': ['db1', 'db2', 'db1'],
            'privilege': ['SELECT', 'INSERT', 'SELECT'],
            'object_name': ['table1', 'table2', 'table1']
        })
        
        user_summary = access_analyzer._analyze_user_access(access_data)
        
        assert 'user1' in user_summary
        assert 'user2' in user_summary
        
        user1_data = user_summary['user1']
        assert user1_data['total_privileges'] == 2
        assert user1_data['databases_accessed'] == 2
        assert 'privileges' in user1_data
    
    def test_analyze_database_usage(self, access_analyzer):
        """Test database usage analysis."""
        access_data = pd.DataFrame({
            'database_name': ['db1', 'db1', 'db2'],
            'user_name': ['user1', 'user2', 'user1'],
            'privilege': ['SELECT', 'INSERT', 'SELECT']
        })
        
        db_usage = access_analyzer._analyze_database_usage(access_data)
        
        assert 'db1' in db_usage
        assert 'db2' in db_usage
        
        db1_data = db_usage['db1']
        assert db1_data['total_users'] == 2
        assert db1_data['total_privileges'] == 2
    
    def test_generate_compliance_alerts(self, access_analyzer):
        """Test compliance alert generation."""
        user_summary = {
            'user1': {
                'total_privileges': 50,  # High privilege count
                'databases_accessed': 10,
                'privileges': {'SELECT': 30, 'INSERT': 10, 'DELETE': 5, 'CREATE': 5}
            },
            'user2': {
                'total_privileges': 5,
                'databases_accessed': 1,
                'privileges': {'SELECT': 5}
            }
        }
        
        alerts = access_analyzer._generate_compliance_alerts(user_summary)
        
        assert len(alerts) > 0
        
        # Should flag user1 for excessive privileges
        alert_messages = [alert['message'] for alert in alerts]
        assert any('user1' in msg for msg in alert_messages)


if __name__ == "__main__":
    pytest.main([__file__]) 