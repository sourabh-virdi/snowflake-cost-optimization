"""
Performance analysis module for Snowflake optimization.
"""

import pandas as pd
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from loguru import logger
from ..connectors.snowflake_connector import SnowflakeConnector
from ..config.settings import get_settings


@dataclass
class PerformanceMetric:
    """Performance metric data structure."""
    metric_name: str
    value: float
    threshold: float
    status: str  # 'good', 'warning', 'critical'
    description: str


class PerformanceAnalyzer:
    """
    Analyzes Snowflake query performance and identifies optimization opportunities.
    """
    
    def __init__(self, connector: SnowflakeConnector):
        """Initialize performance analyzer with Snowflake connector."""
        self.connector = connector
        self.settings = get_settings()
        
    def analyze_query_performance(self, days: int = 7) -> Dict[str, Any]:
        """
        Analyze query performance metrics.
        
        Args:
            days: Number of days to analyze
            
        Returns:
            Dictionary containing performance analysis results
        """
        logger.info(f"Analyzing query performance for {days} days")
        
        try:
            # Get query history data
            query_data = self.connector.get_query_history(days)
            
            if query_data.empty:
                logger.warning("No query data available for performance analysis")
                return {'metrics': [], 'recommendations': []}
            
            # Calculate performance metrics
            metrics = self._calculate_performance_metrics(query_data)
            
            # Identify slow queries
            slow_queries = self._identify_slow_queries(query_data)
            
            # Generate recommendations
            recommendations = self._generate_performance_recommendations(metrics, slow_queries)
            
            return {
                'metrics': metrics,
                'slow_queries': slow_queries,
                'recommendations': recommendations,
                'summary': self._generate_performance_summary(query_data, metrics)
            }
            
        except Exception as e:
            logger.error(f"Error analyzing query performance: {e}")
            return {'metrics': [], 'recommendations': []}
    
    def _calculate_performance_metrics(self, query_data: pd.DataFrame) -> List[PerformanceMetric]:
        """Calculate key performance metrics."""
        metrics = []
        
        try:
            # Average query execution time
            avg_execution_time = query_data['total_elapsed_time'].mean() / 1000  # Convert to seconds
            execution_threshold = self.settings.analysis.slow_query_threshold_ms / 1000
            
            metrics.append(PerformanceMetric(
                metric_name="Average Query Execution Time",
                value=avg_execution_time,
                threshold=execution_threshold,
                status="good" if avg_execution_time < execution_threshold else "warning",
                description=f"Average execution time across all queries"
            ))
            
            # Query throughput (queries per minute)
            time_span_hours = (query_data['start_time'].max() - query_data['start_time'].min()).total_seconds() / 3600
            throughput = len(query_data) / max(time_span_hours * 60, 1)  # Queries per minute
            
            metrics.append(PerformanceMetric(
                metric_name="Query Throughput",
                value=throughput,
                threshold=10.0,  # Arbitrary threshold
                status="good" if throughput > 10 else "warning",
                description="Number of queries executed per minute"
            ))
            
        except Exception as e:
            logger.error(f"Error calculating performance metrics: {e}")
        
        return metrics
    
    def _identify_slow_queries(self, query_data: pd.DataFrame) -> List[Dict[str, Any]]:
        """Identify and analyze slow queries."""
        slow_threshold = self.settings.analysis.slow_query_threshold_ms
        
        slow_queries = query_data[query_data['total_elapsed_time'] > slow_threshold].copy()
        
        if slow_queries.empty:
            return []
        
        # Sort by execution time
        slow_queries = slow_queries.nlargest(10, 'total_elapsed_time')
        
        slow_query_list = []
        for _, query in slow_queries.iterrows():
            slow_query_list.append({
                'query_id': query['query_id'],
                'execution_time_seconds': query['total_elapsed_time'] / 1000,
                'warehouse': query.get('warehouse_name', 'Unknown'),
                'user': query.get('user_name', 'Unknown'),
                'query_type': query.get('query_type', 'Unknown'),
                'start_time': query['start_time']
            })
        
        return slow_query_list
    
    def _generate_performance_recommendations(self, metrics: List[PerformanceMetric], 
                                            slow_queries: List[Dict]) -> List[str]:
        """Generate performance optimization recommendations."""
        recommendations = []
        
        # Check for slow average execution time
        avg_time_metric = next((m for m in metrics if "Average Query Execution Time" in m.metric_name), None)
        if avg_time_metric and avg_time_metric.status != "good":
            recommendations.append("Consider optimizing query patterns or increasing warehouse size")
        
        # Check for slow queries
        if len(slow_queries) > 5:
            recommendations.append("Multiple slow queries detected - review query optimization opportunities")
        
        # Check for low throughput
        throughput_metric = next((m for m in metrics if "Throughput" in m.metric_name), None)
        if throughput_metric and throughput_metric.status != "good":
            recommendations.append("Low query throughput - consider warehouse scaling or query optimization")
        
        if not recommendations:
            recommendations.append("Query performance appears optimal")
        
        return recommendations
    
    def _generate_performance_summary(self, query_data: pd.DataFrame, 
                                    metrics: List[PerformanceMetric]) -> Dict[str, Any]:
        """Generate performance analysis summary."""
        if query_data.empty:
            return {}
        
        return {
            'total_queries_analyzed': len(query_data),
            'analysis_time_span_hours': (query_data['start_time'].max() - query_data['start_time'].min()).total_seconds() / 3600,
            'avg_execution_time_seconds': query_data['total_elapsed_time'].mean() / 1000,
            'longest_query_seconds': query_data['total_elapsed_time'].max() / 1000,
            'total_execution_time_hours': query_data['total_elapsed_time'].sum() / 1000 / 3600,
            'performance_score': self._calculate_performance_score(metrics)
        }
    
    def _calculate_performance_score(self, metrics: List[PerformanceMetric]) -> float:
        """Calculate overall performance score (0-100)."""
        if not metrics:
            return 50.0  # Default score
        
        good_metrics = len([m for m in metrics if m.status == "good"])
        score = (good_metrics / len(metrics)) * 100
        
        return round(score, 1) 