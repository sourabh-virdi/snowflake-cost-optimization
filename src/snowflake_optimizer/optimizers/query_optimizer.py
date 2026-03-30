"""
Query optimization recommendations.
"""

import pandas as pd
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from loguru import logger
from ..connectors.snowflake_connector import SnowflakeConnector
from ..config.settings import get_settings


@dataclass
class QueryRecommendation:
    """Query optimization recommendation."""
    query_id: str
    recommendation_type: str  # 'index', 'rewrite', 'partition', 'materialize'
    current_performance: Dict[str, Any]
    recommended_changes: Dict[str, Any]
    estimated_improvement: float  # Performance improvement percentage
    confidence_score: float  # 0-1 confidence in recommendation
    implementation_effort: str  # 'low', 'medium', 'high'
    description: str


class QueryOptimizer:
    """
    Provides query optimization recommendations based on query performance analysis.
    """
    
    def __init__(self, connector: SnowflakeConnector):
        """Initialize query optimizer."""
        self.connector = connector
        self.settings = get_settings()
    
    def analyze_query_optimization_opportunities(self, days: int = 7) -> List[QueryRecommendation]:
        """
        Analyze queries and provide optimization recommendations.
        
        Args:
            days: Number of days to analyze for recommendations
            
        Returns:
            List of query optimization recommendations
        """
        logger.info(f"Analyzing query optimization opportunities for {days} days")
        
        recommendations = []
        
        try:
            # Get query performance data
            query_data = self.connector.get_query_history(days)
            
            if query_data.empty:
                logger.warning("No query data available for optimization analysis")
                return recommendations
            
            # Identify slow queries
            slow_queries = self._identify_slow_queries(query_data)
            
            # Generate recommendations for slow queries
            for _, query in slow_queries.iterrows():
                query_recommendations = self._analyze_single_query(query)
                recommendations.extend(query_recommendations)
            
            # Sort recommendations by potential impact
            recommendations.sort(key=lambda x: x.estimated_improvement, reverse=True)
            
        except Exception as e:
            logger.error(f"Error analyzing query optimization: {e}")
        
        return recommendations
    
    def _identify_slow_queries(self, query_data: pd.DataFrame) -> pd.DataFrame:
        """Identify slow-performing queries."""
        slow_threshold = self.settings.analysis.slow_query_threshold_ms
        
        # Filter queries that exceed the slow threshold
        slow_queries = query_data[
            query_data['total_elapsed_time'] > slow_threshold
        ].copy()
        
        # Sort by execution time (slowest first)
        slow_queries = slow_queries.sort_values('total_elapsed_time', ascending=False)
        
        # Return top 20 slowest queries
        return slow_queries.head(20)
    
    def _analyze_single_query(self, query_row: pd.Series) -> List[QueryRecommendation]:
        """Analyze a single query and generate recommendations."""
        recommendations = []
        
        try:
            query_id = query_row['query_id']
            execution_time = query_row['total_elapsed_time']
            bytes_scanned = query_row.get('bytes_scanned', 0)
            
            # Basic recommendation: if query scans a lot of data, suggest optimization
            if bytes_scanned > 1000000000:  # 1GB
                recommendations.append(QueryRecommendation(
                    query_id=query_id,
                    recommendation_type='partition',
                    current_performance={
                        'execution_time_ms': execution_time,
                        'bytes_scanned': bytes_scanned
                    },
                    recommended_changes={
                        'suggestion': 'Consider partitioning or clustering keys',
                        'expected_scan_reduction': '50-70%'
                    },
                    estimated_improvement=50.0,
                    confidence_score=0.7,
                    implementation_effort='medium',
                    description=f"Query scans {bytes_scanned/1000000000:.1f}GB - consider partitioning"
                ))
            
            # If query takes very long, suggest general optimization
            if execution_time > 60000:  # 1 minute
                recommendations.append(QueryRecommendation(
                    query_id=query_id,
                    recommendation_type='rewrite',
                    current_performance={
                        'execution_time_ms': execution_time
                    },
                    recommended_changes={
                        'suggestion': 'Review and optimize query logic',
                        'focus_areas': ['joins', 'subqueries', 'aggregations']
                    },
                    estimated_improvement=30.0,
                    confidence_score=0.6,
                    implementation_effort='high',
                    description=f"Query execution time: {execution_time/1000:.1f}s - review query structure"
                ))
                
        except Exception as e:
            logger.error(f"Error analyzing query {query_row.get('query_id', 'unknown')}: {e}")
        
        return recommendations
    
    def get_optimization_summary(self, recommendations: List[QueryRecommendation]) -> Dict[str, Any]:
        """Generate a summary of query optimization opportunities."""
        if not recommendations:
            return {'total_queries_analyzed': 0, 'avg_improvement': 0, 'summary': {}}
        
        total_improvement = sum(rec.estimated_improvement for rec in recommendations)
        avg_improvement = total_improvement / len(recommendations)
        
        recommendation_types = {}
        for rec in recommendations:
            rec_type = rec.recommendation_type
            recommendation_types[rec_type] = recommendation_types.get(rec_type, 0) + 1
        
        high_impact_recs = [rec for rec in recommendations if rec.estimated_improvement > 40]
        
        return {
            'total_queries_analyzed': len(recommendations),
            'avg_improvement_percentage': avg_improvement,
            'recommendation_types': recommendation_types,
            'high_impact_count': len(high_impact_recs),
            'top_opportunities': recommendations[:5] if recommendations else []
        } 