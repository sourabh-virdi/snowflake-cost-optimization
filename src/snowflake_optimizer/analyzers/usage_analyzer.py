"""
Usage pattern analysis module for Snowflake optimization.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from dataclasses import dataclass
from loguru import logger
from ..connectors.snowflake_connector import SnowflakeConnector
from ..config.settings import get_settings


@dataclass
class UsagePattern:
    """Usage pattern data structure."""
    pattern_type: str
    description: str
    peak_hours: List[int]
    peak_days: List[str]
    utilization_score: float
    recommendations: List[str]


@dataclass
class WarehouseUtilization:
    """Warehouse utilization metrics."""
    warehouse_name: str
    avg_utilization: float
    peak_utilization: float
    idle_time_percentage: float
    total_queries: int
    avg_query_duration: float
    cost_efficiency_score: float


class UsageAnalyzer:
    """
    Analyzes Snowflake usage patterns to identify optimization opportunities.
    """
    
    def __init__(self, connector: SnowflakeConnector):
        """Initialize usage analyzer with Snowflake connector."""
        self.connector = connector
        self.settings = get_settings()
        
    def analyze_warehouse_usage_patterns(self, days: int = 30) -> Dict[str, Any]:
        """
        Analyze warehouse usage patterns over time.
        
        Args:
            days: Number of days to analyze
            
        Returns:
            Dictionary containing usage pattern analysis
        """
        logger.info(f"Analyzing warehouse usage patterns for {days} days")
        
        # Get warehouse usage data
        usage_data = self.connector.get_warehouse_usage(days)
        
        if usage_data.empty:
            logger.warning("No warehouse usage data found")
            return {'patterns': [], 'utilization': {}, 'recommendations': []}
        
        # Analyze temporal patterns
        temporal_patterns = self._analyze_temporal_patterns(usage_data)
        
        # Calculate warehouse utilization metrics
        utilization_metrics = self._calculate_utilization_metrics(usage_data)
        
        # Identify usage anomalies
        anomalies = self._identify_usage_anomalies(usage_data)
        
        # Generate optimization recommendations
        recommendations = self._generate_usage_recommendations(
            temporal_patterns, utilization_metrics, anomalies
        )
        
        return {
            'temporal_patterns': temporal_patterns,
            'utilization_metrics': utilization_metrics,
            'anomalies': anomalies,
            'recommendations': recommendations,
            'summary': self._generate_usage_summary(usage_data, utilization_metrics)
        }
    
    def analyze_user_activity_patterns(self, days: int = 30) -> Dict[str, Any]:
        """Analyze user activity patterns and access behavior."""
        logger.info(f"Analyzing user activity patterns for {days} days")
        
        # Get query history for user analysis
        query_data = self.connector.get_query_history(days)
        
        if query_data.empty:
            logger.warning("No query history data found")
            return {'user_patterns': {}, 'peak_users': [], 'recommendations': []}
        
        # Analyze user activity patterns
        user_patterns = self._analyze_user_patterns(query_data)
        
        # Identify peak usage users
        peak_users = self._identify_peak_users(query_data)
        
        # Analyze query patterns by user
        query_patterns = self._analyze_query_patterns_by_user(query_data)
        
        return {
            'user_patterns': user_patterns,
            'peak_users': peak_users,
            'query_patterns': query_patterns,
            'user_recommendations': self._generate_user_recommendations(user_patterns, peak_users)
        }
    
    def analyze_resource_utilization(self, days: int = 30) -> Dict[str, Any]:
        """Analyze overall resource utilization across warehouses."""
        logger.info(f"Analyzing resource utilization for {days} days")
        
        # Get comprehensive usage data
        usage_data = self.connector.get_warehouse_usage(days)
        query_data = self.connector.get_query_history(days)
        
        if usage_data.empty or query_data.empty:
            logger.warning("Insufficient data for resource utilization analysis")
            return {'utilization_scores': {}, 'efficiency_metrics': {}, 'recommendations': []}
        
        # Calculate resource efficiency metrics
        efficiency_metrics = self._calculate_resource_efficiency(usage_data, query_data)
        
        # Identify bottlenecks
        bottlenecks = self._identify_resource_bottlenecks(query_data)
        
        # Calculate utilization scores
        utilization_scores = self._calculate_utilization_scores(usage_data, query_data)
        
        return {
            'efficiency_metrics': efficiency_metrics,
            'bottlenecks': bottlenecks,
            'utilization_scores': utilization_scores,
            'optimization_opportunities': self._identify_optimization_opportunities(
                efficiency_metrics, bottlenecks, utilization_scores
            )
        }
    
    def _analyze_temporal_patterns(self, usage_data: pd.DataFrame) -> List[UsagePattern]:
        """Analyze temporal usage patterns."""
        patterns = []
        
        try:
            # Add time components for analysis
            usage_data['hour'] = pd.to_datetime(usage_data['usage_date']).dt.hour
            usage_data['day_of_week'] = pd.to_datetime(usage_data['usage_date']).dt.day_name()
            
            # Analyze hourly patterns
            hourly_usage = usage_data.groupby('hour')['total_credits'].mean()
            peak_hours = hourly_usage.nlargest(6).index.tolist()
            
            # Analyze daily patterns
            daily_usage = usage_data.groupby('day_of_week')['total_credits'].mean()
            peak_days = daily_usage.nlargest(3).index.tolist()
            
            # Calculate utilization score
            utilization_variance = hourly_usage.var()
            utilization_score = 1.0 / (1.0 + utilization_variance / hourly_usage.mean()) if hourly_usage.mean() > 0 else 0
            
            # Generate recommendations based on patterns
            recommendations = []
            if len(peak_hours) <= 8:  # Concentrated usage
                recommendations.append("Consider implementing auto-suspend during off-peak hours")
            if utilization_variance > hourly_usage.mean():
                recommendations.append("Usage varies significantly - consider dynamic warehouse scaling")
            
            patterns.append(UsagePattern(
                pattern_type="temporal",
                description="Overall temporal usage patterns",
                peak_hours=peak_hours,
                peak_days=peak_days,
                utilization_score=utilization_score,
                recommendations=recommendations
            ))
            
        except Exception as e:
            logger.error(f"Error analyzing temporal patterns: {e}")
        
        return patterns
    
    def _calculate_utilization_metrics(self, usage_data: pd.DataFrame) -> Dict[str, WarehouseUtilization]:
        """Calculate detailed utilization metrics for each warehouse."""
        metrics = {}
        
        for warehouse in usage_data['warehouse_name'].unique():
            warehouse_data = usage_data[usage_data['warehouse_name'] == warehouse]
            
            # Calculate basic metrics
            avg_credits = warehouse_data['total_credits'].mean()
            peak_credits = warehouse_data['total_credits'].max()
            total_queries = warehouse_data['usage_count'].sum()
            avg_execution_time = warehouse_data['total_runtime_minutes'].mean()
            
            # Estimate utilization (simplified)
            avg_utilization = min(avg_credits / 10.0, 1.0)  # Normalize to 0-1 scale
            peak_utilization = min(peak_credits / 10.0, 1.0)
            
            # Calculate idle time percentage (simplified estimation)
            total_possible_time = len(warehouse_data) * 24 * 60  # Total possible minutes
            actual_execution_time = warehouse_data['total_runtime_minutes'].sum()
            idle_time_percentage = max(0, (total_possible_time - actual_execution_time) / total_possible_time * 100)
            
            # Cost efficiency score
            if avg_credits > 0:
                cost_efficiency = (total_queries / avg_credits) * (1 / (1 + idle_time_percentage / 100))
            else:
                cost_efficiency = 0
            
            metrics[warehouse] = WarehouseUtilization(
                warehouse_name=warehouse,
                avg_utilization=round(avg_utilization, 3),
                peak_utilization=round(peak_utilization, 3),
                idle_time_percentage=round(idle_time_percentage, 2),
                total_queries=int(total_queries),
                avg_query_duration=round(avg_execution_time, 2),
                cost_efficiency_score=round(cost_efficiency, 3)
            )
        
        return metrics
    
    def _identify_usage_anomalies(self, usage_data: pd.DataFrame) -> List[Dict[str, Any]]:
        """Identify unusual usage patterns or anomalies."""
        anomalies = []
        
        try:
            for warehouse in usage_data['warehouse_name'].unique():
                warehouse_data = usage_data[usage_data['warehouse_name'] == warehouse]
                
                # Check for sudden spikes in usage
                mean_credits = warehouse_data['total_credits'].mean()
                std_credits = warehouse_data['total_credits'].std()
                
                spikes = warehouse_data[warehouse_data['total_credits'] > mean_credits + 2 * std_credits]
                
                for _, spike in spikes.iterrows():
                    anomalies.append({
                        'type': 'usage_spike',
                        'warehouse': warehouse,
                        'date': spike['usage_date'],
                        'credits': spike['total_credits'],
                        'severity': 'high' if spike['total_credits'] > mean_credits + 3 * std_credits else 'medium',
                        'description': f"Unusual credit usage spike: {spike['total_credits']:.2f} credits"
                    })
                
                # Check for unusually long idle periods
                if len(warehouse_data) > 7:  # Need enough data
                    recent_activity = warehouse_data.tail(3)['total_credits'].mean()
                    if recent_activity < mean_credits * 0.1:  # Less than 10% of average
                        anomalies.append({
                            'type': 'low_activity',
                            'warehouse': warehouse,
                            'date': warehouse_data['usage_date'].max(),
                            'credits': recent_activity,
                            'severity': 'medium',
                            'description': f"Unusually low activity: {recent_activity:.2f} credits"
                        })
        
        except Exception as e:
            logger.error(f"Error identifying usage anomalies: {e}")
        
        return anomalies
    
    def _analyze_user_patterns(self, query_data: pd.DataFrame) -> Dict[str, Any]:
        """Analyze user activity patterns."""
        if query_data.empty:
            return {}
        
        # Add time components
        query_data['hour'] = pd.to_datetime(query_data['start_time']).dt.hour
        query_data['day_of_week'] = pd.to_datetime(query_data['start_time']).dt.day_name()
        
        # User activity summary
        user_summary = query_data.groupby('user_name').agg({
            'query_id': 'count',
            'total_elapsed_time': ['mean', 'sum'],
            'credits_used_cloud_services': 'sum',
            'bytes_scanned': 'sum'
        }).round(2)
        
        user_summary.columns = [
            'total_queries', 'avg_duration_ms', 'total_duration_ms', 
            'total_credits', 'total_bytes_scanned'
        ]
        
        # Peak activity hours by user
        user_hourly = query_data.groupby(['user_name', 'hour']).size().reset_index(name='query_count')
        user_peak_hours = user_hourly.loc[user_hourly.groupby('user_name')['query_count'].idxmax()]
        
        return {
            'user_summary': user_summary.to_dict('index'),
            'peak_hours_by_user': user_peak_hours.set_index('user_name')[['hour', 'query_count']].to_dict('index')
        }
    
    def _identify_peak_users(self, query_data: pd.DataFrame, top_n: int = 10) -> List[Dict[str, Any]]:
        """Identify users with highest resource consumption."""
        if query_data.empty:
            return []
        
        user_metrics = query_data.groupby('user_name').agg({
            'query_id': 'count',
            'total_elapsed_time': 'sum',
            'credits_used_cloud_services': 'sum',
            'bytes_scanned': 'sum'
        }).round(2)
        
        user_metrics.columns = ['total_queries', 'total_duration_ms', 'total_credits', 'total_bytes_scanned']
        
        # Calculate a composite score for "peak usage"
        user_metrics['usage_score'] = (
            user_metrics['total_queries'] * 0.3 +
            (user_metrics['total_duration_ms'] / 1000000) * 0.4 +  # Convert to seconds, scale down
            user_metrics['total_credits'] * 0.3
        )
        
        top_users = user_metrics.nlargest(top_n, 'usage_score')
        
        return top_users.to_dict('index')
    
    def _analyze_query_patterns_by_user(self, query_data: pd.DataFrame) -> Dict[str, Any]:
        """Analyze query patterns and types by user."""
        if query_data.empty:
            return {}
        
        # Query type distribution by user
        query_types = query_data.groupby(['user_name', 'query_type']).size().unstack(fill_value=0)
        
        # Most frequent queries per user (simplified)
        frequent_patterns = {}
        for user in query_data['user_name'].unique():
            user_queries = query_data[query_data['user_name'] == user]
            
            # Simple pattern analysis - group by query length and warehouse
            patterns = user_queries.groupby(['warehouse_name']).agg({
                'query_id': 'count',
                'total_elapsed_time': 'mean'
            }).round(2)
            
            patterns.columns = ['query_count', 'avg_duration_ms']
            frequent_patterns[user] = patterns.to_dict('index')
        
        return {
            'query_type_distribution': query_types.to_dict('index'),
            'frequent_patterns': frequent_patterns
        }
    
    def _calculate_resource_efficiency(self, usage_data: pd.DataFrame, query_data: pd.DataFrame) -> Dict[str, Any]:
        """Calculate resource efficiency metrics."""
        efficiency_metrics = {}
        
        if not usage_data.empty and not query_data.empty:
            # Merge data for analysis
            query_summary = query_data.groupby('warehouse_name').agg({
                'query_id': 'count',
                'total_elapsed_time': 'mean',
                'bytes_scanned': 'mean'
            }).round(2)
            
            usage_summary = usage_data.groupby('warehouse_name').agg({
                'total_credits': 'mean',
                'usage_count': 'mean'
            }).round(2)
            
            # Calculate efficiency metrics
            for warehouse in usage_summary.index:
                if warehouse in query_summary.index:
                    efficiency_metrics[warehouse] = {
                        'queries_per_credit': usage_summary.loc[warehouse, 'usage_count'] / 
                                            max(usage_summary.loc[warehouse, 'total_credits'], 0.001),
                        'avg_query_duration': query_summary.loc[warehouse, 'total_elapsed_time'],
                        'avg_bytes_scanned': query_summary.loc[warehouse, 'bytes_scanned']
                    }
        
        return efficiency_metrics
    
    def _identify_resource_bottlenecks(self, query_data: pd.DataFrame) -> List[Dict[str, Any]]:
        """Identify potential resource bottlenecks."""
        bottlenecks = []
        
        if not query_data.empty:
            # Identify warehouses with high queuing times
            warehouse_queue_times = query_data.groupby('warehouse_name').agg({
                'queued_provisioning_time': 'mean',
                'queued_overload_time': 'mean',
                'queued_repair_time': 'mean'
            }).round(2)
            
            for warehouse, times in warehouse_queue_times.iterrows():
                total_queue_time = times.sum()
                if total_queue_time > 1000:  # More than 1 second average queuing
                    bottlenecks.append({
                        'warehouse': warehouse,
                        'type': 'queuing_bottleneck',
                        'avg_queue_time_ms': total_queue_time,
                        'severity': 'high' if total_queue_time > 5000 else 'medium'
                    })
        
        return bottlenecks
    
    def _calculate_utilization_scores(self, usage_data: pd.DataFrame, query_data: pd.DataFrame) -> Dict[str, float]:
        """Calculate overall utilization scores for warehouses."""
        scores = {}
        
        for warehouse in usage_data['warehouse_name'].unique():
            warehouse_usage = usage_data[usage_data['warehouse_name'] == warehouse]
            warehouse_queries = query_data[query_data['warehouse_name'] == warehouse] if not query_data.empty else pd.DataFrame()
            
            # Calculate various utilization metrics
            credit_consistency = 1.0 - (warehouse_usage['total_credits'].std() / max(warehouse_usage['total_credits'].mean(), 0.001))
            query_volume = min(warehouse_usage['usage_count'].sum() / 1000, 1.0)  # Normalize
            
            # Combine metrics into overall score
            overall_score = (credit_consistency * 0.5 + query_volume * 0.5)
            scores[warehouse] = round(max(0.0, min(1.0, overall_score)), 3)
        
        return scores
    
    def _generate_usage_recommendations(self, patterns: List[UsagePattern], 
                                      utilization: Dict[str, WarehouseUtilization], 
                                      anomalies: List[Dict]) -> List[str]:
        """Generate usage optimization recommendations."""
        recommendations = []
        
        # Analyze patterns for recommendations
        if patterns:
            pattern = patterns[0]  # Primary pattern
            if pattern.utilization_score < 0.5:
                recommendations.append("Consider implementing auto-scaling based on usage patterns")
            
            if len(pattern.peak_hours) <= 6:
                recommendations.append("Usage is concentrated in specific hours - optimize warehouse scheduling")
        
        # Analyze utilization for recommendations
        low_utilization_warehouses = [w for w, u in utilization.items() if u.avg_utilization < 0.3]
        if low_utilization_warehouses:
            recommendations.append(f"Consider downsizing or consolidating low-utilization warehouses: {', '.join(low_utilization_warehouses[:3])}")
        
        high_idle_warehouses = [w for w, u in utilization.items() if u.idle_time_percentage > 70]
        if high_idle_warehouses:
            recommendations.append(f"Implement auto-suspend for warehouses with high idle time: {', '.join(high_idle_warehouses[:3])}")
        
        # Analyze anomalies for recommendations
        if len(anomalies) > 5:
            recommendations.append("Multiple usage anomalies detected - review query patterns and user access")
        
        if not recommendations:
            recommendations.append("Usage patterns appear optimal - continue monitoring")
        
        return recommendations
    
    def _generate_usage_summary(self, usage_data: pd.DataFrame, utilization: Dict[str, WarehouseUtilization]) -> Dict[str, Any]:
        """Generate a summary of usage analysis."""
        if usage_data.empty:
            return {}
        
        total_warehouses = len(usage_data['warehouse_name'].unique())
        total_credits = usage_data['total_credits'].sum()
        avg_daily_credits = total_credits / len(usage_data['usage_date'].unique())
        
        # Calculate average utilization across all warehouses
        avg_utilization = np.mean([u.avg_utilization for u in utilization.values()]) if utilization else 0
        
        return {
            'total_warehouses': total_warehouses,
            'total_credits_analyzed': round(total_credits, 2),
            'avg_daily_credits': round(avg_daily_credits, 2),
            'avg_utilization_score': round(avg_utilization, 3),
            'analysis_period_days': len(usage_data['usage_date'].unique())
        }
    
    def _generate_user_recommendations(self, user_patterns: Dict[str, Any], peak_users: List[Dict]) -> List[str]:
        """Generate user-specific recommendations."""
        recommendations = []
        
        if peak_users:
            top_user = list(peak_users.keys())[0] if peak_users else None
            if top_user:
                recommendations.append(f"Review query patterns for top user: {top_user}")
        
        if len(peak_users) > 50:  # Many active users
            recommendations.append("Consider implementing query governance and user training")
        
        return recommendations
    
    def _identify_optimization_opportunities(self, efficiency_metrics: Dict, bottlenecks: List, utilization_scores: Dict) -> List[str]:
        """Identify optimization opportunities based on all analysis."""
        opportunities = []
        
        # Check efficiency metrics
        low_efficiency_warehouses = [w for w, m in efficiency_metrics.items() 
                                   if m.get('queries_per_credit', 0) < 1.0]
        if low_efficiency_warehouses:
            opportunities.append(f"Optimize query efficiency for warehouses: {', '.join(low_efficiency_warehouses[:3])}")
        
        # Check bottlenecks
        if bottlenecks:
            opportunities.append("Address queuing bottlenecks with warehouse scaling or query optimization")
        
        # Check utilization scores
        low_utilization = [w for w, s in utilization_scores.items() if s < 0.4]
        if low_utilization:
            opportunities.append(f"Improve utilization for warehouses: {', '.join(low_utilization[:3])}")
        
        return opportunities 