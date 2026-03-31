"""
Warehouse optimization recommendations.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from loguru import logger
from ..connectors.snowflake_connector import SnowflakeConnector
from ..config.settings import get_settings


@dataclass
class WarehouseRecommendation:
    """Warehouse optimization recommendation."""
    warehouse_name: str
    recommendation_type: str  # 'resize', 'auto_suspend', 'schedule', 'cluster'
    current_config: Dict[str, Any]
    recommended_config: Dict[str, Any]
    estimated_savings: float  # Monthly savings in credits/dollars
    confidence_score: float  # 0-1 confidence in recommendation
    implementation_effort: str  # 'low', 'medium', 'high'
    description: str
    impact_analysis: Dict[str, Any]


class WarehouseOptimizer:
    """
    Provides warehouse optimization recommendations based on usage patterns and cost analysis.
    """
    
    def __init__(self, connector: SnowflakeConnector):
        """Initialize warehouse optimizer."""
        self.connector = connector
        self.settings = get_settings()
        
        # Warehouse size mappings (approximate credits per hour)
        self.warehouse_sizes = {
            'X-Small': 1,
            'Small': 2,
            'Medium': 4,
            'Large': 8,
            'X-Large': 16,
            '2X-Large': 32,
            '3X-Large': 64,
            '4X-Large': 128
        }
    
    def analyze_warehouse_optimization_opportunities(self, days: int = 30) -> List[WarehouseRecommendation]:
        """
        Analyze warehouses and provide optimization recommendations.
        
        Args:
            days: Number of days to analyze for recommendations
            
        Returns:
            List of warehouse optimization recommendations
        """
        logger.info(f"Analyzing warehouse optimization opportunities for {days} days")
        
        recommendations = []
        
        try:
            # Get comprehensive warehouse data
            usage_data = self.connector.get_warehouse_usage(days)
            query_data = self.connector.get_query_history(days)
            
            if usage_data.empty:
                logger.warning("No usage data available for warehouse optimization")
                return recommendations
            
            # Analyze each warehouse
            for warehouse in usage_data['warehouse_name'].unique():
                warehouse_usage = usage_data[usage_data['warehouse_name'] == warehouse]
                warehouse_queries = query_data[query_data['warehouse_name'] == warehouse] if not query_data.empty else pd.DataFrame()
                
                # Generate recommendations for this warehouse
                warehouse_recommendations = self._analyze_single_warehouse(
                    warehouse, warehouse_usage, warehouse_queries, days
                )
                recommendations.extend(warehouse_recommendations)
            
            # Sort recommendations by potential savings
            recommendations.sort(key=lambda x: x.estimated_savings, reverse=True)
            
        except Exception as e:
            logger.error(f"Error analyzing warehouse optimization: {e}")
        
        return recommendations
    
    def _analyze_single_warehouse(self, warehouse_name: str, usage_data: pd.DataFrame, 
                                 query_data: pd.DataFrame, analysis_days: int) -> List[WarehouseRecommendation]:
        """Analyze a single warehouse and generate recommendations."""
        recommendations = []
        
        # Calculate basic metrics
        total_credits = usage_data['total_credits'].sum()
        avg_daily_credits = total_credits / analysis_days
        total_queries = usage_data['usage_count'].sum()
        avg_execution_minutes = usage_data['total_runtime_minutes'].mean()
        
        # Analyze utilization patterns
        utilization_analysis = self._analyze_utilization_patterns(warehouse_name, usage_data, query_data)
        
        # Generate sizing recommendations
        sizing_rec = self._generate_sizing_recommendation(
            warehouse_name, utilization_analysis, avg_daily_credits, total_queries
        )
        if sizing_rec:
            recommendations.append(sizing_rec)
        
        # Generate auto-suspend recommendations
        suspend_rec = self._generate_auto_suspend_recommendation(
            warehouse_name, usage_data, query_data, utilization_analysis
        )
        if suspend_rec:
            recommendations.append(suspend_rec)
        
        # Generate scheduling recommendations
        schedule_rec = self._generate_scheduling_recommendation(
            warehouse_name, usage_data, utilization_analysis
        )
        if schedule_rec:
            recommendations.append(schedule_rec)
        
        # Generate clustering recommendations for multi-cluster warehouses
        cluster_rec = self._generate_clustering_recommendation(
            warehouse_name, utilization_analysis, query_data
        )
        if cluster_rec:
            recommendations.append(cluster_rec)
        
        return recommendations
    
    def _analyze_utilization_patterns(self, warehouse_name: str, usage_data: pd.DataFrame, 
                                    query_data: pd.DataFrame) -> Dict[str, Any]:
        """Analyze detailed utilization patterns for a warehouse."""
        # Calculate utilization metrics
        daily_credits = usage_data.groupby('usage_date')['total_credits'].sum()
        credit_variance = daily_credits.var()
        credit_mean = daily_credits.mean()
        
        # Estimate peak vs off-peak usage
        usage_data['hour'] = pd.to_datetime(usage_data['usage_date']).dt.hour
        hourly_usage = usage_data.groupby('hour')['total_credits'].mean()
        
        peak_hours = hourly_usage.nlargest(8).index.tolist()  # Top 8 hours
        peak_usage = hourly_usage[peak_hours].sum()
        off_peak_usage = hourly_usage.drop(peak_hours).sum()
        
        # Calculate idle time estimation
        if not query_data.empty:
            total_execution_time = query_data['total_elapsed_time'].sum() / 1000 / 60  # Convert to minutes
            potential_runtime = len(usage_data) * 24 * 60  # Total possible minutes
            idle_percentage = max(0, (potential_runtime - total_execution_time) / potential_runtime * 100)
        else:
            idle_percentage = 50  # Default estimate
        
        # Calculate query patterns
        query_patterns = {}
        if not query_data.empty:
            query_patterns = {
                'avg_query_duration': query_data['total_elapsed_time'].mean() / 1000,  # seconds
                'query_count': len(query_data),
                'concurrent_queries': self._estimate_concurrency(query_data),
                'query_types': query_data['query_type'].value_counts().to_dict() if 'query_type' in query_data.columns else {}
            }
        
        return {
            'credit_variance': credit_variance,
            'credit_mean': credit_mean,
            'peak_hours': peak_hours,
            'peak_usage_ratio': peak_usage / (peak_usage + off_peak_usage) if (peak_usage + off_peak_usage) > 0 else 0,
            'idle_percentage': idle_percentage,
            'query_patterns': query_patterns,
            'daily_usage_pattern': daily_credits.to_dict()
        }
    
    def _generate_sizing_recommendation(self, warehouse_name: str, utilization: Dict[str, Any], 
                                      avg_daily_credits: float, total_queries: int) -> Optional[WarehouseRecommendation]:
        """Generate warehouse sizing recommendations."""
        try:
            # Estimate current size based on credit usage
            estimated_current_size = self._estimate_warehouse_size(avg_daily_credits)
            
            # Calculate efficiency metrics
            queries_per_credit = total_queries / max(avg_daily_credits * 30, 1)  # Monthly estimate
            idle_percentage = utilization.get('idle_percentage', 50)
            
            # Determine if resizing is beneficial
            recommended_size = None
            sizing_logic = ""
            
            # If warehouse has high idle time and low query volume, consider downsizing
            if idle_percentage > 70 and queries_per_credit < 50:
                current_size_index = list(self.warehouse_sizes.keys()).index(estimated_current_size)
                if current_size_index > 0:  # Can downsize
                    recommended_size = list(self.warehouse_sizes.keys())[current_size_index - 1]
                    sizing_logic = "High idle time and low query volume suggest downsizing opportunity"
            
            # If warehouse has low idle time and high concurrency, consider upsizing
            elif idle_percentage < 30 and utilization['query_patterns'].get('concurrent_queries', 0) > 10:
                current_size_index = list(self.warehouse_sizes.keys()).index(estimated_current_size)
                if current_size_index < len(self.warehouse_sizes) - 1:  # Can upsize
                    recommended_size = list(self.warehouse_sizes.keys())[current_size_index + 1]
                    sizing_logic = "Low idle time and high concurrency suggest upsizing for better performance"
            
            if recommended_size and recommended_size != estimated_current_size:
                # Calculate estimated savings/cost
                current_credits_per_hour = self.warehouse_sizes[estimated_current_size]
                recommended_credits_per_hour = self.warehouse_sizes[recommended_size]
                
                # Estimate monthly savings (negative means additional cost)
                estimated_monthly_savings = (current_credits_per_hour - recommended_credits_per_hour) * 24 * 30 * 0.7  # 70% utilization estimate
                
                # Calculate confidence based on data quality
                confidence = self._calculate_confidence_score(utilization, total_queries)
                
                return WarehouseRecommendation(
                    warehouse_name=warehouse_name,
                    recommendation_type='resize',
                    current_config={'size': estimated_current_size},
                    recommended_config={'size': recommended_size},
                    estimated_savings=estimated_monthly_savings,
                    confidence_score=confidence,
                    implementation_effort='low',
                    description=f"Resize warehouse from {estimated_current_size} to {recommended_size}. {sizing_logic}",
                    impact_analysis={
                        'performance_impact': 'improved' if recommended_credits_per_hour > current_credits_per_hour else 'maintained',
                        'cost_impact': estimated_monthly_savings,
                        'idle_time_reduction': idle_percentage * 0.3 if recommended_credits_per_hour < current_credits_per_hour else 0
                    }
                )
                
        except Exception as e:
            logger.error(f"Error generating sizing recommendation for {warehouse_name}: {e}")
        
        return None
    
    def _generate_auto_suspend_recommendation(self, warehouse_name: str, usage_data: pd.DataFrame,
                                            query_data: pd.DataFrame, utilization: Dict[str, Any]) -> Optional[WarehouseRecommendation]:
        """Generate auto-suspend recommendations."""
        try:
            idle_percentage = utilization.get('idle_percentage', 50)
            
            # If warehouse has significant idle time, recommend auto-suspend
            if idle_percentage > 50:
                # Analyze query patterns to determine optimal suspend time
                if not query_data.empty:
                    # Calculate gaps between queries
                    query_times = pd.to_datetime(query_data['start_time']).sort_values()
                    time_gaps = query_times.diff().dt.total_seconds() / 60  # Minutes
                    
                    # Find optimal suspend time (where we wouldn't interrupt normal usage)
                    optimal_suspend_minutes = max(1, min(60, int(time_gaps.quantile(0.7))))
                else:
                    optimal_suspend_minutes = 10  # Default
                
                # Estimate savings from auto-suspend
                estimated_idle_hours_per_day = 24 * (idle_percentage / 100)
                current_size = self._estimate_warehouse_size(usage_data['total_credits'].mean())
                credits_per_hour = self.warehouse_sizes.get(current_size, 4)
                
                # Conservative estimate: save 50% of idle time with auto-suspend
                estimated_monthly_savings = estimated_idle_hours_per_day * 30 * credits_per_hour * 0.5
                
                confidence = 0.8 if not query_data.empty else 0.6
                
                return WarehouseRecommendation(
                    warehouse_name=warehouse_name,
                    recommendation_type='auto_suspend',
                    current_config={'auto_suspend': 'unknown'},
                    recommended_config={'auto_suspend_minutes': optimal_suspend_minutes},
                    estimated_savings=estimated_monthly_savings,
                    confidence_score=confidence,
                    implementation_effort='low',
                    description=f"Enable auto-suspend after {optimal_suspend_minutes} minutes of inactivity",
                    impact_analysis={
                        'idle_time_savings': estimated_idle_hours_per_day,
                        'potential_restart_delays': 'minimal' if optimal_suspend_minutes > 5 else 'low',
                        'monthly_credit_savings': estimated_monthly_savings
                    }
                )
                
        except Exception as e:
            logger.error(f"Error generating auto-suspend recommendation for {warehouse_name}: {e}")
        
        return None
    
    def _generate_scheduling_recommendation(self, warehouse_name: str, usage_data: pd.DataFrame,
                                          utilization: Dict[str, Any]) -> Optional[WarehouseRecommendation]:
        """Generate warehouse scheduling recommendations."""
        try:
            peak_hours = utilization.get('peak_hours', [])
            peak_usage_ratio = utilization.get('peak_usage_ratio', 0.5)
            
            # If usage is heavily concentrated in specific hours, recommend scheduling
            if peak_usage_ratio > 0.7 and len(peak_hours) <= 10:
                # Suggest schedule-based warehouse management
                off_peak_hours = [h for h in range(24) if h not in peak_hours]
                
                # Estimate savings from shutting down during off-peak
                avg_daily_credits = usage_data['total_credits'].mean()
                off_peak_savings_ratio = 1 - peak_usage_ratio
                estimated_monthly_savings = avg_daily_credits * 30 * off_peak_savings_ratio * 0.8  # Conservative
                
                return WarehouseRecommendation(
                    warehouse_name=warehouse_name,
                    recommendation_type='schedule',
                    current_config={'schedule': 'always_on'},
                    recommended_config={
                        'peak_hours': peak_hours,
                        'off_peak_action': 'suspend',
                        'schedule_type': 'time_based'
                    },
                    estimated_savings=estimated_monthly_savings,
                    confidence_score=0.7,
                    implementation_effort='medium',
                    description=f"Implement time-based scheduling with peak hours: {peak_hours}",
                    impact_analysis={
                        'peak_hours_count': len(peak_hours),
                        'off_peak_savings': off_peak_savings_ratio,
                        'schedule_complexity': 'medium'
                    }
                )
                
        except Exception as e:
            logger.error(f"Error generating scheduling recommendation for {warehouse_name}: {e}")
        
        return None
    
    def _generate_clustering_recommendation(self, warehouse_name: str, utilization: Dict[str, Any],
                                          query_data: pd.DataFrame) -> Optional[WarehouseRecommendation]:
        """Generate multi-cluster warehouse recommendations."""
        try:
            if query_data.empty:
                return None
            
            concurrent_queries = utilization['query_patterns'].get('concurrent_queries', 0)
            query_count = utilization['query_patterns'].get('query_count', 0)
            
            # If high concurrency and query volume, consider multi-cluster
            if concurrent_queries > 15 and query_count > 1000:
                # Estimate optimal cluster count
                optimal_clusters = min(10, max(2, int(concurrent_queries / 5)))
                
                # Multi-cluster warehouses can be more expensive but improve performance
                # Conservative savings estimate focusing on performance improvement value
                estimated_performance_value = concurrent_queries * 0.1  # Value of reduced queuing
                
                return WarehouseRecommendation(
                    warehouse_name=warehouse_name,
                    recommendation_type='cluster',
                    current_config={'clusters': 1},
                    recommended_config={
                        'min_clusters': 1,
                        'max_clusters': optimal_clusters,
                        'scaling_policy': 'auto'
                    },
                    estimated_savings=-estimated_performance_value,  # Negative = additional cost
                    confidence_score=0.6,
                    implementation_effort='high',
                    description=f"Consider multi-cluster warehouse (1-{optimal_clusters} clusters) for high concurrency workloads",
                    impact_analysis={
                        'concurrency_improvement': concurrent_queries,
                        'queue_time_reduction': 'significant',
                        'cost_increase': 'moderate_to_high',
                        'performance_gain': 'high'
                    }
                )
                
        except Exception as e:
            logger.error(f"Error generating clustering recommendation for {warehouse_name}: {e}")
        
        return None
    
    def _estimate_warehouse_size(self, avg_daily_credits: float) -> str:
        """Estimate warehouse size based on average daily credit usage."""
        # Rough estimation: assume 12 hours average daily usage
        estimated_credits_per_hour = avg_daily_credits / 12
        
        # Find closest warehouse size
        closest_size = 'Medium'  # Default
        min_diff = float('inf')
        
        for size, credits_per_hour in self.warehouse_sizes.items():
            diff = abs(credits_per_hour - estimated_credits_per_hour)
            if diff < min_diff:
                min_diff = diff
                closest_size = size
        
        return closest_size
    
    def _estimate_concurrency(self, query_data: pd.DataFrame) -> int:
        """Estimate average query concurrency."""
        if query_data.empty:
            return 0
        
        try:
            # Convert timestamps and calculate overlaps
            query_data = query_data.copy()
            query_data['start_time'] = pd.to_datetime(query_data['start_time'])
            query_data['end_time'] = pd.to_datetime(query_data['end_time'])
            
            # Simple concurrency estimation using time windows
            max_concurrency = 0
            time_windows = pd.date_range(
                start=query_data['start_time'].min(),
                end=query_data['end_time'].max(),
                freq='1min'
            )
            
            # Sample every 10th time window for performance
            for timestamp in time_windows[::10]:
                concurrent = len(query_data[
                    (query_data['start_time'] <= timestamp) & 
                    (query_data['end_time'] >= timestamp)
                ])
                max_concurrency = max(max_concurrency, concurrent)
            
            return max_concurrency
            
        except Exception as e:
            logger.error(f"Error estimating concurrency: {e}")
            return 0
    
    def _calculate_confidence_score(self, utilization: Dict[str, Any], total_queries: int) -> float:
        """Calculate confidence score for recommendations."""
        base_confidence = 0.5
        
        # Increase confidence with more data
        if total_queries > 1000:
            base_confidence += 0.2
        elif total_queries > 100:
            base_confidence += 0.1
        
        # Increase confidence with consistent patterns
        credit_variance = utilization.get('credit_variance', 0)
        credit_mean = utilization.get('credit_mean', 1)
        
        if credit_mean > 0:
            cv = credit_variance / credit_mean  # Coefficient of variation
            if cv < 0.5:  # Consistent usage
                base_confidence += 0.2
        
        # Increase confidence with clear usage patterns
        peak_usage_ratio = utilization.get('peak_usage_ratio', 0.5)
        if peak_usage_ratio > 0.7 or peak_usage_ratio < 0.3:  # Clear peak or distributed pattern
            base_confidence += 0.1
        
        return min(1.0, base_confidence)
    
    def get_optimization_summary(self, recommendations: List[WarehouseRecommendation]) -> Dict[str, Any]:
        """Generate a summary of optimization opportunities."""
        if not recommendations:
            return {'total_savings': 0, 'recommendation_count': 0, 'summary': {}}
        
        total_savings = sum(rec.estimated_savings for rec in recommendations if rec.estimated_savings > 0)
        total_additional_cost = sum(abs(rec.estimated_savings) for rec in recommendations if rec.estimated_savings < 0)
        
        recommendation_types = {}
        for rec in recommendations:
            rec_type = rec.recommendation_type
            recommendation_types[rec_type] = recommendation_types.get(rec_type, 0) + 1
        
        high_confidence_recs = [rec for rec in recommendations if rec.confidence_score > 0.7]
        
        return {
            'total_potential_savings': total_savings,
            'total_additional_cost': total_additional_cost,
            'net_savings': total_savings - total_additional_cost,
            'recommendation_count': len(recommendations),
            'high_confidence_count': len(high_confidence_recs),
            'recommendation_types': recommendation_types,
            'top_opportunities': sorted(recommendations, key=lambda x: x.estimated_savings, reverse=True)[:5]
        } 