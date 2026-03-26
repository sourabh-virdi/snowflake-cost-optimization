"""
Cost analysis module for Snowflake usage patterns.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from dataclasses import dataclass
from loguru import logger
from sklearn.ensemble import IsolationForest
from ..connectors.snowflake_connector import SnowflakeConnector
from ..config.settings import get_settings


@dataclass
class CostAlert:
    """Cost alert data structure."""
    alert_type: str
    severity: str  # 'low', 'medium', 'high', 'critical'
    message: str
    details: Dict[str, Any]
    affected_resource: str
    cost_impact: float
    recommendation: str


@dataclass
class CostTrend:
    """Cost trend analysis result."""
    period: str
    trend_direction: str  # 'increasing', 'decreasing', 'stable'
    trend_magnitude: float
    projected_monthly_cost: float
    confidence_score: float


class CostAnalyzer:
    """
    Analyzes Snowflake costs across warehouses, storage, and data transfer.
    Identifies cost anomalies, trends, and optimization opportunities.
    """
    
    def __init__(self, connector: SnowflakeConnector):
        """Initialize cost analyzer with Snowflake connector."""
        self.connector = connector
        self.settings = get_settings()
        
    def analyze_warehouse_costs(self, days: int = 30) -> Dict[str, Any]:
        """
        Analyze warehouse costs and identify optimization opportunities.
        
        Args:
            days: Number of days to analyze
            
        Returns:
            Dictionary containing cost analysis results
        """
        logger.info(f"Analyzing warehouse costs for {days} days")
        
        # Get warehouse usage data
        usage_data = self.connector.get_warehouse_usage(days)
        
        if usage_data.empty:
            logger.warning("No warehouse usage data found")
            return {'total_cost': 0, 'analysis': {}, 'alerts': []}
        
        # Calculate total costs
        total_cost = usage_data['total_credits'].sum()
        
        # Analyze cost by warehouse
        warehouse_summary = usage_data.groupby('warehouse_name').agg({
            'total_credits': ['sum', 'mean', 'std'],
            'usage_count': 'sum',
            'total_runtime_minutes': 'sum'
        }).round(2)
        
        warehouse_summary.columns = [
            'total_credits', 'avg_daily_credits', 'credit_volatility',
            'total_usage_events', 'total_runtime_minutes'
        ]
        
        # Calculate efficiency metrics
        warehouse_summary['cost_per_usage'] = (
            warehouse_summary['total_credits'] / warehouse_summary['total_usage_events']
        ).round(4)
        
        warehouse_summary['cost_per_minute'] = (
            warehouse_summary['total_credits'] / warehouse_summary['total_runtime_minutes']
        ).round(4)
        
        # Identify cost anomalies
        alerts = self._detect_cost_anomalies(usage_data)
        
        # Identify underutilized warehouses
        underutilized_alerts = self._identify_underutilized_warehouses(warehouse_summary)
        alerts.extend(underutilized_alerts)
        
        # Calculate cost trends
        trends = self._calculate_cost_trends(usage_data)
        
        return {
            'total_cost': total_cost,
            'daily_average': total_cost / days,
            'warehouse_summary': warehouse_summary.to_dict('index'),
            'trends': trends,
            'alerts': [alert.__dict__ for alert in alerts],
            'top_cost_warehouses': self._get_top_cost_warehouses(warehouse_summary),
            'cost_efficiency_scores': self._calculate_efficiency_scores(warehouse_summary)
        }
    
    def analyze_storage_costs(self) -> Dict[str, Any]:
        """Analyze storage costs and identify optimization opportunities."""
        logger.info("Analyzing storage costs")
        
        # Get storage usage data
        storage_data = self.connector.get_storage_usage()
        
        if storage_data.empty:
            logger.warning("No storage usage data found")
            return {'total_storage_gb': 0, 'analysis': {}, 'alerts': []}
        
        # Calculate storage metrics
        storage_data['total_bytes'] = (
            storage_data['active_bytes'] + 
            storage_data['time_travel_bytes'] + 
            storage_data['failsafe_bytes']
        )
        storage_data['total_gb'] = storage_data['total_bytes'] / (1024**3)
        
        # Analyze by database
        db_summary = storage_data.groupby('database_name').agg({
            'total_gb': 'sum',
            'active_bytes': 'sum',
            'time_travel_bytes': 'sum',
            'failsafe_bytes': 'sum',
            'row_count': 'sum'
        }).round(2)
        
        # Identify large tables
        large_tables = storage_data.nlargest(20, 'total_gb')[
            ['database_name', 'schema_name', 'table_name', 'total_gb', 
             'row_count', 'table_created', 'table_last_altered']
        ]
        
        # Identify storage optimization opportunities
        alerts = self._identify_storage_optimization_opportunities(storage_data)
        
        total_storage_gb = storage_data['total_gb'].sum()
        
        return {
            'total_storage_gb': total_storage_gb,
            'database_summary': db_summary.to_dict('index'),
            'large_tables': large_tables.to_dict('records'),
            'alerts': [alert.__dict__ for alert in alerts],
            'storage_breakdown': self._get_storage_breakdown(storage_data),
            'unused_tables': self._identify_unused_tables(storage_data)
        }
    
    def analyze_overall_costs(self, days: int = 30) -> Dict[str, Any]:
        """Perform comprehensive cost analysis across all Snowflake services."""
        logger.info(f"Performing comprehensive cost analysis for {days} days")
        
        # Get comprehensive cost data
        cost_data = self.connector.get_cost_analysis_data(days)
        
        # Analyze each cost component
        warehouse_analysis = self.analyze_warehouse_costs(days)
        storage_analysis = self.analyze_storage_costs()
        
        # Analyze data transfer costs if available
        transfer_analysis = {}
        if 'transfer_costs' in cost_data and not cost_data['transfer_costs'].empty:
            transfer_analysis = self._analyze_transfer_costs(cost_data['transfer_costs'])
        
        # Calculate total estimated monthly cost
        estimated_monthly_cost = self._estimate_monthly_cost(
            warehouse_analysis.get('daily_average', 0),
            storage_analysis.get('total_storage_gb', 0),
            transfer_analysis.get('daily_transfer_gb', 0)
        )
        
        # Combine all alerts
        all_alerts = []
        all_alerts.extend(warehouse_analysis.get('alerts', []))
        all_alerts.extend(storage_analysis.get('alerts', []))
        all_alerts.extend(transfer_analysis.get('alerts', []))
        
        # Prioritize alerts by cost impact
        sorted_alerts = sorted(all_alerts, key=lambda x: x['cost_impact'], reverse=True)
        
        return {
            'estimated_monthly_cost': estimated_monthly_cost,
            'warehouse_analysis': warehouse_analysis,
            'storage_analysis': storage_analysis,
            'transfer_analysis': transfer_analysis,
            'priority_alerts': sorted_alerts[:10],  # Top 10 alerts
            'cost_breakdown': {
                'warehouse_percentage': self._calculate_cost_percentage(
                    warehouse_analysis.get('daily_average', 0) * 30, estimated_monthly_cost
                ),
                'storage_percentage': self._calculate_storage_cost_percentage(
                    storage_analysis.get('total_storage_gb', 0), estimated_monthly_cost
                )
            },
            'recommendations': self._generate_cost_recommendations(sorted_alerts)
        }
    
    def _detect_cost_anomalies(self, usage_data: pd.DataFrame) -> List[CostAlert]:
        """Detect cost anomalies using isolation forest and statistical methods."""
        alerts = []
        
        try:
            # Group by warehouse and date for anomaly detection
            daily_costs = usage_data.groupby(['warehouse_name', 'usage_date'])['total_credits'].sum().reset_index()
            
            for warehouse in daily_costs['warehouse_name'].unique():
                warehouse_data = daily_costs[daily_costs['warehouse_name'] == warehouse]
                
                if len(warehouse_data) < 7:  # Need at least a week of data
                    continue
                
                costs = warehouse_data['total_credits'].values.reshape(-1, 1)
                
                # Use Isolation Forest for anomaly detection
                iso_forest = IsolationForest(contamination=0.1, random_state=42)
                anomaly_labels = iso_forest.fit_predict(costs)
                
                # Also check for statistical outliers (z-score > 2.5)
                mean_cost = warehouse_data['total_credits'].mean()
                std_cost = warehouse_data['total_credits'].std()
                
                for idx, (_, row) in enumerate(warehouse_data.iterrows()):
                    cost = row['total_credits']
                    z_score = abs((cost - mean_cost) / std_cost) if std_cost > 0 else 0
                    
                    if anomaly_labels[idx] == -1 or z_score > 2.5:
                        severity = 'critical' if z_score > 3 else 'high' if z_score > 2.5 else 'medium'
                        
                        alerts.append(CostAlert(
                            alert_type='cost_anomaly',
                            severity=severity,
                            message=f"Unusual cost spike detected for warehouse {warehouse}",
                            details={
                                'date': row['usage_date'].isoformat(),
                                'cost': cost,
                                'average_cost': mean_cost,
                                'z_score': z_score
                            },
                            affected_resource=warehouse,
                            cost_impact=cost - mean_cost,
                            recommendation="Investigate queries executed on this date for potential optimization"
                        ))
                        
        except Exception as e:
            logger.error(f"Error detecting cost anomalies: {e}")
        
        return alerts
    
    def _identify_underutilized_warehouses(self, warehouse_summary: pd.DataFrame) -> List[CostAlert]:
        """Identify warehouses with low utilization."""
        alerts = []
        
        for warehouse, stats in warehouse_summary.iterrows():
            # Consider warehouse underutilized if cost per minute is very high
            if stats['cost_per_minute'] > 0.1:  # Arbitrary threshold
                alerts.append(CostAlert(
                    alert_type='underutilization',
                    severity='medium',
                    message=f"Warehouse {warehouse} appears underutilized",
                    details={
                        'cost_per_minute': stats['cost_per_minute'],
                        'total_runtime_minutes': stats['total_runtime_minutes'],
                        'total_cost': stats['total_credits']
                    },
                    affected_resource=warehouse,
                    cost_impact=stats['total_credits'] * 0.3,  # Estimated savings
                    recommendation="Consider downsizing warehouse or optimizing query patterns"
                ))
        
        return alerts
    
    def _calculate_cost_trends(self, usage_data: pd.DataFrame) -> List[CostTrend]:
        """Calculate cost trends over time."""
        trends = []
        
        try:
            # Daily cost trend
            daily_costs = usage_data.groupby('usage_date')['total_credits'].sum()
            
            if len(daily_costs) >= 7:
                # Calculate trend using linear regression
                x = np.arange(len(daily_costs))
                y = daily_costs.values
                
                slope, _ = np.polyfit(x, y, 1)
                
                trend_direction = 'increasing' if slope > 0.1 else 'decreasing' if slope < -0.1 else 'stable'
                trend_magnitude = abs(slope)
                
                # Project monthly cost
                current_daily_avg = daily_costs.tail(7).mean()
                projected_monthly = current_daily_avg * 30
                
                trends.append(CostTrend(
                    period='daily',
                    trend_direction=trend_direction,
                    trend_magnitude=trend_magnitude,
                    projected_monthly_cost=projected_monthly,
                    confidence_score=0.8 if len(daily_costs) >= 14 else 0.6
                ))
                
        except Exception as e:
            logger.error(f"Error calculating cost trends: {e}")
        
        return trends
    
    def _identify_storage_optimization_opportunities(self, storage_data: pd.DataFrame) -> List[CostAlert]:
        """Identify storage optimization opportunities."""
        alerts = []
        
        # Find tables with high time travel storage
        time_travel_threshold = storage_data['time_travel_bytes'].quantile(0.9)
        high_time_travel = storage_data[storage_data['time_travel_bytes'] > time_travel_threshold]
        
        for _, table in high_time_travel.iterrows():
            time_travel_gb = table['time_travel_bytes'] / (1024**3)
            
            alerts.append(CostAlert(
                alert_type='storage_optimization',
                severity='medium',
                message=f"High time travel storage for table {table['table_name']}",
                details={
                    'time_travel_gb': time_travel_gb,
                    'database': table['database_name'],
                    'schema': table['schema_name']
                },
                affected_resource=f"{table['database_name']}.{table['schema_name']}.{table['table_name']}",
                cost_impact=time_travel_gb * 23,  # Approximate monthly cost per GB
                recommendation="Consider reducing data retention period or implementing lifecycle policies"
            ))
        
        return alerts
    
    def _get_top_cost_warehouses(self, warehouse_summary: pd.DataFrame, top_n: int = 5) -> List[Dict]:
        """Get top cost warehouses."""
        return warehouse_summary.nlargest(top_n, 'total_credits').to_dict('index')
    
    def _calculate_efficiency_scores(self, warehouse_summary: pd.DataFrame) -> Dict[str, float]:
        """Calculate efficiency scores for warehouses."""
        scores = {}
        
        for warehouse, stats in warehouse_summary.iterrows():
            # Simple efficiency score based on cost per query and utilization
            cost_score = 1 / (1 + stats['cost_per_usage'])  # Lower cost per usage = higher score
            utilization_score = min(stats['total_runtime_minutes'] / (24 * 60), 1.0)  # Max 24 hours
            
            efficiency_score = (cost_score + utilization_score) / 2
            scores[warehouse] = round(efficiency_score, 3)
        
        return scores
    
    def _analyze_transfer_costs(self, transfer_data: pd.DataFrame) -> Dict[str, Any]:
        """Analyze data transfer costs."""
        if transfer_data.empty:
            return {}
        
        total_transfer_gb = transfer_data['total_transfer_gb'].sum()
        daily_avg = total_transfer_gb / len(transfer_data['transfer_date'].unique())
        
        # Identify high transfer routes
        route_summary = transfer_data.groupby(['source_region', 'target_region']).agg({
            'total_transfer_gb': 'sum',
            'bytes_transferred': 'count'
        }).round(2)
        
        return {
            'total_transfer_gb': total_transfer_gb,
            'daily_transfer_gb': daily_avg,
            'route_summary': route_summary.to_dict('index'),
            'alerts': []  # Add transfer-specific alerts if needed
        }
    
    def _estimate_monthly_cost(self, daily_warehouse_cost: float, storage_gb: float, daily_transfer_gb: float) -> float:
        """Estimate monthly Snowflake cost."""
        # Rough estimates based on Snowflake pricing
        monthly_warehouse = daily_warehouse_cost * 30  # Credits
        monthly_storage = storage_gb * 23  # $23 per TB per month
        monthly_transfer = daily_transfer_gb * 30 * 0.09  # $0.09 per GB
        
        return monthly_warehouse + monthly_storage + monthly_transfer
    
    def _calculate_cost_percentage(self, component_cost: float, total_cost: float) -> float:
        """Calculate percentage of total cost."""
        return (component_cost / total_cost * 100) if total_cost > 0 else 0.0
    
    def _calculate_storage_cost_percentage(self, storage_gb: float, total_cost: float) -> float:
        """Calculate storage cost percentage."""
        storage_cost = storage_gb * 23
        return (storage_cost / total_cost * 100) if total_cost > 0 else 0.0
    
    def _get_storage_breakdown(self, storage_data: pd.DataFrame) -> Dict[str, float]:
        """Get storage breakdown by type."""
        total_active = storage_data['active_bytes'].sum() / (1024**3)
        total_time_travel = storage_data['time_travel_bytes'].sum() / (1024**3)
        total_failsafe = storage_data['failsafe_bytes'].sum() / (1024**3)
        
        return {
            'active_gb': total_active,
            'time_travel_gb': total_time_travel,
            'failsafe_gb': total_failsafe
        }
    
    def _identify_unused_tables(self, storage_data: pd.DataFrame, unused_days: int = 90) -> List[Dict]:
        """Identify potentially unused tables."""
        cutoff_date = datetime.now() - timedelta(days=unused_days)
        
        unused_tables = storage_data[
            pd.to_datetime(storage_data['table_last_altered']) < cutoff_date
        ].nlargest(10, 'active_bytes')[
            ['database_name', 'schema_name', 'table_name', 'total_gb', 'table_last_altered']
        ]
        
        return unused_tables.to_dict('records')
    
    def _generate_cost_recommendations(self, alerts: List[Dict]) -> List[str]:
        """Generate high-level cost optimization recommendations."""
        recommendations = []
        
        # Count alert types
        alert_counts = {}
        for alert in alerts:
            alert_type = alert.get('alert_type', 'unknown')
            alert_counts[alert_type] = alert_counts.get(alert_type, 0) + 1
        
        if alert_counts.get('underutilization', 0) > 0:
            recommendations.append("Consider rightsizing underutilized warehouses to reduce compute costs")
        
        if alert_counts.get('storage_optimization', 0) > 0:
            recommendations.append("Implement data lifecycle policies to optimize storage costs")
        
        if alert_counts.get('cost_anomaly', 0) > 0:
            recommendations.append("Review and optimize queries causing unusual cost spikes")
        
        if not recommendations:
            recommendations.append("Overall cost patterns appear normal - continue monitoring")
        
        return recommendations 