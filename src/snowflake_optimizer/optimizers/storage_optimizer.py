"""
Storage optimization recommendations.
"""

import pandas as pd
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime, timedelta
from loguru import logger
from ..connectors.snowflake_connector import SnowflakeConnector
from ..config.settings import get_settings


@dataclass
class StorageRecommendation:
    """Storage optimization recommendation."""
    object_name: str
    object_type: str  # 'table', 'view', 'materialized_view'
    recommendation_type: str  # 'cleanup', 'compress', 'partition', 'lifecycle'
    current_storage: Dict[str, Any]
    recommended_changes: Dict[str, Any]
    estimated_savings: float  # Storage savings in GB or cost savings
    confidence_score: float  # 0-1 confidence in recommendation
    implementation_effort: str  # 'low', 'medium', 'high'
    description: str
    risk_level: str  # 'low', 'medium', 'high'


class StorageOptimizer:
    """
    Provides storage optimization recommendations based on storage usage analysis.
    """
    
    def __init__(self, connector: SnowflakeConnector):
        """Initialize storage optimizer."""
        self.connector = connector
        self.settings = get_settings()
    
    def analyze_storage_optimization_opportunities(self) -> List[StorageRecommendation]:
        """
        Analyze storage usage and provide optimization recommendations.
        
        Returns:
            List of storage optimization recommendations
        """
        logger.info("Analyzing storage optimization opportunities")
        
        recommendations = []
        
        try:
            # Get storage usage data
            storage_data = self.connector.get_storage_usage()
            
            if storage_data.empty:
                logger.warning("No storage data available for optimization analysis")
                return recommendations
            
            # Analyze different storage optimization opportunities
            recommendations.extend(self._identify_large_tables(storage_data))
            recommendations.extend(self._identify_time_travel_waste(storage_data))
            recommendations.extend(self._identify_unused_tables(storage_data))
            recommendations.extend(self._identify_compression_opportunities(storage_data))
            
            # Sort recommendations by potential savings
            recommendations.sort(key=lambda x: x.estimated_savings, reverse=True)
            
        except Exception as e:
            logger.error(f"Error analyzing storage optimization: {e}")
        
        return recommendations
    
    def _identify_large_tables(self, storage_data: pd.DataFrame) -> List[StorageRecommendation]:
        """Identify large tables that may benefit from optimization."""
        recommendations = []
        
        try:
            # Find tables larger than 1GB
            large_tables = storage_data[storage_data['active_bytes'] > 1024**3].copy()
            large_tables['total_gb'] = large_tables['active_bytes'] / (1024**3)
            
            # Sort by size (largest first)
            large_tables = large_tables.sort_values('active_bytes', ascending=False).head(10)
            
            for _, table in large_tables.iterrows():
                size_gb = table['total_gb']
                
                recommendations.append(StorageRecommendation(
                    object_name=f"{table['database_name']}.{table['schema_name']}.{table['table_name']}",
                    object_type='table',
                    recommendation_type='partition',
                    current_storage={
                        'size_gb': size_gb,
                        'row_count': table.get('row_count', 0)
                    },
                    recommended_changes={
                        'suggestion': 'Consider partitioning or clustering',
                        'expected_benefit': 'Improved query performance and storage efficiency'
                    },
                    estimated_savings=size_gb * 0.1,  # Estimate 10% storage optimization
                    confidence_score=0.6,
                    implementation_effort='medium',
                    description=f"Large table ({size_gb:.1f}GB) - consider partitioning strategy",
                    risk_level='low'
                ))
                
        except Exception as e:
            logger.error(f"Error identifying large tables: {e}")
        
        return recommendations
    
    def _identify_time_travel_waste(self, storage_data: pd.DataFrame) -> List[StorageRecommendation]:
        """Identify tables with excessive time travel storage."""
        recommendations = []
        
        try:
            # Find tables with significant time travel storage
            time_travel_threshold = 500 * 1024**2  # 500MB
            high_time_travel = storage_data[
                storage_data['time_travel_bytes'] > time_travel_threshold
            ].copy()
            
            for _, table in high_time_travel.iterrows():
                time_travel_gb = table['time_travel_bytes'] / (1024**3)
                
                recommendations.append(StorageRecommendation(
                    object_name=f"{table['database_name']}.{table['schema_name']}.{table['table_name']}",
                    object_type='table',
                    recommendation_type='lifecycle',
                    current_storage={
                        'time_travel_gb': time_travel_gb,
                        'retention_days': 'unknown'
                    },
                    recommended_changes={
                        'suggestion': 'Review and optimize data retention period',
                        'recommended_retention': '1-7 days for non-critical data'
                    },
                    estimated_savings=time_travel_gb * 0.7,  # Estimate 70% reduction
                    confidence_score=0.8,
                    implementation_effort='low',
                    description=f"High time travel storage ({time_travel_gb:.1f}GB) - review retention policy",
                    risk_level='low'
                ))
                
        except Exception as e:
            logger.error(f"Error identifying time travel waste: {e}")
        
        return recommendations
    
    def _identify_unused_tables(self, storage_data: pd.DataFrame) -> List[StorageRecommendation]:
        """Identify potentially unused tables."""
        recommendations = []
        
        try:
            # Find tables that haven't been modified recently
            cutoff_date = datetime.now() - timedelta(days=self.settings.optimization.unused_table_days)
            
            # Filter tables by last altered date
            if 'table_last_altered' in storage_data.columns:
                old_tables = storage_data[
                    pd.to_datetime(storage_data['table_last_altered']) < cutoff_date
                ].copy()
                
                # Focus on tables with significant storage
                old_tables = old_tables[old_tables['active_bytes'] > 100 * 1024**2]  # > 100MB
                
                for _, table in old_tables.head(5).iterrows():  # Top 5 candidates
                    size_gb = table['active_bytes'] / (1024**3)
                    
                    recommendations.append(StorageRecommendation(
                        object_name=f"{table['database_name']}.{table['schema_name']}.{table['table_name']}",
                        object_type='table',
                        recommendation_type='cleanup',
                        current_storage={
                            'size_gb': size_gb,
                            'last_altered': table.get('table_last_altered', 'unknown')
                        },
                        recommended_changes={
                            'suggestion': 'Review if table is still needed',
                            'action': 'Archive or drop if unused'
                        },
                        estimated_savings=size_gb,  # Full storage savings if dropped
                        confidence_score=0.5,  # Lower confidence - needs manual review
                        implementation_effort='low',
                        description=f"Potentially unused table ({size_gb:.1f}GB) - last modified {table.get('table_last_altered', 'unknown')}",
                        risk_level='high'  # High risk because dropping data is irreversible
                    ))
                    
        except Exception as e:
            logger.error(f"Error identifying unused tables: {e}")
        
        return recommendations
    
    def _identify_compression_opportunities(self, storage_data: pd.DataFrame) -> List[StorageRecommendation]:
        """Identify tables that may benefit from better compression."""
        recommendations = []
        
        try:
            # Simple heuristic: tables with high row count but relatively small storage
            # might already be well compressed, while tables with low row count but
            # large storage might benefit from compression review
            
            storage_data = storage_data.copy()
            storage_data['bytes_per_row'] = storage_data['active_bytes'] / storage_data['row_count'].replace(0, 1)
            
            # Find tables with high bytes per row (potential compression candidates)
            compression_candidates = storage_data[
                (storage_data['bytes_per_row'] > 1000) &  # > 1KB per row
                (storage_data['active_bytes'] > 100 * 1024**2)  # > 100MB total
            ].head(5)
            
            for _, table in compression_candidates.iterrows():
                size_gb = table['active_bytes'] / (1024**3)
                bytes_per_row = table['bytes_per_row']
                
                recommendations.append(StorageRecommendation(
                    object_name=f"{table['database_name']}.{table['schema_name']}.{table['table_name']}",
                    object_type='table',
                    recommendation_type='compress',
                    current_storage={
                        'size_gb': size_gb,
                        'bytes_per_row': bytes_per_row,
                        'row_count': table['row_count']
                    },
                    recommended_changes={
                        'suggestion': 'Review data types and compression options',
                        'focus_areas': ['varchar sizing', 'data type optimization', 'column ordering']
                    },
                    estimated_savings=size_gb * 0.2,  # Estimate 20% compression improvement
                    confidence_score=0.4,  # Lower confidence - depends on data characteristics
                    implementation_effort='medium',
                    description=f"Large bytes per row ({bytes_per_row:.0f} bytes) - review compression",
                    risk_level='low'
                ))
                
        except Exception as e:
            logger.error(f"Error identifying compression opportunities: {e}")
        
        return recommendations
    
    def get_optimization_summary(self, recommendations: List[StorageRecommendation]) -> Dict[str, Any]:
        """Generate a summary of storage optimization opportunities."""
        if not recommendations:
            return {'total_savings_gb': 0, 'recommendation_count': 0, 'summary': {}}
        
        total_savings = sum(rec.estimated_savings for rec in recommendations)
        
        recommendation_types = {}
        risk_levels = {}
        
        for rec in recommendations:
            rec_type = rec.recommendation_type
            risk_level = rec.risk_level
            
            recommendation_types[rec_type] = recommendation_types.get(rec_type, 0) + 1
            risk_levels[risk_level] = risk_levels.get(risk_level, 0) + 1
        
        high_impact_recs = [rec for rec in recommendations if rec.estimated_savings > 1.0]  # > 1GB savings
        low_risk_recs = [rec for rec in recommendations if rec.risk_level == 'low']
        
        return {
            'total_potential_savings_gb': total_savings,
            'recommendation_count': len(recommendations),
            'recommendation_types': recommendation_types,
            'risk_distribution': risk_levels,
            'high_impact_count': len(high_impact_recs),
            'low_risk_count': len(low_risk_recs),
            'top_opportunities': recommendations[:5]
        } 