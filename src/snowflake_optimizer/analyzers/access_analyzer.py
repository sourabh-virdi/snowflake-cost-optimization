"""
Access pattern and data governance analysis module.
"""

import pandas as pd
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from loguru import logger
from ..connectors.snowflake_connector import SnowflakeConnector
from ..config.settings import get_settings


@dataclass
class AccessPattern:
    """Access pattern data structure."""
    user_name: str
    object_name: str
    access_count: int
    last_access: str
    risk_level: str  # 'low', 'medium', 'high'


class AccessAnalyzer:
    """
    Analyzes data access patterns and provides governance insights.
    """
    
    def __init__(self, connector: SnowflakeConnector):
        """Initialize access analyzer with Snowflake connector."""
        self.connector = connector
        self.settings = get_settings()
        
    def analyze_access_patterns(self, days: int = 30) -> Dict[str, Any]:
        """
        Analyze user access patterns and data governance metrics.
        
        Args:
            days: Number of days to analyze
            
        Returns:
            Dictionary containing access analysis results
        """
        logger.info(f"Analyzing access patterns for {days} days")
        
        try:
            # Get access data
            access_data = self.connector.get_user_access_patterns(days)
            
            if access_data.empty:
                logger.warning("No access data available for analysis")
                return {'patterns': [], 'insights': {}, 'recommendations': []}
            
            # Analyze user access patterns
            user_patterns = self._analyze_user_access(access_data)
            
            # Identify governance insights
            governance_insights = self._identify_governance_insights(access_data)
            
            # Generate recommendations
            recommendations = self._generate_governance_recommendations(user_patterns, governance_insights)
            
            return {
                'user_patterns': user_patterns,
                'governance_insights': governance_insights,
                'recommendations': recommendations,
                'summary': self._generate_access_summary(access_data)
            }
            
        except Exception as e:
            logger.error(f"Error analyzing access patterns: {e}")
            return {'patterns': [], 'insights': {}, 'recommendations': []}
    
    def _analyze_user_access(self, access_data: pd.DataFrame) -> List[AccessPattern]:
        """Analyze user access patterns."""
        patterns = []
        
        try:
            # Group by user and object
            user_object_access = access_data.groupby(['user_name', 'object_name']).agg({
                'access_count': 'sum',
                'created_on': 'max'
            }).reset_index()
            
            for _, row in user_object_access.iterrows():
                risk_level = self._assess_access_risk(row['access_count'], row['created_on'])
                
                patterns.append(AccessPattern(
                    user_name=row['user_name'],
                    object_name=row['object_name'],
                    access_count=row['access_count'],
                    last_access=row['created_on'].isoformat() if pd.notna(row['created_on']) else 'Unknown',
                    risk_level=risk_level
                ))
                
        except Exception as e:
            logger.error(f"Error analyzing user access: {e}")
        
        return patterns
    
    def _identify_governance_insights(self, access_data: pd.DataFrame) -> Dict[str, Any]:
        """Identify data governance insights."""
        insights = {}
        
        try:
            # Most active users
            active_users = access_data.groupby('user_name')['access_count'].sum().nlargest(10)
            insights['most_active_users'] = active_users.to_dict()
            
            # Most accessed objects
            accessed_objects = access_data.groupby('object_name')['access_count'].sum().nlargest(10)
            insights['most_accessed_objects'] = accessed_objects.to_dict()
            
            # Privilege distribution
            privilege_dist = access_data['privilege'].value_counts().to_dict()
            insights['privilege_distribution'] = privilege_dist
            
            # Object type distribution
            object_type_dist = access_data['object_type'].value_counts().to_dict()
            insights['object_type_distribution'] = object_type_dist
            
        except Exception as e:
            logger.error(f"Error identifying governance insights: {e}")
        
        return insights
    
    def _assess_access_risk(self, access_count: int, last_access: pd.Timestamp) -> str:
        """Assess risk level for access pattern."""
        # Simple risk assessment logic
        if access_count > 100:
            return 'high'  # Very active access
        elif access_count > 10:
            return 'medium'  # Moderate access
        else:
            return 'low'  # Low access
    
    def _generate_governance_recommendations(self, patterns: List[AccessPattern], 
                                          insights: Dict[str, Any]) -> List[str]:
        """Generate data governance recommendations."""
        recommendations = []
        
        # Check for high-risk access patterns
        high_risk_patterns = [p for p in patterns if p.risk_level == 'high']
        if len(high_risk_patterns) > 10:
            recommendations.append("Review high-activity access patterns for potential optimization")
        
        # Check for privilege distribution
        privilege_dist = insights.get('privilege_distribution', {})
        if 'OWNERSHIP' in privilege_dist and privilege_dist['OWNERSHIP'] > 100:
            recommendations.append("Review ownership privileges - consider principle of least privilege")
        
        # Check for object access patterns
        accessed_objects = insights.get('most_accessed_objects', {})
        if len(accessed_objects) > 20:
            recommendations.append("Consider implementing data access governance policies")
        
        if not recommendations:
            recommendations.append("Access patterns appear normal - continue monitoring")
        
        return recommendations
    
    def _generate_access_summary(self, access_data: pd.DataFrame) -> Dict[str, Any]:
        """Generate access analysis summary."""
        if access_data.empty:
            return {}
        
        return {
            'total_access_records': len(access_data),
            'unique_users': access_data['user_name'].nunique(),
            'unique_objects': access_data['object_name'].nunique(),
            'total_access_events': access_data['access_count'].sum(),
            'analysis_period_days': (access_data['created_on'].max() - access_data['created_on'].min()).days
        } 