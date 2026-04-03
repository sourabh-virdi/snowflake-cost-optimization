"""
Data governance dashboard page.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import sys
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent / "src"))

from snowflake_optimizer.analyzers.access_analyzer import AccessAnalyzer


def show_data_governance(connector, analysis_days: int = 30):
    """Display the data governance dashboard."""
    st.title("Data Governance Dashboard")
    
    # Create access analyzer
    access_analyzer = AccessAnalyzer(connector)
    
    # Load data with caching
    cache_key = f"governance_data_{analysis_days}"
    if cache_key not in st.session_state:
        with st.spinner("Loading data governance analysis..."):
            st.session_state[cache_key] = access_analyzer.analyze_access_patterns(analysis_days)
    
    governance_data = st.session_state[cache_key]
    
    # Display overview metrics
    display_governance_overview(governance_data)
    
    # Display detailed analysis
    col1, col2 = st.columns(2)
    
    with col1:
        display_access_insights(governance_data.get('governance_insights', {}))
    
    with col2:
        display_user_patterns(governance_data.get('user_patterns', []))
    
    # Display recommendations
    display_governance_recommendations(governance_data.get('recommendations', []))


def display_governance_overview(governance_data: dict):
    """Display governance overview metrics."""
    st.markdown("### Data Governance Overview")
    
    summary = governance_data.get('summary', {})
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        unique_users = summary.get('unique_users', 0)
        st.metric("Active Users", unique_users)
    
    with col2:
        unique_objects = summary.get('unique_objects', 0)
        st.metric("Accessed Objects", unique_objects)
    
    with col3:
        total_access_events = summary.get('total_access_events', 0)
        st.metric("Access Events", f"{total_access_events:,}")
    
    with col4:
        analysis_period = summary.get('analysis_period_days', 0)
        st.metric("Analysis Period", f"{analysis_period} days")


def display_access_insights(insights: dict):
    """Display access pattern insights."""
    st.markdown("### Access Pattern Insights")
    
    if not insights:
        st.info("No access insights data available")
        return
    
    # Most active users
    active_users = insights.get('most_active_users', {})
    if active_users:
        st.markdown("#### Most Active Users")
        users_df = pd.DataFrame(list(active_users.items()), columns=['User', 'Access Count'])
        users_df = users_df.sort_values('Access Count', ascending=False).head(10)
        st.dataframe(users_df, use_container_width=True)
    
    # Privilege distribution
    privilege_dist = insights.get('privilege_distribution', {})
    if privilege_dist:
        st.markdown("#### Privilege Distribution")
        priv_df = pd.DataFrame(list(privilege_dist.items()), columns=['Privilege', 'Count'])
        priv_df = priv_df.sort_values('Count', ascending=False)
        
        fig = px.pie(priv_df, values='Count', names='Privilege', 
                    title="Distribution of Privileges")
        st.plotly_chart(fig, use_container_width=True)


def display_user_patterns(user_patterns: list):
    """Display user access patterns."""
    st.markdown("### User Access Patterns")
    
    if not user_patterns:
        st.info("No user pattern data available")
        return
    
    # Convert patterns to DataFrame for display
    pattern_data = []
    for pattern in user_patterns[:20]:  # Show top 20
        if hasattr(pattern, '__dict__'):
            pattern_data.append({
                'User': pattern.user_name,
                'Object': pattern.object_name,
                'Access Count': pattern.access_count,
                'Last Access': pattern.last_access,
                'Risk Level': pattern.risk_level
            })
    
    if pattern_data:
        df = pd.DataFrame(pattern_data)
        st.dataframe(df, use_container_width=True)


def display_governance_recommendations(recommendations: list):
    """Display data governance recommendations."""
    if not recommendations:
        return
    
    st.markdown("### Data Governance Recommendations")
    
    for i, rec in enumerate(recommendations, 1):
        st.info(f"**{i}.** {rec}") 