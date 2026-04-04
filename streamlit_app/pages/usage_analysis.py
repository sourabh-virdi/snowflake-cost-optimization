"""
Usage analysis dashboard page.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent / "src"))

from snowflake_optimizer.analyzers.usage_analyzer import UsageAnalyzer


def show_usage_analysis(connector, analysis_days: int = 30):
    """Display the usage analysis dashboard."""
    st.title("Usage Analysis Dashboard")
    
    # Create usage analyzer
    usage_analyzer = UsageAnalyzer(connector)
    
    # Load data with caching
    cache_key = f"usage_data_{analysis_days}"
    if cache_key not in st.session_state:
        with st.spinner("Loading usage analysis data..."):
            st.session_state[cache_key] = usage_analyzer.analyze_warehouse_usage_patterns(analysis_days)
    
    usage_data = st.session_state[cache_key]
    
    # Display overview metrics
    display_usage_overview(usage_data)
    
    # Display detailed analysis
    col1, col2 = st.columns(2)
    
    with col1:
        display_temporal_patterns(usage_data.get('temporal_patterns', []))
    
    with col2:
        display_utilization_metrics(usage_data.get('utilization_metrics', {}))
    
    # Display recommendations
    display_usage_recommendations(usage_data.get('recommendations', []))


def display_usage_overview(usage_data: dict):
    """Display usage overview metrics."""
    st.markdown("### Usage Overview")
    
    summary = usage_data.get('summary', {})
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_warehouses = summary.get('total_warehouses', 0)
        st.metric("Active Warehouses", total_warehouses)
    
    with col2:
        total_credits = summary.get('total_credits_analyzed', 0)
        st.metric("Total Credits", f"{total_credits:,.1f}")
    
    with col3:
        avg_daily_credits = summary.get('avg_daily_credits', 0)
        st.metric("Avg Daily Credits", f"{avg_daily_credits:,.1f}")
    
    with col4:
        avg_utilization = summary.get('avg_utilization_score', 0)
        st.metric("Utilization Score", f"{avg_utilization:.2f}")


def display_temporal_patterns(patterns: list):
    """Display temporal usage patterns."""
    st.markdown("### Temporal Patterns")
    
    if not patterns:
        st.info("No temporal patterns data available")
        return
    
    pattern = patterns[0]  # Get the first (main) pattern
    
    # Display peak hours
    peak_hours = pattern.peak_hours if hasattr(pattern, 'peak_hours') else []
    if peak_hours:
        st.markdown("#### Peak Hours")
        peak_hours_str = ", ".join([f"{hour}:00" for hour in peak_hours])
        st.write(f"Primary peak hours: {peak_hours_str}")
    
    # Display peak days
    peak_days = pattern.peak_days if hasattr(pattern, 'peak_days') else []
    if peak_days:
        st.markdown("#### Peak Days")
        peak_days_str = ", ".join(peak_days)
        st.write(f"Primary peak days: {peak_days_str}")
    
    # Display utilization score
    utilization_score = pattern.utilization_score if hasattr(pattern, 'utilization_score') else 0
    st.markdown("#### Utilization Pattern Score")
    st.write(f"Score: {utilization_score:.2f}")
    
    # Display recommendations
    recommendations = pattern.recommendations if hasattr(pattern, 'recommendations') else []
    if recommendations:
        st.markdown("#### Pattern-Based Recommendations")
        for rec in recommendations:
            st.write(f"• {rec}")


def display_utilization_metrics(utilization_metrics: dict):
    """Display warehouse utilization metrics."""
    st.markdown("### Warehouse Utilization")
    
    if not utilization_metrics:
        st.info("No utilization data available")
        return
    
    # Convert to DataFrame for display
    util_data = []
    for warehouse, metrics in utilization_metrics.items():
        if hasattr(metrics, '__dict__'):
            util_data.append({
                'Warehouse': warehouse,
                'Avg Utilization': f"{metrics.avg_utilization:.1%}",
                'Peak Utilization': f"{metrics.peak_utilization:.1%}",
                'Idle Time %': f"{metrics.idle_time_percentage:.1f}%",
                'Total Queries': metrics.total_queries,
                'Efficiency Score': f"{metrics.cost_efficiency_score:.3f}"
            })
    
    if util_data:
        df = pd.DataFrame(util_data)
        st.dataframe(df, use_container_width=True)
        
        # Create utilization chart
        if len(util_data) > 1:
            chart_data = pd.DataFrame([
                {
                    'Warehouse': metrics.warehouse_name,
                    'Avg Utilization': metrics.avg_utilization,
                    'Efficiency Score': metrics.cost_efficiency_score
                }
                for metrics in utilization_metrics.values()
                if hasattr(metrics, '__dict__')
            ])
            
            if not chart_data.empty:
                fig = px.scatter(
                    chart_data, 
                    x='Avg Utilization', 
                    y='Efficiency Score',
                    hover_name='Warehouse',
                    title="Warehouse Utilization vs Efficiency"
                )
                st.plotly_chart(fig, use_container_width=True)


def display_usage_recommendations(recommendations: list):
    """Display usage optimization recommendations."""
    if not recommendations:
        return
    
    st.markdown("### Usage Optimization Recommendations")
    
    for i, rec in enumerate(recommendations, 1):
        st.success(f"**{i}.** {rec}")
    
    if len(recommendations) > 5:
        with st.expander(f"View all {len(recommendations)} recommendations"):
            for i, rec in enumerate(recommendations, 1):
                st.write(f"{i}. {rec}")


def create_usage_trend_chart(usage_data: dict):
    """Create usage trend visualization."""
    # This would create a trend chart based on usage data
    # Implementation depends on the specific data structure
    pass 