"""
Cost analysis dashboard page.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent / "src"))

from snowflake_optimizer.analyzers.cost_analyzer import CostAnalyzer


def show_cost_dashboard(connector, analysis_days: int = 30):
    """Display the cost analysis dashboard."""
    st.title("Cost Analysis Dashboard")
    
    # Create cost analyzer
    cost_analyzer = CostAnalyzer(connector)
    
    # Load data with caching
    cache_key = f"cost_data_{analysis_days}"
    if cache_key not in st.session_state:
        with st.spinner("Loading cost analysis data..."):
            st.session_state[cache_key] = cost_analyzer.analyze_overall_costs(analysis_days)
    
    cost_data = st.session_state[cache_key]
    
    # Display overview metrics
    display_cost_overview(cost_data)
    
    # Display detailed analysis
    col1, col2 = st.columns(2)
    
    with col1:
        display_warehouse_costs(cost_data.get('warehouse_analysis', {}))
    
    with col2:
        display_storage_costs(cost_data.get('storage_analysis', {}))
    
    # Display alerts and recommendations
    display_cost_alerts(cost_data.get('priority_alerts', []))
    display_cost_recommendations(cost_data.get('recommendations', []))


def display_cost_overview(cost_data: dict):
    """Display cost overview metrics."""
    st.markdown("### Cost Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        monthly_cost = cost_data.get('estimated_monthly_cost', 0)
        st.metric("Estimated Monthly Cost", f"${monthly_cost:,.2f}")
    
    with col2:
        warehouse_cost = cost_data.get('warehouse_analysis', {}).get('total_cost', 0)
        st.metric("Warehouse Credits", f"{warehouse_cost:,.1f}")
    
    with col3:
        storage_gb = cost_data.get('storage_analysis', {}).get('total_storage_gb', 0)
        st.metric("Storage (GB)", f"{storage_gb:,.1f}")
    
    with col4:
        breakdown = cost_data.get('cost_breakdown', {})
        warehouse_pct = breakdown.get('warehouse_percentage', 0)
        st.metric("Warehouse %", f"{warehouse_pct:.1f}%")


def display_warehouse_costs(warehouse_data: dict):
    """Display warehouse cost analysis."""
    st.markdown("### Warehouse Costs")
    
    warehouse_summary = warehouse_data.get('warehouse_summary', {})
    
    if warehouse_summary:
        # Convert to DataFrame for visualization
        df = pd.DataFrame.from_dict(warehouse_summary, orient='index').reset_index()
        df.rename(columns={'index': 'warehouse'}, inplace=True)
        
        # Top warehouses by cost
        df_sorted = df.nlargest(10, 'total_credits')
        
        fig = px.bar(df_sorted, x='warehouse', y='total_credits', 
                    title="Top Warehouses by Credit Usage")
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)
        
        # Cost efficiency chart
        if 'cost_per_query' in df.columns:
            fig2 = px.scatter(df, x='total_queries', y='cost_per_query', 
                            hover_name='warehouse', 
                            title="Cost Efficiency: Cost per Query vs Query Volume",
                            labels={'total_queries': 'Total Queries', 
                                   'cost_per_query': 'Cost per Query'})
            st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No warehouse cost data available")


def display_storage_costs(storage_data: dict):
    """Display storage cost analysis."""
    st.markdown("### Storage Costs")
    
    storage_breakdown = storage_data.get('storage_breakdown', {})
    
    if storage_breakdown:
        # Create storage breakdown pie chart
        breakdown_df = pd.DataFrame([
            {'Type': 'Active', 'Size_GB': storage_breakdown.get('active_gb', 0)},
            {'Type': 'Time Travel', 'Size_GB': storage_breakdown.get('time_travel_gb', 0)},
            {'Type': 'Failsafe', 'Size_GB': storage_breakdown.get('failsafe_gb', 0)}
        ])
        
        fig = px.pie(breakdown_df, values='Size_GB', names='Type', 
                    title="Storage Breakdown by Type")
        st.plotly_chart(fig, use_container_width=True)
        
        # Large tables
        large_tables = storage_data.get('large_tables', [])
        if large_tables:
            st.markdown("#### Largest Tables")
            df_tables = pd.DataFrame(large_tables)
            df_tables = df_tables.nlargest(10, 'total_gb')
            
            # Display as table
            st.dataframe(
                df_tables[['database_name', 'schema_name', 'table_name', 'total_gb', 'row_count']],
                use_container_width=True
            )
    else:
        st.info("No storage cost data available")


def display_cost_alerts(alerts: list):
    """Display cost alerts."""
    if not alerts:
        return
    
    st.markdown("### Cost Alerts")
    
    # Group alerts by severity
    critical_alerts = [a for a in alerts if a.get('severity') == 'critical']
    high_alerts = [a for a in alerts if a.get('severity') == 'high']
    medium_alerts = [a for a in alerts if a.get('severity') == 'medium']
    
    # Display critical alerts
    if critical_alerts:
        st.markdown("#### Critical Alerts")
        for alert in critical_alerts[:3]:
            st.error(f"**{alert.get('message', 'Unknown alert')}** - "
                    f"Impact: ${alert.get('cost_impact', 0):,.2f}")
    
    # Display high priority alerts
    if high_alerts:
        st.markdown("#### High Priority Alerts")
        for alert in high_alerts[:3]:
            st.warning(f"**{alert.get('message', 'Unknown alert')}** - "
                      f"Impact: ${alert.get('cost_impact', 0):,.2f}")
    
    # Display medium priority alerts in expander
    if medium_alerts:
        with st.expander(f"Medium Priority Alerts ({len(medium_alerts)})"):
            for alert in medium_alerts[:5]:
                st.info(f"**{alert.get('message', 'Unknown alert')}** - "
                       f"Impact: ${alert.get('cost_impact', 0):,.2f}")


def display_cost_recommendations(recommendations: list):
    """Display cost optimization recommendations."""
    if not recommendations:
        return
    
    st.markdown("### Cost Optimization Recommendations")
    
    for i, rec in enumerate(recommendations[:5], 1):
        st.success(f"**{i}.** {rec}")
    
    if len(recommendations) > 5:
        with st.expander(f"View all {len(recommendations)} recommendations"):
            for i, rec in enumerate(recommendations, 1):
                st.write(f"{i}. {rec}")


def create_cost_trend_chart(cost_data: dict):
    """Create cost trend visualization."""
    trends = cost_data.get('warehouse_analysis', {}).get('trends', [])
    
    if not trends:
        return None
    
    # Create a simple trend chart (placeholder for now)
    fig = go.Figure()
    
    # Add trend data if available
    for trend in trends:
        if trend.get('period') == 'daily':
            # Placeholder trend line
            fig.add_trace(go.Scatter(
                x=['Day 1', 'Day 7', 'Day 14', 'Day 21', 'Day 30'],
                y=[100, 110, 120, 115, 125],  # Placeholder data
                mode='lines+markers',
                name='Cost Trend',
                line=dict(color='blue')
            ))
    
    fig.update_layout(
        title="Cost Trend Analysis",
        xaxis_title="Time Period",
        yaxis_title="Credits",
        showlegend=True
    )
    
    return fig 