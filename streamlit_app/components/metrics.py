"""
Metrics components for Streamlit dashboard.
"""

import streamlit as st
import pandas as pd
from typing import Dict, Any, List, Optional


def display_key_metrics(data: Dict[str, Any]):
    """Display key metrics in a structured layout."""
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        cost_data = data.get('cost_analysis', {})
        monthly_cost = cost_data.get('estimated_monthly_cost', 0)
        st.metric(
            label="Est. Monthly Cost",
            value=f"${monthly_cost:,.2f}",
            delta=None
        )
    
    with col2:
        warehouse_cost = cost_data.get('warehouse_analysis', {}).get('total_cost', 0)
        st.metric(
            label="Total Credits",
            value=f"{warehouse_cost:,.1f}",
            delta=None
        )
    
    with col3:
        storage_gb = cost_data.get('storage_analysis', {}).get('total_storage_gb', 0)
        st.metric(
            label="Storage (GB)",
            value=f"{storage_gb:,.1f}",
            delta=None
        )
    
    with col4:
        usage_data = data.get('usage_analysis', {})
        total_warehouses = usage_data.get('summary', {}).get('total_warehouses', 0)
        st.metric(
            label="Active Warehouses",
            value=total_warehouses,
            delta=None
        )


def display_efficiency_metrics(utilization_data: Dict[str, Any]):
    """Display warehouse efficiency metrics."""
    if not utilization_data:
        st.info("No utilization data available")
        return
    
    st.markdown("### Efficiency Metrics")
    
    # Convert utilization data to DataFrame
    util_list = []
    for warehouse, metrics in utilization_data.items():
        if hasattr(metrics, '__dict__'):
            util_list.append({
                'Warehouse': warehouse,
                'Avg Utilization': f"{metrics.avg_utilization:.1%}",
                'Idle Time %': f"{metrics.idle_time_percentage:.1f}%",
                'Total Queries': metrics.total_queries,
                'Efficiency Score': f"{metrics.cost_efficiency_score:.3f}"
            })
    
    if util_list:
        df = pd.DataFrame(util_list)
        st.dataframe(df, use_container_width=True)


def display_alert_summary(alerts: List[Dict[str, Any]]):
    """Display a summary of alerts by severity."""
    if not alerts:
        st.success("No alerts - everything looks good!")
        return
    
    # Count alerts by severity
    severity_counts = {}
    total_impact = 0
    
    for alert in alerts:
        severity = alert.get('severity', 'unknown')
        severity_counts[severity] = severity_counts.get(severity, 0) + 1
        total_impact += alert.get('cost_impact', 0)
    
    st.markdown("### Alert Summary")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        critical_count = severity_counts.get('critical', 0)
        st.metric(
            label="Critical",
            value=critical_count,
            delta=None
        )
    
    with col2:
        high_count = severity_counts.get('high', 0)
        st.metric(
            label="High",
            value=high_count,
            delta=None
        )
    
    with col3:
        medium_count = severity_counts.get('medium', 0)
        st.metric(
            label="Medium",
            value=medium_count,
            delta=None
        )
    
    with col4:
        st.metric(
            label="Total Impact",
            value=f"${total_impact:,.2f}",
            delta=None
        )


def display_cost_breakdown_metrics(cost_breakdown: Dict[str, float]):
    """Display cost breakdown metrics."""
    if not cost_breakdown:
        return
    
    st.markdown("### Cost Breakdown")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        warehouse_pct = cost_breakdown.get('warehouse_percentage', 0)
        st.metric(
            label="Warehouse",
            value=f"{warehouse_pct:.1f}%",
            delta=None
        )
    
    with col2:
        storage_pct = cost_breakdown.get('storage_percentage', 0)
        st.metric(
            label="Storage",
            value=f"{storage_pct:.1f}%",
            delta=None
        )
    
    with col3:
        other_pct = 100 - warehouse_pct - storage_pct
        st.metric(
            label="Other",
            value=f"{other_pct:.1f}%",
            delta=None
        )


def display_warehouse_performance_grid(warehouse_summary: Dict[str, Any]):
    """Display warehouse performance in a grid layout."""
    if not warehouse_summary:
        st.info("No warehouse performance data available")
        return
    
    st.markdown("### Warehouse Performance")
    
    # Convert to list for easier handling
    warehouses = []
    for name, stats in warehouse_summary.items():
        warehouses.append({
            'name': name,
            'total_credits': stats.get('total_credits', 0),
            'avg_daily_credits': stats.get('avg_daily_credits', 0),
            'total_queries': stats.get('total_queries', 0),
            'cost_per_query': stats.get('cost_per_query', 0)
        })
    
    # Sort by total credits (highest first)
    warehouses.sort(key=lambda x: x['total_credits'], reverse=True)
    
    # Display top warehouses in grid
    for i in range(0, min(len(warehouses), 6), 3):  # Show up to 6 warehouses, 3 per row
        cols = st.columns(3)
        
        for j, col in enumerate(cols):
            if i + j < len(warehouses):
                warehouse = warehouses[i + j]
                
                with col:
                    st.markdown(f"**{warehouse['name']}**")
                    st.metric("Credits", f"{warehouse['total_credits']:,.1f}")
                    st.metric("Queries", f"{warehouse['total_queries']:,}")
                    st.metric("Cost/Query", f"{warehouse['cost_per_query']:.4f}")


def display_optimization_score(optimization_data: Dict[str, Any]):
    """Display overall optimization score."""
    # Calculate a simple optimization score based on available data
    score = 75  # Default score
    
    # Adjust based on alerts
    alerts = optimization_data.get('alerts', [])
    critical_alerts = len([a for a in alerts if a.get('severity') == 'critical'])
    high_alerts = len([a for a in alerts if a.get('severity') == 'high'])
    
    score -= (critical_alerts * 10 + high_alerts * 5)
    
    # Adjust based on utilization
    utilization_data = optimization_data.get('utilization_metrics', {})
    if utilization_data:
        avg_utilization = sum(
            getattr(m, 'avg_utilization', 0.5) for m in utilization_data.values()
        ) / len(utilization_data)
        
        # Higher utilization is generally better
        score += (avg_utilization - 0.5) * 20
    
    score = max(0, min(100, score))  # Clamp between 0-100
    
    # Display score with color coding
    if score >= 80:
        color = "green"
        status = "Excellent"
    elif score >= 60:
        color = "orange"
        status = "Good"
    else:
        color = "red"
        status = "Needs Attention"
    
    st.markdown("### Optimization Score")
    st.markdown(f"<h2 style='color: {color};'>{score:.0f}/100 - {status}</h2>", 
                unsafe_allow_html=True)
    
    # Show improvement areas
    if score < 80:
        st.markdown("**Areas for Improvement:**")
        if critical_alerts > 0:
            st.write(f"• {critical_alerts} critical alerts need immediate attention")
        if high_alerts > 0:
            st.write(f"• {high_alerts} high-priority issues to resolve")
        if utilization_data and avg_utilization < 0.5:
            st.write("• Warehouse utilization could be improved")


def display_savings_potential(recommendations: List[Dict[str, Any]]):
    """Display potential savings from recommendations."""
    if not recommendations:
        st.info("No savings opportunities identified")
        return
    
    total_savings = sum(
        rec.get('estimated_savings', 0) for rec in recommendations 
        if rec.get('estimated_savings', 0) > 0
    )
    
    st.markdown("### Savings Potential")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric(
            label="Monthly Savings",
            value=f"${total_savings:,.2f}",
            delta=None
        )
    
    with col2:
        annual_savings = total_savings * 12
        st.metric(
            label="Annual Savings",
            value=f"${annual_savings:,.2f}",
            delta=None
        )
    
    # Show top opportunities
    if recommendations:
        st.markdown("**Top Opportunities:**")
        sorted_recs = sorted(
            recommendations, 
            key=lambda x: x.get('estimated_savings', 0), 
            reverse=True
        )
        
        for i, rec in enumerate(sorted_recs[:3], 1):
            savings = rec.get('estimated_savings', 0)
            if savings > 0:
                st.write(f"{i}. ${savings:,.2f}/month - {rec.get('description', 'Unknown')}")


def create_metric_card(title: str, value: str, delta: Optional[str] = None, 
                      help_text: Optional[str] = None):
    """Create a custom metric card with styling."""
    delta_html = ""
    if delta:
        delta_color = "green" if delta.startswith("+") else "red" if delta.startswith("-") else "gray"
        delta_html = f'<p style="color: {delta_color}; margin: 0; font-size: 0.8em;">{delta}</p>'
    
    help_html = ""
    if help_text:
        help_html = f'<p style="color: gray; margin: 0; font-size: 0.7em;">{help_text}</p>'
    
    card_html = f"""
    <div style="
        background-color: #f0f2f6; 
        padding: 1rem; 
        border-radius: 0.5rem; 
        margin: 0.5rem 0;
        border-left: 4px solid #1f77b4;
    ">
        <h4 style="margin: 0; color: #1f77b4;">{title}</h4>
        <h2 style="margin: 0.2rem 0;">{value}</h2>
        {delta_html}
        {help_html}
    </div>
    """
    
    return card_html 