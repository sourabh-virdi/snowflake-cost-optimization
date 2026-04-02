"""
Chart components for Streamlit dashboard.
"""

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from typing import Dict, Any, List, Optional


def create_cost_trend_chart(cost_data: Dict[str, Any]) -> Optional[go.Figure]:
    """Create cost trend visualization."""
    warehouse_analysis = cost_data.get('warehouse_analysis', {})
    trends = warehouse_analysis.get('trends', [])
    
    if not trends:
        return None
    
    fig = go.Figure()
    
    # Sample data for demonstration - replace with actual trend data
    dates = pd.date_range(start='2024-01-01', periods=30, freq='D')
    sample_costs = [100 + i * 2 + (i % 7) * 10 for i in range(30)]
    
    fig.add_trace(go.Scatter(
        x=dates,
        y=sample_costs,
        mode='lines+markers',
        name='Daily Cost',
        line=dict(color='blue', width=2),
        marker=dict(size=4)
    ))
    
    fig.update_layout(
        title="Cost Trend Analysis",
        xaxis_title="Date",
        yaxis_title="Cost (Credits)",
        showlegend=True,
        height=400
    )
    
    return fig


def create_warehouse_utilization_chart(utilization_data: Dict[str, Any]) -> Optional[go.Figure]:
    """Create warehouse utilization visualization."""
    if not utilization_data:
        return None
    
    warehouses = list(utilization_data.keys())
    utilization_scores = []
    efficiency_scores = []
    
    for warehouse, metrics in utilization_data.items():
        if hasattr(metrics, '__dict__'):
            utilization_scores.append(metrics.avg_utilization)
            efficiency_scores.append(metrics.cost_efficiency_score)
        else:
            utilization_scores.append(0.5)
            efficiency_scores.append(0.5)
    
    fig = go.Figure()
    
    # Add utilization bars
    fig.add_trace(go.Bar(
        x=warehouses,
        y=utilization_scores,
        name='Utilization',
        marker_color='lightblue'
    ))
    
    # Add efficiency line
    fig.add_trace(go.Scatter(
        x=warehouses,
        y=efficiency_scores,
        mode='lines+markers',
        name='Efficiency',
        yaxis='y2',
        line=dict(color='red', width=2),
        marker=dict(size=8)
    ))
    
    fig.update_layout(
        title="Warehouse Utilization vs Efficiency",
        xaxis_title="Warehouse",
        yaxis_title="Utilization Score",
        yaxis2=dict(
            title="Efficiency Score",
            overlaying='y',
            side='right'
        ),
        showlegend=True,
        height=400
    )
    
    return fig


def create_cost_breakdown_pie_chart(cost_breakdown: Dict[str, float]) -> Optional[go.Figure]:
    """Create cost breakdown pie chart."""
    if not cost_breakdown:
        return None
    
    warehouse_pct = cost_breakdown.get('warehouse_percentage', 0)
    storage_pct = cost_breakdown.get('storage_percentage', 0)
    other_pct = 100 - warehouse_pct - storage_pct
    
    labels = ['Warehouse', 'Storage', 'Other']
    values = [warehouse_pct, storage_pct, other_pct]
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c']
    
    fig = go.Figure(data=[go.Pie(
        labels=labels,
        values=values,
        marker_colors=colors,
        textinfo='label+percent',
        hovertemplate='<b>%{label}</b><br>%{percent}<br>%{value:.1f}%<extra></extra>'
    )])
    
    fig.update_layout(
        title="Cost Breakdown by Category",
        height=400
    )
    
    return fig


def create_warehouse_performance_heatmap(warehouse_summary: Dict[str, Any]) -> Optional[go.Figure]:
    """Create warehouse performance heatmap."""
    if not warehouse_summary:
        return None
    
    warehouses = list(warehouse_summary.keys())
    metrics = ['total_credits', 'total_queries', 'cost_per_query', 'avg_daily_credits']
    
    # Normalize data for heatmap
    z_data = []
    for metric in metrics:
        row = []
        values = [warehouse_summary[w].get(metric, 0) for w in warehouses]
        max_val = max(values) if values else 1
        
        for warehouse in warehouses:
            normalized_value = warehouse_summary[warehouse].get(metric, 0) / max_val
            row.append(normalized_value)
        z_data.append(row)
    
    fig = go.Figure(data=go.Heatmap(
        z=z_data,
        x=warehouses,
        y=[m.replace('_', ' ').title() for m in metrics],
        colorscale='RdYlBu_r',
        hovertemplate='<b>%{y}</b><br>Warehouse: %{x}<br>Normalized Value: %{z:.2f}<extra></extra>'
    ))
    
    fig.update_layout(
        title="Warehouse Performance Heatmap",
        xaxis_title="Warehouse",
        yaxis_title="Metrics",
        height=400
    )
    
    return fig


def create_query_performance_chart(query_data: pd.DataFrame) -> Optional[go.Figure]:
    """Create query performance analysis chart."""
    if query_data.empty:
        return None
    
    # Create histogram of query execution times
    fig = go.Figure()
    
    fig.add_trace(go.Histogram(
        x=query_data['total_elapsed_time'] / 1000,  # Convert to seconds
        nbinsx=50,
        name='Query Execution Time',
        marker_color='skyblue',
        opacity=0.7
    ))
    
    fig.update_layout(
        title="Query Execution Time Distribution",
        xaxis_title="Execution Time (seconds)",
        yaxis_title="Number of Queries",
        height=400
    )
    
    return fig


def create_storage_usage_chart(storage_data: Dict[str, Any]) -> Optional[go.Figure]:
    """Create storage usage visualization."""
    breakdown = storage_data.get('storage_breakdown', {})
    
    if not breakdown:
        return None
    
    categories = ['Active', 'Time Travel', 'Failsafe']
    sizes = [
        breakdown.get('active_gb', 0),
        breakdown.get('time_travel_gb', 0),
        breakdown.get('failsafe_gb', 0)
    ]
    
    colors = ['#2E86C1', '#F39C12', '#E74C3C']
    
    fig = go.Figure(data=[go.Pie(
        labels=categories,
        values=sizes,
        marker_colors=colors,
        textinfo='label+percent+value',
        hovertemplate='<b>%{label}</b><br>%{value:.1f} GB<br>%{percent}<extra></extra>'
    )])
    
    fig.update_layout(
        title="Storage Usage by Type",
        height=400
    )
    
    return fig


def create_user_activity_chart(user_data: Dict[str, Any]) -> Optional[go.Figure]:
    """Create user activity visualization."""
    user_summary = user_data.get('user_summary', {})
    
    if not user_summary:
        return None
    
    # Get top 10 users by query count
    users = list(user_summary.keys())[:10]
    query_counts = [user_summary[user].get('total_queries', 0) for user in users]
    
    fig = go.Figure(data=[go.Bar(
        x=users,
        y=query_counts,
        marker_color='lightgreen'
    )])
    
    fig.update_layout(
        title="Top Users by Query Count",
        xaxis_title="User",
        yaxis_title="Total Queries",
        xaxis_tickangle=-45,
        height=400
    )
    
    return fig


def create_optimization_savings_chart(recommendations: List[Any]) -> Optional[go.Figure]:
    """Create optimization savings potential chart."""
    if not recommendations:
        return None
    
    # Group by recommendation type
    savings_by_type = {}
    for rec in recommendations:
        if hasattr(rec, 'recommendation_type') and hasattr(rec, 'estimated_savings'):
            if rec.estimated_savings > 0:
                rec_type = rec.recommendation_type
                savings_by_type[rec_type] = savings_by_type.get(rec_type, 0) + rec.estimated_savings
    
    if not savings_by_type:
        return None
    
    types = list(savings_by_type.keys())
    savings = list(savings_by_type.values())
    
    fig = go.Figure(data=[go.Bar(
        x=types,
        y=savings,
        marker_color='gold',
        text=[f'${s:,.0f}' for s in savings],
        textposition='auto'
    )])
    
    fig.update_layout(
        title="Potential Monthly Savings by Recommendation Type",
        xaxis_title="Recommendation Type",
        yaxis_title="Monthly Savings ($)",
        height=400
    )
    
    return fig


def create_timeline_chart(dates: List[str], values: List[float], 
                         title: str, y_label: str) -> go.Figure:
    """Create a generic timeline chart."""
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=dates,
        y=values,
        mode='lines+markers',
        line=dict(width=2),
        marker=dict(size=6)
    ))
    
    fig.update_layout(
        title=title,
        xaxis_title="Date",
        yaxis_title=y_label,
        height=400
    )
    
    return fig


def create_comparison_chart(categories: List[str], values1: List[float], 
                           values2: List[float], label1: str, label2: str,
                           title: str) -> go.Figure:
    """Create a comparison bar chart."""
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        name=label1,
        x=categories,
        y=values1,
        marker_color='lightblue'
    ))
    
    fig.add_trace(go.Bar(
        name=label2,
        x=categories,
        y=values2,
        marker_color='lightcoral'
    ))
    
    fig.update_layout(
        title=title,
        barmode='group',
        height=400
    )
    
    return fig 