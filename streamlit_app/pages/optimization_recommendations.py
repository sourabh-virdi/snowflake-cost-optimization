"""
Optimization recommendations page for Snowflake Cost Optimizer.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from loguru import logger

from snowflake_optimizer import SnowflakeConnector, WarehouseOptimizer, QueryOptimizer, StorageOptimizer
from snowflake_optimizer.analyzers import CostAnalyzer


def show_optimization_recommendations(connector: SnowflakeConnector, analysis_days: int):
    """Display optimization recommendations page."""
    st.title("Optimization Recommendations")
    
    try:
        # Get optimization data
        warehouse_optimizer = WarehouseOptimizer(connector)
        query_optimizer = QueryOptimizer(connector) 
        storage_optimizer = StorageOptimizer(connector)
        
        # Load recommendations
        with st.spinner("Loading optimization recommendations..."):
            warehouse_recs = warehouse_optimizer.analyze_warehouse_optimization_opportunities(analysis_days)
            query_recs = query_optimizer.analyze_query_optimization_opportunities(analysis_days)
            storage_recs = storage_optimizer.analyze_storage_optimization_opportunities()
        
        # Display overview
        display_optimization_overview(warehouse_recs, query_recs, storage_recs)
        
        # Display detailed recommendations
        display_detailed_recommendations(warehouse_recs, query_recs, storage_recs)
        
        # Display savings analysis
        display_savings_analysis(warehouse_recs, query_recs, storage_recs)
        
    except Exception as e:
        st.error(f"Error loading optimization recommendations: {e}")
        logger.error(f"Optimization recommendations error: {e}")


def display_optimization_overview(warehouse_recs: List, query_recs: List, storage_recs: List):
    """Display optimization overview metrics."""
    st.markdown("### Optimization Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_recs = len(warehouse_recs) + len(query_recs) + len(storage_recs)
        st.metric("Total Recommendations", total_recs)
    
    with col2:
        high_impact = sum(1 for rec in warehouse_recs + query_recs + storage_recs 
                         if hasattr(rec, 'estimated_improvement') and rec.estimated_improvement > 30)
        st.metric("High Impact", high_impact)
    
    with col3:
        # Calculate potential savings
        warehouse_savings = sum(getattr(rec, 'estimated_savings', 0) for rec in warehouse_recs)
        query_savings = sum(getattr(rec, 'estimated_improvement', 0) * 10 for rec in query_recs)  # Rough estimate
        storage_savings = sum(getattr(rec, 'estimated_savings', 0) for rec in storage_recs)
        total_savings = warehouse_savings + query_savings + storage_savings
        st.metric("Potential Monthly Savings", f"${total_savings:,.2f}")
    
    with col4:
        # Implementation effort distribution
        if warehouse_recs or query_recs or storage_recs:
            all_recs = warehouse_recs + query_recs + storage_recs
            low_effort = sum(1 for rec in all_recs 
                           if hasattr(rec, 'implementation_effort') and rec.implementation_effort == 'low')
            st.metric("Quick Wins (Low Effort)", low_effort)


def display_detailed_recommendations(warehouse_recs: List, query_recs: List, storage_recs: List):
    """Display detailed recommendations in tabs."""
    st.markdown("### Detailed Recommendations")
    
    tab1, tab2, tab3 = st.tabs(["Warehouse", "Query", "Storage"])
    
    with tab1:
        display_warehouse_recommendations(warehouse_recs)
    
    with tab2:
        display_query_recommendations(query_recs)
    
    with tab3:
        display_storage_recommendations(storage_recs)


def display_warehouse_recommendations(recommendations: List):
    """Display warehouse optimization recommendations."""
    if not recommendations:
        st.info("No warehouse optimization recommendations available.")
        return
    
    st.markdown("#### Warehouse Optimization Opportunities")
    
    for i, rec in enumerate(recommendations[:10], 1):  # Show top 10
        with st.expander(f"{i}. {rec.recommendation_type.title()} - {rec.warehouse_name}"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Current Configuration:**")
                for key, value in rec.current_config.items():
                    st.write(f"• {key}: {value}")
                
                st.markdown("**Recommendation:**")
                st.write(rec.description)
            
            with col2:
                st.markdown("**Recommended Configuration:**")
                for key, value in rec.recommended_config.items():
                    st.write(f"• {key}: {value}")
                
                # Metrics
                st.metric("Estimated Savings", f"${rec.estimated_savings:,.2f}")
                st.metric("Confidence", f"{rec.confidence_score:.1%}")
                
                # Implementation effort
                effort_label = {
                    'low': '[LOW]',
                    'medium': '[MEDIUM]', 
                    'high': '[HIGH]'
                }.get(rec.implementation_effort, '[MEDIUM]')
                st.write(f"**Implementation:** {effort_label} {rec.implementation_effort.title()}")


def display_query_recommendations(recommendations: List):
    """Display query optimization recommendations."""
    if not recommendations:
        st.info("No query optimization recommendations available.")
        return
    
    st.markdown("#### Query Optimization Opportunities")
    
    # Summary chart
    if recommendations:
        rec_types = {}
        for rec in recommendations:
            rec_type = rec.recommendation_type
            rec_types[rec_type] = rec_types.get(rec_type, 0) + 1
        
        if rec_types:
            df = pd.DataFrame(list(rec_types.items()), columns=['Type', 'Count'])
            fig = px.bar(df, x='Type', y='Count', title="Recommendation Types")
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
    
    # Detailed recommendations
    for i, rec in enumerate(recommendations[:10], 1):
        with st.expander(f"{i}. {rec.recommendation_type.title()} - Query {rec.query_id[:8]}..."):
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Current Performance:**")
                for key, value in rec.current_performance.items():
                    if isinstance(value, (int, float)):
                        if 'time' in key.lower():
                            st.write(f"• {key}: {value:,.0f} ms")
                        elif 'bytes' in key.lower():
                            st.write(f"• {key}: {value/1024/1024:,.1f} MB")
                        else:
                            st.write(f"• {key}: {value:,.0f}")
                    else:
                        st.write(f"• {key}: {value}")
            
            with col2:
                st.markdown("**Recommended Changes:**")
                for key, value in rec.recommended_changes.items():
                    st.write(f"• {key}: {value}")
                
                st.metric("Expected Improvement", f"{rec.estimated_improvement:.1f}%")
                st.metric("Confidence", f"{rec.confidence_score:.1%}")
                
                effort_label = {
                    'low': '[LOW]',
                    'medium': '[MEDIUM]',
                    'high': '[HIGH]'
                }.get(rec.implementation_effort, '[MEDIUM]')
                st.write(f"**Implementation:** {effort_label} {rec.implementation_effort.title()}")
                
            st.markdown("**Description:**")
            st.write(rec.description)


def display_storage_recommendations(recommendations: List):
    """Display storage optimization recommendations."""
    if not recommendations:
        st.info("No storage optimization recommendations available.")
        return
    
    st.markdown("#### Storage Optimization Opportunities")
    
    # Summary chart
    if recommendations:
        rec_types = {}
        savings_by_type = {}
        
        for rec in recommendations:
            rec_type = rec.recommendation_type
            rec_types[rec_type] = rec_types.get(rec_type, 0) + 1
            savings_by_type[rec_type] = savings_by_type.get(rec_type, 0) + rec.estimated_savings
        
        if rec_types:
            col1, col2 = st.columns(2)
            
            with col1:
                df_counts = pd.DataFrame(list(rec_types.items()), columns=['Type', 'Count'])
                fig1 = px.bar(df_counts, x='Type', y='Count', title="Recommendations by Type")
                fig1.update_xaxes(tickangle=45)
                st.plotly_chart(fig1, use_container_width=True)
            
            with col2:
                df_savings = pd.DataFrame(list(savings_by_type.items()), columns=['Type', 'Savings'])
                fig2 = px.bar(df_savings, x='Type', y='Savings', title="Potential Savings by Type")
                fig2.update_xaxes(tickangle=45)
                st.plotly_chart(fig2, use_container_width=True)
    
    # Detailed recommendations
    for i, rec in enumerate(recommendations[:10], 1):
        with st.expander(f"{i}. {rec.recommendation_type.title()} - {rec.object_name}"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Current Storage:**")
                for key, value in rec.current_storage.items():
                    if isinstance(value, (int, float)) and 'bytes' in key.lower():
                        st.write(f"• {key}: {value/1024/1024/1024:,.1f} GB")
                    else:
                        st.write(f"• {key}: {value}")
            
            with col2:
                st.markdown("**Recommended Changes:**")
                for key, value in rec.recommended_changes.items():
                    st.write(f"• {key}: {value}")
                
                st.metric("Estimated Savings", f"${rec.estimated_savings:,.2f}")
                st.metric("Confidence", f"{rec.confidence_score:.1%}")
                
                effort_label = {
                    'low': '[LOW]',
                    'medium': '[MEDIUM]',
                    'high': '[HIGH]'
                }.get(rec.implementation_effort, '[MEDIUM]')
                st.write(f"**Implementation:** {effort_label} {rec.implementation_effort.title()}")
            
            st.markdown("**Description:**")
            st.write(rec.description)


def display_savings_analysis(warehouse_recs: List, query_recs: List, storage_recs: List):
    """Display savings analysis and implementation timeline."""
    st.markdown("### Savings Analysis")
    
    # Calculate savings by implementation effort
    all_recs = []
    
    for rec in warehouse_recs:
        all_recs.append({
            'type': 'Warehouse',
            'effort': getattr(rec, 'implementation_effort', 'medium'),
            'savings': getattr(rec, 'estimated_savings', 0),
            'confidence': getattr(rec, 'confidence_score', 0.5)
        })
    
    for rec in query_recs:
        all_recs.append({
            'type': 'Query',
            'effort': getattr(rec, 'implementation_effort', 'medium'),
            'savings': getattr(rec, 'estimated_improvement', 0) * 10,  # Rough conversion
            'confidence': getattr(rec, 'confidence_score', 0.5)
        })
    
    for rec in storage_recs:
        all_recs.append({
            'type': 'Storage',
            'effort': getattr(rec, 'implementation_effort', 'medium'),
            'savings': getattr(rec, 'estimated_savings', 0),
            'confidence': getattr(rec, 'confidence_score', 0.5)
        })
    
    if all_recs:
        df = pd.DataFrame(all_recs)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Savings by effort level
            effort_savings = df.groupby('effort')['savings'].sum().reset_index()
            fig1 = px.pie(effort_savings, values='savings', names='effort', 
                         title="Potential Savings by Implementation Effort")
            st.plotly_chart(fig1, use_container_width=True)
        
        with col2:
            # Savings by recommendation type
            type_savings = df.groupby('type')['savings'].sum().reset_index()
            fig2 = px.bar(type_savings, x='type', y='savings', 
                         title="Potential Savings by Recommendation Type")
            st.plotly_chart(fig2, use_container_width=True)
        
        # Implementation roadmap
        st.markdown("#### Implementation Roadmap")
        
        effort_order = ['low', 'medium', 'high']
        effort_labels = {'low': '[LOW] Quick Wins', 'medium': '[MEDIUM] Medium Term', 'high': '[HIGH] Long Term'}
        
        for effort in effort_order:
            effort_recs = df[df['effort'] == effort]
            if not effort_recs.empty:
                total_savings = effort_recs['savings'].sum()
                avg_confidence = effort_recs['confidence'].mean()
                
                st.markdown(f"**{effort_labels[effort]} ({len(effort_recs)} recommendations)**")
                st.write(f"• Potential Savings: ${total_savings:,.2f}")
                st.write(f"• Average Confidence: {avg_confidence:.1%}")
                st.write(f"• Recommendation: {'Implement immediately' if effort == 'low' else 'Plan for implementation' if effort == 'medium' else 'Consider for future phases'}")
                st.markdown("---")
    
    else:
        st.info("No savings analysis available - run optimization analysis first.") 