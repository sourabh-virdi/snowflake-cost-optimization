"""
Main Streamlit application for Snowflake Cost Optimization.
"""

import streamlit as st
import sys
import os
from pathlib import Path
from typing import Dict, Any, Optional

# Add src to Python path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from snowflake_optimizer import (
    SnowflakeConnector, CostAnalyzer, UsageAnalyzer, 
    PerformanceAnalyzer, AccessAnalyzer
)
from snowflake_optimizer.config import get_settings
from pages import cost_dashboard, usage_analysis, optimization_recommendations, data_governance
# Metrics are now displayed using internal methods
# Chart components are imported by individual pages as needed
import plotly.express as px
import pandas as pd
from loguru import logger

# Constants
DEFAULT_ANALYSIS_DAYS = 30
MIN_ANALYSIS_DAYS = 1
MAX_ANALYSIS_DAYS = 365
CACHE_TTL_MINUTES = 30

# Page navigation configuration
PAGES = {
    "Dashboard": {"description": "Overview and key metrics"},
    "Cost Analysis": {"description": "Detailed cost breakdown"},
    "Usage Analysis": {"description": "Resource utilization patterns"},
    "Performance Analysis": {"description": "Query performance insights"},
    "Data Governance": {"description": "Access patterns and compliance"},
    "Optimization Recommendations": {"description": "Cost-saving suggestions"}
}

# Severity mapping for alerts
SEVERITY_ICONS = {
    'critical': '[CRITICAL]',
    'high': '[HIGH]', 
    'medium': '[MEDIUM]',
    'low': '[LOW]'
}


# Configure Streamlit page
st.set_page_config(
    page_title="Snowflake Cost Optimizer",
    page_icon="SF",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-container {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .sidebar-content {
        background-color: #f8f9fa;
    }
    .stAlert {
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)


class SnowflakeOptimizerApp:
    """Main Streamlit application class."""
    
    def __init__(self):
        """Initialize the application."""
        self.settings = None
        self.connector = None
        self._initialize_session_state()
    
    def _initialize_session_state(self):
        """Initialize Streamlit session state variables."""
        default_state = {
            'connected': False,
            'connection_error': None,
            'cached_data': {},
            'connector': None,
            'settings': None
        }
        
        for key, default_value in default_state.items():
            if key not in st.session_state:
                st.session_state[key] = default_value
        
        # Initialize connector and settings from session state if connected
        if st.session_state.connected and st.session_state.connector is not None:
            self.connector = st.session_state.connector
            self.settings = st.session_state.settings
    
    def setup_sidebar(self) -> int:
        """Set up the application sidebar and return analysis days."""
        with st.sidebar:
            st.markdown("### Configuration")
            
            # Connection status and controls
            self._display_connection_status()
            
            # Analysis period
            st.markdown("### Analysis Settings")
            analysis_days = st.slider(
                "Analysis Period (Days)",
                min_value=MIN_ANALYSIS_DAYS,
                max_value=MAX_ANALYSIS_DAYS,
                value=DEFAULT_ANALYSIS_DAYS,
                step=1
            )
            
            # Cache management section
            if st.session_state.connected and st.session_state.connector:
                self._display_cache_management()
            
            # Help section
            with st.expander("Help"):
                st.markdown("""
                **Navigation:** Use the tabs above to switch between different analysis views
                
                **Tips:**
                - Increase analysis period for trends
                - Use filters to focus on specific areas
                - Check alerts for immediate actions
                - Monitor cache statistics for performance
                """)
            
        return analysis_days
    
    def _display_connection_status(self):
        """Display connection status and controls in sidebar."""
        if st.session_state.connected:
            st.success("Connected to Snowflake")
            if st.button("Disconnect"):
                self._disconnect_snowflake()
        else:
            st.error("Not connected to Snowflake")
            if st.button("Connect"):
                self._connect_to_snowflake()
        
        # Show connection error if any
        if st.session_state.connection_error:
            st.error(f"Connection Error: {st.session_state.connection_error}")

    def _connect_to_snowflake(self):
        """Connect to Snowflake using configuration system (env vars or config file)."""
        try:
            with st.spinner("Connecting to Snowflake..."):
                # Initialize settings and connector
                self.settings = get_settings()
                
                # Validate required settings
                required_fields = [
                    self.settings.snowflake.account,
                    self.settings.snowflake.user,
                    self.settings.snowflake.warehouse,
                    self.settings.snowflake.database,
                    self.settings.snowflake.schema_name
                ]
                
                if not all(required_fields):
                    st.session_state.connection_error = (
                        "Missing Snowflake configuration. Please set credentials in "
                        "environment variables or config/config.yaml file"
                    )
                    return
                
                # Check authentication method
                if not self.settings.snowflake.password and not self.settings.snowflake.private_key_path:
                    st.session_state.connection_error = (
                        "Missing authentication: provide either password or private_key_path"
                    )
                    return
                
                self.connector = SnowflakeConnector(self.settings.snowflake)
                
                # Test connection
                if self.connector.test_connection():
                    st.session_state.connected = True
                    st.session_state.connection_error = None
                    # Store connector and settings in session state for persistence
                    st.session_state.connector = self.connector
                    st.session_state.settings = self.settings
                    st.success("Successfully connected to Snowflake!")
                    st.rerun()
                else:
                    st.session_state.connection_error = "Connection test failed"
                    
        except Exception as e:
            st.session_state.connection_error = str(e)
            logger.error(f"Connection error: {e}")

    def _disconnect_snowflake(self):
        """Disconnect from Snowflake."""
        try:
            if self.connector:
                self.connector.close()
            # Clear all connection-related state
            connection_state_keys = ['connected', 'connection_error', 'cached_data', 'connector', 'settings']
            for key in connection_state_keys:
                st.session_state[key] = None if 'error' in key else (False if 'connected' in key else {})
            
            # Clear instance variables
            self.connector = None
            self.settings = None
            st.success("Disconnected from Snowflake")
            st.rerun()
        except Exception as e:
            logger.error(f"Disconnect error: {e}")
    
    def _display_connection_setup(self):
        """Display connection setup instructions."""
        st.markdown('<div class="main-header">Snowflake Cost Optimizer</div>', unsafe_allow_html=True)
        
        st.markdown("## Getting Started")
        
        st.info("""
        **Welcome to the Snowflake Cost Optimization Platform!**
        
        To get started, configure your Snowflake connection using **environment variables** or the **config file**.
        """)
        
        # Configuration tabs for better organization
        tab1, tab2, tab3 = st.tabs(["Config Options", "Permissions", "Manual Setup"])
        
        with tab1:
            self._display_config_options()
        
        with tab2:
            self._display_permissions_info()
        
        with tab3:
            self._display_manual_setup()

    def _display_config_options(self):
        """Display configuration options in a tab."""
        st.markdown("### Configuration Priority Order")
        st.markdown("**Environment Variables** → **Config File** → **Manual Entry**")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Environment Variables (Recommended)")
            st.code("""
# Create .env file in project root:
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_ROLE=your_role
            """, language="bash")
        
        with col2:
            st.markdown("#### Config File (Current Setup)")
            st.success("Credentials configured in `config/config.yaml`")
            st.markdown("The app will use these automatically.")
            
            st.markdown("#### Key Pair Authentication")
            st.code("""
SNOWFLAKE_PRIVATE_KEY_PATH=path/to/key.p8
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=passphrase
            """, language="bash")

    def _display_permissions_info(self):
        """Display required permissions information."""
        st.markdown("### Required Snowflake Permissions")
        
        permissions = [
            ("Account Usage", "`SNOWFLAKE.ACCOUNT_USAGE` schema", "Cost and usage analysis"),
            ("Information Schema", "`INFORMATION_SCHEMA` views", "Metadata analysis"),
            ("Target Resources", "Warehouses, databases, schemas", "Optimization analysis")
        ]
        
        for label, resource, purpose in permissions:
            st.markdown(f"**{label}: {resource}**")
            st.markdown(f"   *Purpose: {purpose}*")

    def _display_manual_setup(self):
        """Display manual connection setup form."""
        st.markdown("### Manual Connection Test")
        st.warning("This is for testing only. Use environment variables for production.")
        
        with st.form("manual_connection"):
            col1, col2 = st.columns(2)
            
            with col1:
                account = st.text_input("Account Identifier*")
                user = st.text_input("Username*")
                password = st.text_input("Password*", type="password")
                warehouse = st.text_input("Warehouse*")
            
            with col2:
                database = st.text_input("Database*")
                schema = st.text_input("Schema*")
                role = st.text_input("Role (optional)")
            
            submitted = st.form_submit_button("Test Connection", type="primary")
            
            if submitted:
                required_fields = [account, user, password, warehouse, database, schema]
                if all(required_fields):
                    # Temporarily set environment variables for testing
                    test_env = {
                        'SNOWFLAKE_ACCOUNT': account,
                        'SNOWFLAKE_USER': user,
                        'SNOWFLAKE_PASSWORD': password,
                        'SNOWFLAKE_WAREHOUSE': warehouse,
                        'SNOWFLAKE_DATABASE': database,
                        'SNOWFLAKE_SCHEMA': schema,
                        'SNOWFLAKE_ROLE': role or ''
                    }
                    os.environ.update(test_env)
                    self._connect_to_snowflake()
                else:
                    st.error("Please fill in all required fields (marked with *)")

 

    def _display_dashboard(self, analysis_days: int):
        """Display the main dashboard."""
        st.markdown('<div class="main-header">Snowflake Cost Optimizer Dashboard</div>', unsafe_allow_html=True)
        
        try:
            # Load or fetch dashboard data
            dashboard_data = self._get_dashboard_data(analysis_days)
            
            if not dashboard_data:
                st.warning("No data available for the dashboard")
                return
            
            # Display dashboard components
            self._display_key_metrics(dashboard_data)
            self._display_dashboard_charts(dashboard_data)
            self._display_dashboard_alerts(dashboard_data)
            
        except Exception as e:
            st.error(f"Error loading dashboard: {e}")
            logger.error(f"Dashboard error: {e}")

    def _get_dashboard_data(self, analysis_days: int) -> Optional[Dict[str, Any]]:
        """Load dashboard data from cache or fetch new data."""
        cache_key = f"dashboard_data_{analysis_days}"
        
        if cache_key not in st.session_state.cached_data:
            with st.spinner("Loading dashboard data..."):
                dashboard_data = self._load_dashboard_data(analysis_days, cache_key)
                if dashboard_data:
                    st.session_state.cached_data[cache_key] = dashboard_data
                    
        # Show cache status for transparency
        if hasattr(self.connector, 'enable_cache') and self.connector.enable_cache:
            cache_stats = self.connector.get_cache_stats()
            if cache_stats.get('valid_entries', 0) > 0:
                st.info(f"Using cached data where available ({cache_stats['valid_entries']} cached queries)")
        
        return st.session_state.cached_data.get(cache_key)

    def _load_dashboard_data(self, analysis_days: int, cache_key: str) -> Optional[Dict[str, Any]]:
        """Load data for the dashboard."""
        # Safety check: ensure connector is available
        if self.connector is None and st.session_state.connected:
            if st.session_state.connector is not None:
                self.connector = st.session_state.connector
                self.settings = st.session_state.settings
            else:
                # Reconnect if needed
                self.settings = get_settings()
                self.connector = SnowflakeConnector(self.settings.snowflake)
                st.session_state.connector = self.connector
                st.session_state.settings = self.settings
        
        try:
            cost_analyzer = CostAnalyzer(self.connector)
            usage_analyzer = UsageAnalyzer(self.connector)
            
            # Load analysis data
            cost_analysis = cost_analyzer.analyze_overall_costs(analysis_days)
            usage_analysis = usage_analyzer.analyze_warehouse_usage_patterns(analysis_days)
            
            return {
                'cost_analysis': cost_analysis,
                'usage_analysis': usage_analysis,
                'analysis_days': analysis_days
            }
        except Exception as e:
            logger.error(f"Error loading dashboard data: {e}")
            return None

    def _display_key_metrics(self, dashboard_data: Dict[str, Any]):
        """Display key metrics in the dashboard."""
        cost_data = dashboard_data.get('cost_analysis', {})
        usage_data = dashboard_data.get('usage_analysis', {})
        
        st.markdown("### Key Metrics")
        
        col1, col2, col3, col4 = st.columns(4)
        
        metrics = [
            (col1, "Estimated Monthly Cost", f"${cost_data.get('estimated_monthly_cost', 0):,.2f}"),
            (col2, "Total Credits (Period)", f"{cost_data.get('warehouse_analysis', {}).get('total_cost', 0):,.2f}"),
            (col3, "Total Storage", f"{cost_data.get('storage_analysis', {}).get('total_storage_gb', 0):,.1f} GB"),
            (col4, "Active Warehouses", str(usage_data.get('summary', {}).get('total_warehouses', 0)))
        ]
        
        for col, label, value in metrics:
            with col:
                st.metric(label, value)

    def _display_dashboard_charts(self, dashboard_data: Dict[str, Any]):
        """Display charts in the dashboard."""
        st.markdown("### Analysis Overview")
        
        col1, col2 = st.columns(2)
        
        with col1:
            self._display_cost_breakdown_chart(dashboard_data)
        
        with col2:
            self._display_warehouse_performance_chart(dashboard_data)

    def _display_cost_breakdown_chart(self, dashboard_data: Dict[str, Any]):
        """Display cost breakdown pie chart."""
        cost_analysis = dashboard_data.get('cost_analysis', {})
        cost_breakdown = cost_analysis.get('cost_breakdown', {})
        
        if cost_breakdown:
            warehouse_pct = cost_breakdown.get('warehouse_percentage', 0)
            storage_pct = cost_breakdown.get('storage_percentage', 0)
            other_pct = 100 - warehouse_pct - storage_pct
            
            breakdown_df = pd.DataFrame([
                {'Category': 'Warehouse', 'Percentage': warehouse_pct},
                {'Category': 'Storage', 'Percentage': storage_pct},
                {'Category': 'Other', 'Percentage': max(0, other_pct)}
            ])
            
            fig = px.pie(breakdown_df, values='Percentage', names='Category', 
                        title="Cost Breakdown")
            st.plotly_chart(fig, use_container_width=True)

    def _display_warehouse_performance_chart(self, dashboard_data: Dict[str, Any]):
        """Display warehouse performance scatter plot."""
        usage_analysis = dashboard_data.get('usage_analysis', {})
        utilization_metrics = usage_analysis.get('utilization_metrics', {})
        
        if utilization_metrics:
            util_data = [
                {
                    'Warehouse': name, 
                    'Utilization': metrics.avg_utilization, 
                    'Efficiency': metrics.cost_efficiency_score
                }
                for name, metrics in utilization_metrics.items()
            ]
            
            util_df = pd.DataFrame(util_data)
            fig = px.scatter(util_df, x='Utilization', y='Efficiency', 
                           hover_name='Warehouse', title="Warehouse Performance")
            st.plotly_chart(fig, use_container_width=True)

    def _display_dashboard_alerts(self, dashboard_data: Dict[str, Any]):
        """Display alerts and recommendations."""
        st.markdown("### Priority Alerts & Recommendations")
        
        cost_analysis = dashboard_data.get('cost_analysis', {})
        priority_alerts = cost_analysis.get('priority_alerts', [])
        recommendations = cost_analysis.get('recommendations', [])
        
        col1, col2 = st.columns(2)
        
        with col1:
            if priority_alerts:
                st.markdown("#### High Priority Alerts")
                for alert in priority_alerts[:5]:
                    severity = alert.get('severity', 'medium')
                    severity_text = SEVERITY_ICONS.get(severity, '[MEDIUM]')
                    
                    st.warning(
                        f"{severity_text} **{alert.get('message', 'Unknown alert')}** "
                        f"(Impact: ${alert.get('cost_impact', 0):,.2f})"
                    )
        
        with col2:
            if recommendations:
                st.markdown("#### Optimization Recommendations")
                for i, rec in enumerate(recommendations[:3], 1):
                    st.info(f"**{i}.** {rec}")

    def run(self):
        """Run the Streamlit application."""
        # Set up sidebar and get analysis settings
        analysis_days = self.setup_sidebar()
        
        # Check connection status for tab display
        if not st.session_state.connected:
            # If not connected, only show connection setup
            self._display_connection_setup()
            return
        
        # Main navigation using tabs
        tab_names = list(PAGES.keys())
        tabs = st.tabs(tab_names)
        
        # Route to appropriate page based on active tab
        with tabs[0]:  # Dashboard
            self._display_dashboard(analysis_days)
            
        with tabs[1]:  # Cost Analysis
            self._route_connected_page(
                lambda: cost_dashboard.show_cost_dashboard(self.connector, analysis_days)
            )
            
        with tabs[2]:  # Usage Analysis
            self._route_connected_page(
                lambda: usage_analysis.show_usage_analysis(self.connector, analysis_days)
            )
            
        with tabs[3]:  # Performance Analysis
            self._route_connected_page(
                lambda: st.info("Performance analysis module coming soon!")
            )
            
        with tabs[4]:  # Data Governance
            self._route_connected_page(
                lambda: data_governance.show_data_governance(self.connector, analysis_days)
            )
            
        with tabs[5]:  # Optimization Recommendations
            self._route_connected_page(
                lambda: optimization_recommendations.show_optimization_recommendations(self.connector, analysis_days)
            )

    def _route_connected_page(self, page_function):
        """Execute a page function (connection is already verified)."""
        try:
            page_function()
        except Exception as e:
            st.error(f"Error loading page: {e}")
            logger.error(f"Page error: {e}")

    def _display_cache_management(self):
        """Display cache management controls in sidebar."""
        st.markdown("### Cache Management")
        
        if not hasattr(self.connector, 'enable_cache') or not self.connector.enable_cache:
            st.info("Query caching is disabled")
            return
        
        # Get cache statistics
        try:
            cache_stats = self.connector.get_cache_stats()
            
            if cache_stats.get('status') == 'disabled':
                st.info("Query caching is disabled")
                return
            
            # Display cache stats
            with st.expander("Cache Statistics", expanded=False):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.metric("Valid Entries", cache_stats.get('valid_entries', 0))
                    st.metric("Total Size", f"{cache_stats.get('total_size_mb', 0):.1f} MB")
                
                with col2:
                    st.metric("Expired Entries", cache_stats.get('expired_entries', 0))
                    st.metric("Total Entries", cache_stats.get('total_entries', 0))
                
                # Cache hit rate estimation (would need implementation)
                if cache_stats.get('valid_entries', 0) > 0:
                    st.success("Cache is active and populated")
                     # Cache management buttons
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if st.button("Clear Expired", help="Remove expired cache entries"):
                            self.connector.cache.clear_expired()
                            st.success("Cleared expired entries")
                            st.rerun()
                    
                    with col2:
                        if st.button("Clear All", help="Clear all cached queries"):
                            self.connector.clear_cache()
                            st.success("Cleared all cache")
                            st.rerun()
                else:
                    st.info("Cache is empty")
            
           
            
            # Cache settings
            with st.expander("Cache Settings"):
                st.markdown("**Cache TTL by Query Type:**")
                ttl_info = {
                    "Warehouse Usage": "30 minutes",
                    "Query History": "1 hour", 
                    "Storage Usage": "6 hours",
                    "User Access": "24 hours",
                    "Cost Analysis": "2 hours"
                }
                
                for query_type, ttl in ttl_info.items():
                    st.markdown(f"• **{query_type}**: {ttl}")
                
                if st.button("Refresh Data", help="Force refresh current page data"):
                    # Clear cache for current session
                    if 'cached_data' in st.session_state:
                        st.session_state.cached_data = {}
                    st.success("Data will be refreshed on next load")
                    st.rerun()
                    
        except Exception as e:
            st.error(f"Error accessing cache: {e}")
            logger.error(f"Cache management error: {e}")


def main():
    """Main entry point for the Streamlit application."""
    app = SnowflakeOptimizerApp()
    app.run()


if __name__ == "__main__":
    main() 