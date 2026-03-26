"""
Snowflake Cost Optimization and Data Governance Platform

An intelligent application that analyzes Snowflake usage patterns,
warehouse spend, query performance, and data access to provide
optimization recommendations and cost-saving opportunities.
"""

__version__ = "1.0.0"
__author__ = "Snowflake Optimizer Team"

from .connectors.snowflake_connector import SnowflakeConnector
from .analyzers.cost_analyzer import CostAnalyzer
from .analyzers.usage_analyzer import UsageAnalyzer
from .analyzers.performance_analyzer import PerformanceAnalyzer
from .analyzers.access_analyzer import AccessAnalyzer
from .optimizers.warehouse_optimizer import WarehouseOptimizer
from .optimizers.query_optimizer import QueryOptimizer
from .optimizers.storage_optimizer import StorageOptimizer

__all__ = [
    "SnowflakeConnector",
    "CostAnalyzer",
    "UsageAnalyzer", 
    "PerformanceAnalyzer",
    "AccessAnalyzer",
    "WarehouseOptimizer",
    "QueryOptimizer",
    "StorageOptimizer",
] 