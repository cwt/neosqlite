"""
NeoSQLite vs PyMongo - Comprehensive API Comparison Package

This package provides modular comparison tests between NeoSQLite and PyMongo APIs.
Each module focuses on a specific category of functionality.

Usage:
    from api_comparison import run_all_comparisons
    from api_comparison import run_category

    # Run all comparisons
    run_all_comparisons()

    # Run specific category
    run_category("crud")
    run_category("aggregation")
"""

from .reporter import CompatibilityReporter, reporter
from .runner import run_all_comparisons, run_category, cleanup_test_collections

# Import all comparison modules to register them
from . import crud
from . import query_operators
from . import expr_operators
from . import update_operators
from . import aggregation_stages
from . import aggregation_additional
from . import aggregation_stages_additional
from . import aggregation_stages_extended
from . import aggregation_cursor
from . import index_operations
from . import find_modify
from . import bulk_operations
from . import bulk_executors
from . import distinct
from . import binary_operations
from . import nested_queries
from . import raw_batches
from . import change_streams
from . import text_search
from . import gridfs_operations
from . import objectid_ops
from . import type_operator
from . import cursor_operations
from . import cursor_methods
from . import collection_methods
from . import collection_methods_additional
from . import database_methods
from . import mod_operator
from . import update_additional
from . import update_modifiers
from . import expr_additional
from . import expr_extended
from . import expr_complete
from . import expr_success
from . import date_operators
from . import math_operators
from . import string_operators
from . import array_operators
from . import object_operators
from . import search_index
from . import reindex
from . import elemmatch

__all__ = [
    "CompatibilityReporter",
    "reporter",
    "run_all_comparisons",
    "run_category",
    "cleanup_test_collections",
    # Comparison modules (imported for side effects)
    "crud",
    "query_operators",
    "expr_operators",
    "update_operators",
    "aggregation_stages",
    "aggregation_additional",
    "aggregation_stages_additional",
    "aggregation_stages_extended",
    "aggregation_cursor",
    "index_operations",
    "find_modify",
    "bulk_operations",
    "bulk_executors",
    "distinct",
    "binary_operations",
    "nested_queries",
    "raw_batches",
    "change_streams",
    "text_search",
    "gridfs_operations",
    "objectid_ops",
    "type_operator",
    "cursor_operations",
    "cursor_methods",
    "collection_methods",
    "collection_methods_additional",
    "database_methods",
    "mod_operator",
    "update_additional",
    "update_modifiers",
    "expr_additional",
    "expr_extended",
    "expr_complete",
    "expr_success",
    "date_operators",
    "math_operators",
    "string_operators",
    "array_operators",
    "object_operators",
    "search_index",
    "reindex",
    "elemmatch",
]
