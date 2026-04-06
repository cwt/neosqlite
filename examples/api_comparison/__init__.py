"""
NeoSQLite vs PyMongo - Comprehensive API Comparison Package

This package provides modular comparison tests between NeoSQLite and PyMongo APIs.
Each module focuses on a specific category of functionality.

Usage:
    from api_comparison import run_all_comparisons
    from api_comparison import run_category
    from api_comparison import run_benchmark

    # Run all comparisons
    run_all_comparisons()

    # Run specific category
    run_category("crud_operations")
    run_category("aggregation")

    # Run benchmark
    run_benchmark(iterations=10)
"""

# Import all comparison modules to register them
# Additional operator modules (split from new_operators)
from . import (
    aggregation_advanced_operators,
    aggregation_basic_stages,
    aggregation_bucket_stages,
    aggregation_complex_stages,
    aggregation_cursor_properties,
    aggregation_root_count_stages,
    array_expression_operators,
    array_set_sampling,
    binary_data,
    binary_operators,
    bitwise_operators,
    bulk_executors,
    bulk_operations,
    change_streams,
    collection_additional,
    collection_methods,
    crud_operations,
    cursor_methods,
    cursor_operations,
    database_methods,
    date_operations,
    distinct,
    elemmatch_operator,
    expr_array_object_operators,
    expr_comprehensive,
    expr_core_operators,
    expr_math_conditional,
    expr_type_conversion_operators,
    expression_operators,
    fill_stage,
    find_and_modify,
    graphlookup,
    gridfs,
    index_operations,
    json_schema,
    math_operators,
    mod_operator,
    nested_queries,
    object_field_operators,
    object_inspection,
    objectid,
    project_with_unwind_fts,
    pullall_operator,
    query_operators,
    raw_batches,
    reindex,
    search_index,
    session_transactions,
    string_operators,
    text_search,
    type_operator,
    type_operators,
    update_array_modifiers,
    update_array_operators,
    update_field_operators,
    window_functions,
    window_math,
)
from .reporter import CompatibilityReporter, reporter
from .runner import (
    cleanup_test_collections,
    run_all_comparisons,
    run_benchmark,
    run_category,
)

__all__ = [
    "CompatibilityReporter",
    "reporter",
    "run_all_comparisons",
    "run_category",
    "run_benchmark",
    "cleanup_test_collections",
    # Comparison modules (imported for side effects)
    "crud_operations",
    "query_operators",
    "expr_core_operators",
    "update_field_operators",
    "aggregation_basic_stages",
    "aggregation_advanced_operators",
    "aggregation_complex_stages",
    "aggregation_root_count_stages",
    "aggregation_cursor_properties",
    "index_operations",
    "find_and_modify",
    "bulk_operations",
    "bulk_executors",
    "distinct",
    "binary_data",
    "nested_queries",
    "raw_batches",
    "change_streams",
    "text_search",
    "project_with_unwind_fts",
    "gridfs",
    "objectid",
    "type_operator",
    "cursor_operations",
    "cursor_methods",
    "session_transactions",
    "collection_methods",
    "collection_additional",
    "database_methods",
    "mod_operator",
    "update_array_operators",
    "update_array_modifiers",
    "expr_array_object_operators",
    "expr_math_conditional",
    "expr_comprehensive",
    "expr_type_conversion_operators",
    "date_operations",
    "math_operators",
    "string_operators",
    "array_expression_operators",
    "object_field_operators",
    "search_index",
    "reindex",
    "elemmatch_operator",
    "bitwise_operators",
    "pullall_operator",
    "window_functions",
    "graphlookup",
    "fill_stage",
    "json_schema",
    "window_math",
    # Additional operator modules
    "aggregation_bucket_stages",
    "type_operators",
    "expression_operators",
    "object_inspection",
    "array_set_sampling",
    "binary_operators",
]
