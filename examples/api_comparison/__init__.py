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
    run_category("crud")
    run_category("aggregation")

    # Run benchmark
    run_benchmark(iterations=10)
"""

# Import all comparison modules to register them
# Additional operator modules (split from new_operators)
from . import (
    aggregation_additional,
    aggregation_bucket,
    aggregation_cursor,
    aggregation_stages,
    aggregation_stages_additional,
    aggregation_stages_extended,
    array_operators,
    array_operators_extended,
    binary_operations,
    binary_operators,
    bitwise_operators,
    bulk_executors,
    bulk_operations,
    change_streams,
    collection_methods,
    collection_methods_additional,
    crud,
    cursor_methods,
    cursor_operations,
    database_methods,
    date_operators,
    distinct,
    elemmatch,
    expr_additional,
    expr_complete,
    expr_extended,
    expr_operators,
    expr_success,
    expression_operators,
    fill_stage,
    find_modify,
    graph_lookup,
    gridfs_operations,
    index_operations,
    json_schema,
    math_operators,
    mod_operator,
    nested_queries,
    object_operators,
    object_operators_extended,
    objectid_ops,
    project_unwind_fts,
    pullall_operator,
    query_operators,
    raw_batches,
    reindex,
    search_index,
    session_methods,
    string_operators,
    text_search,
    type_operator,
    type_operators,
    update_additional,
    update_modifiers,
    update_operators,
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
    "project_unwind_fts",
    "gridfs_operations",
    "objectid_ops",
    "type_operator",
    "cursor_operations",
    "cursor_methods",
    "session_methods",
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
    "bitwise_operators",
    "pullall_operator",
    "window_functions",
    "graph_lookup",
    "fill_stage",
    "json_schema",
    "window_math",
    # Additional operator modules
    "aggregation_bucket",
    "type_operators",
    "expression_operators",
    "object_operators_extended",
    "array_operators_extended",
    "binary_operators",
]
