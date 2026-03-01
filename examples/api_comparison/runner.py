"""
Test Runner - Orchestrates running all comparison tests
"""

from .utils import test_pymongo_connection

# Import all comparison functions
from .crud import compare_crud_operations
from .query_operators import compare_query_operators
from .expr_operators import compare_expr_operator
from .update_operators import compare_update_operators
from .aggregation_stages import compare_aggregation_stages
from .index_operations import compare_index_operations
from .find_modify import compare_find_and_modify
from .bulk_operations import compare_bulk_operations
from .distinct import compare_distinct
from .binary_operations import compare_binary_support
from .nested_queries import compare_nested_field_queries
from .raw_batches import compare_raw_batch_operations
from .change_streams import compare_change_streams
from .text_search import compare_text_search
from .gridfs_operations import compare_gridfs_operations
from .objectid_ops import compare_objectid_operations
from .type_operator import compare_type_operator
from .aggregation_additional import compare_additional_aggregation
from .cursor_operations import compare_cursor_operations
from .mod_operator import compare_mod_operator
from .update_additional import compare_additional_update_operators
from .aggregation_stages_additional import compare_additional_aggregation_stages
from .expr_additional import compare_additional_expr_operators
from .collection_methods import compare_collection_methods
from .date_operators import compare_date_expr_operators
from .math_operators import compare_math_operators
from .string_operators import compare_string_operators
from .array_operators import compare_array_operators
from .object_operators import compare_object_operators
from .collection_methods_additional import compare_additional_collection_methods
from .search_index import compare_search_index_operations
from .bulk_executors import compare_bulk_operation_executors
from .reindex import compare_reindex_operation
from .elemmatch import compare_elemmatch_operator
from .update_modifiers import compare_update_modifiers
from .aggregation_stages_extended import (
    compare_additional_aggregation_stages_extended,
)
from .expr_extended import compare_additional_expr_operators_extended
from .cursor_methods import compare_cursor_methods
from .aggregation_cursor import compare_aggregation_cursor_methods
from .database_methods import compare_database_methods
from .expr_complete import compare_additional_expr_operators_complete
from .expr_success import compare_additional_expr_success_stories


# Ordered list of all comparison functions with their category names
COMPARISON_FUNCTIONS = [
    ("crud", compare_crud_operations),
    ("query", compare_query_operators),
    ("expr", compare_expr_operator),
    ("update", compare_update_operators),
    ("aggregation_stages", compare_aggregation_stages),
    ("index", compare_index_operations),
    ("find_modify", compare_find_and_modify),
    ("bulk", compare_bulk_operations),
    ("distinct", compare_distinct),
    ("binary", compare_binary_support),
    ("nested", compare_nested_field_queries),
    ("raw_batches", compare_raw_batch_operations),
    ("change_streams", compare_change_streams),
    ("text", compare_text_search),
    ("gridfs", compare_gridfs_operations),
    ("objectid", compare_objectid_operations),
    ("type", compare_type_operator),
    ("aggregation_additional", compare_additional_aggregation),
    ("cursor", compare_cursor_operations),
    ("mod", compare_mod_operator),
    ("update_additional", compare_additional_update_operators),
    ("aggregation_stages_additional", compare_additional_aggregation_stages),
    ("expr_additional", compare_additional_expr_operators),
    ("collection_methods", compare_collection_methods),
    ("date", compare_date_expr_operators),
    ("math", compare_math_operators),
    ("string", compare_string_operators),
    ("array", compare_array_operators),
    ("object", compare_object_operators),
    ("collection_additional", compare_additional_collection_methods),
    ("search_index", compare_search_index_operations),
    ("bulk_executors", compare_bulk_operation_executors),
    ("reindex", compare_reindex_operation),
    ("elemmatch", compare_elemmatch_operator),
    ("update_modifiers", compare_update_modifiers),
    ("aggregation_extended", compare_additional_aggregation_stages_extended),
    ("expr_extended", compare_additional_expr_operators_extended),
    ("cursor_methods", compare_cursor_methods),
    ("aggregation_cursor", compare_aggregation_cursor_methods),
    ("database", compare_database_methods),
    ("expr_complete", compare_additional_expr_operators_complete),
    ("expr_success", compare_additional_expr_success_stories),
]


def cleanup_test_collections():
    """Clean up test collections from previous runs"""
    print("Cleaning up test collections...")

    # Clean up NeoSQLite
    try:
        import neosqlite

        with neosqlite.Connection(":memory:") as neo_conn:
            # In-memory DB auto-cleans, but we can drop collections explicitly
            test_collections = [
                "test_collection",
                "test_agg_cursor",
                "test_agg_stages",
                "users",
                "products",
                "items",
                "test_index",
                "test_bulk",
                "test_distinct",
                "test_binary",
                "test_nested",
                "test_raw",
                "test_text",
                "test_gridfs",
                "test_objectid",
                "test_type",
                "test_unwind",
                "test_facet",
                "test_lookup",
                "test_sample",
                "test_push",
                "test_addtoset",
                "test_pull",
                "test_pop",
                "test_currentdate",
                "test_nested",
                "test_raw_batches",
                "test_text",
                "test_binary",
                "test_distinct",
                "test_bulk",
                "test_find_modify",
                "test_index",
                "test_agg",
                "test_update",
                "test_query",
                "test_expr",
                "test_crud",
            ]
            for coll_name in test_collections:
                try:
                    neo_conn.drop_collection(coll_name)
                except Exception:
                    pass
            print("NeoSQLite: Cleaned up common test collections")
    except Exception as e:
        print(f"NeoSQLite: Cleanup skipped - {e}")

    # Clean up MongoDB
    client = test_pymongo_connection()
    if client:
        try:
            db = client.test_database
            test_collections = [
                "test_collection",
                "test_agg_cursor",
                "test_agg_stages",
                "users",
                "products",
                "items",
                "test_index",
                "test_bulk",
                "test_distinct",
                "test_binary",
                "test_nested",
                "test_raw",
                "test_text",
                "test_gridfs",
                "test_objectid",
                "test_type",
                "test_unwind",
                "test_facet",
                "test_lookup",
                "test_sample",
                "test_push",
                "test_addtoset",
                "test_pull",
                "test_pop",
                "test_currentdate",
                "test_nested",
                "test_raw_batches",
                "test_text",
                "test_binary",
                "test_distinct",
                "test_bulk",
                "test_find_modify",
                "test_index",
                "test_agg",
                "test_update",
                "test_query",
                "test_expr",
                "test_crud",
            ]
            for coll_name in test_collections:
                try:
                    db.drop_collection(coll_name)
                except Exception:
                    pass
            print("MongoDB [test_database]: Cleaned up common test collections")
        except Exception as e:
            print(f"MongoDB [test_database]: Cleanup skipped - {e}")

        client.close()
        print("Cleanup completed\n")


def run_category(category: str) -> bool:
    """
    Run a specific category of comparison tests.

    Args:
        category: The category name (e.g., 'crud', 'aggregation', 'expr')

    Returns:
        True if all tests passed, False otherwise
    """
    for cat_name, func in COMPARISON_FUNCTIONS:
        if cat_name == category or category in cat_name:
            print(f"\n{'='*80}")
            print(f"Running: {cat_name}")
            print("=" * 80)
            try:
                func()
                return True
            except Exception as e:
                print(f"Error running {cat_name}: {e}")
                return False

    print(f"Unknown category: {category}")
    print(
        f"Available categories: {', '.join(name for name, _ in COMPARISON_FUNCTIONS)}"
    )
    return False


def run_all_comparisons():
    """Run all comparison tests and return exit code"""
    print("NeoSQLite vs PyMongo - Comprehensive API Comparison")
    print("=" * 80)

    # Clean up any leftover test collections first
    cleanup_test_collections()

    # Run all comparison functions
    for category, func in COMPARISON_FUNCTIONS:
        print(f"\n{'='*80}")
        print(f"Running: {category}")
        print("=" * 80)
        try:
            func()
        except Exception as e:
            print(f"Error in {category}: {e}")
            import traceback

            traceback.print_exc()

    # Print final report
    from . import reporter as global_reporter

    global_reporter.print_report()

    if len(global_reporter.failed_tests) > 0:
        return 1
    return 0
