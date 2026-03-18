"""
Test Runner - Orchestrates running all comparison tests
"""

import time
from .utils import (
    test_pymongo_connection,
    enable_tier_tracking,
    disable_tier_tracking,
    get_tier_changes,
    clear_tier_changes,
)

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
from .session_methods import compare_session_methods
from .aggregation_cursor import compare_aggregation_cursor_methods
from .database_methods import compare_database_methods
from .expr_complete import compare_additional_expr_operators_complete
from .expr_success import compare_additional_expr_success_stories
from .bitwise_operators import compare_bitwise_operators
from .pullall_operator import compare_pullall_operator
from .window_functions import compare_window_functions
from .graph_lookup import compare_graph_lookup
from .fill_stage import compare_fill_stage
from .json_schema import compare_json_schema
from .window_math import compare_window_math
from .options_classes import compare_options_classes
from .aggregation_bucket import compare_bucket_aggregation
from .type_operators import compare_type_operators
from .expression_operators import compare_expression_operators
from .object_operators_extended import compare_object_operators_extended
from .array_operators_extended import compare_array_operators_extended
from .binary_operators import compare_binary_operators


# Ordered list of all comparison functions with their category names
# Format: (display_name, function)
COMPARISON_FUNCTIONS = [
    # Core Operations
    ("CRUD Operations", compare_crud_operations),
    ("Query Operators", compare_query_operators),
    ("$expr Operator", compare_expr_operator),
    ("Update Operators", compare_update_operators),
    # Aggregation
    ("Aggregation Stages", compare_aggregation_stages),
    ("Aggregation (Additional)", compare_additional_aggregation),
    (
        "Aggregation Stages (Extended)",
        compare_additional_aggregation_stages_extended,
    ),
    ("Aggregation (Extended)", compare_additional_aggregation_stages),
    ("Aggregation Cursor", compare_aggregation_cursor_methods),
    # Indexing
    ("Index Operations", compare_index_operations),
    ("Search Index", compare_search_index_operations),
    ("Reindex", compare_reindex_operation),
    # Bulk Operations
    ("Bulk Operations", compare_bulk_operations),
    ("Bulk Executors", compare_bulk_operation_executors),
    # Query Features
    ("Find & Modify", compare_find_and_modify),
    ("Distinct", compare_distinct),
    ("Nested Queries", compare_nested_field_queries),
    ("$elemMatch", compare_elemmatch_operator),
    # Cursor Operations
    ("Cursor Operations", compare_cursor_operations),
    ("Cursor Methods", compare_cursor_methods),
    # Session & Transactions
    ("Session & Transactions", compare_session_methods),
    # Data Types
    ("Binary Data", compare_binary_support),
    ("ObjectId", compare_objectid_operations),
    ("$type Operator", compare_type_operator),
    ("Date Operations", compare_date_expr_operators),
    # Aggregation Operators
    ("Math Operators", compare_math_operators),
    ("String Operators", compare_string_operators),
    ("Array Operators", compare_array_operators),
    ("Object Operators", compare_object_operators),
    ("$expr (Additional)", compare_additional_expr_operators),
    ("$expr (Extended)", compare_additional_expr_operators_extended),
    ("$expr (Complete)", compare_additional_expr_operators_complete),
    ("$expr (Success Cases)", compare_additional_expr_success_stories),
    # Advanced Features
    ("Raw Batches", compare_raw_batch_operations),
    ("Change Streams", compare_change_streams),
    ("Text Search", compare_text_search),
    ("GridFS", compare_gridfs_operations),
    ("Window Functions", compare_window_functions),
    ("Window Math", compare_window_math),
    ("$graphLookup", compare_graph_lookup),
    ("$fill Stage", compare_fill_stage),
    ("JSON Schema", compare_json_schema),
    ("Bucket Aggregation", compare_bucket_aggregation),
    # Update Operations
    ("$mod Operator", compare_mod_operator),
    ("Update (Additional)", compare_additional_update_operators),
    ("Update Modifiers", compare_update_modifiers),
    ("Bitwise Operators", compare_bitwise_operators),
    ("$pullAll Operator", compare_pullall_operator),
    # Collection & Database
    ("Collection Methods", compare_collection_methods),
    ("Collection (Additional)", compare_additional_collection_methods),
    ("Database Methods", compare_database_methods),
    # Additional Operators
    ("Type Operators", compare_type_operators),
    ("Expression Operators", compare_expression_operators),
    ("Object Operators (Extended)", compare_object_operators_extended),
    ("Array Operators (Extended)", compare_array_operators_extended),
    ("Binary Operators", compare_binary_operators),
    # Utility
    ("Options Classes", compare_options_classes),
]


class TimingContext:
    """Context manager for timing operations"""

    def __init__(self, name: str, reporter=None):
        self.name = name
        self.reporter = reporter
        self.start_time: float = 0.0
        self.timing_ms: float = 0.0

    def __enter__(self):
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.timing_ms = (time.perf_counter() - self.start_time) * 1000
        return False

    def get_timing(self):
        return self.timing_ms


def run_benchmark(iterations: int = 10, silent: bool = False):
    """Run all comparison tests as benchmark and return results"""
    import sys
    import os
    from io import StringIO
    from .reporter import BenchmarkReporter
    import neosqlite

    # Patch neosqlite.Connection to use a disk database instead of :memory:
    # This allows us to benchmark disk I/O performance without changing every test file.
    DB_PATH = "benchmark_api.db"
    original_init = neosqlite.Connection.__init__

    def patched_init(self, *args, **kwargs):
        if not args or args[0] == ":memory:":
            # Replace :memory: with our benchmark file
            new_args = (DB_PATH,) + args[1:]
            original_init(self, *new_args, **kwargs)
        else:
            original_init(self, *args, **kwargs)

    # Apply the patch
    neosqlite.Connection.__init__ = patched_init  # type: ignore[method-assign]

    def cleanup_db_files():
        for f in [DB_PATH, f"{DB_PATH}-wal", f"{DB_PATH}-shm"]:
            if os.path.exists(f):
                try:
                    os.remove(f)
                except OSError:
                    pass

    old_stdout = None

    if not silent:
        print("=" * 80)
        print("NeoSQLite vs PyMongo - Benchmark Mode (DISK I/O)")
        print(f"Iterations: {iterations}")
        print(f"Database: {DB_PATH}")
        print("=" * 80)
    else:
        devnull = StringIO()
        old_stdout = sys.stdout
        sys.stdout = devnull

    # Use the global benchmark_reporter instance so benchmark modules can mark skips
    from .reporter import benchmark_reporter as global_reporter

    if global_reporter is None:
        bench_reporter = BenchmarkReporter(iterations=iterations)
        # Update the global reference in the current module's namespace
        import sys

        sys.modules[__name__]
        reporter_mod_name = (
            __package__ + ".reporter" if __package__ else "reporter"
        )
        if reporter_mod_name in sys.modules:
            sys.modules[reporter_mod_name].benchmark_reporter = bench_reporter  # type: ignore[attr-defined]
    else:
        bench_reporter = global_reporter
        bench_reporter.results = {}
        bench_reporter.iterations = iterations

    try:
        for category, func in COMPARISON_FUNCTIONS:
            if not silent:
                print(f"\nBenchmarking: {category}")
            bench_reporter.start_category(category)

            for i in range(iterations):
                cleanup_db_files()
                cleanup_test_collections()

                from .timing import reset_timings

                reset_timings()

                try:
                    func()
                except Exception as e:
                    print(f"  Iteration {i + 1} error: {e}")
                    continue

                from .timing import get_last_neo_timing, get_last_mongo_timing

                neo_timing = get_last_neo_timing()
                mongo_timing = get_last_mongo_timing()

                if neo_timing > 0:
                    bench_reporter.record_neo_timing(category, neo_timing)
                if mongo_timing > 0:
                    bench_reporter.record_mongo_timing(category, mongo_timing)

            result = bench_reporter.results[category]
            neo_stats = result.get_neo_stats()
            mongo_stats = result.get_mongo_stats()

            # Display status with skip information
            if result.is_fully_skipped():
                if not silent:
                    print(f"  {category}: ⚠️ SKIPPED - {result.skip_reason}")
            elif result.is_partial():
                skip_side = "MongoDB" if result.mongo_skipped else "NeoSQLite"
                if not silent:
                    print(f"  {category}: ⚠️ PARTIAL ({skip_side} skipped)")
                    if result.skip_reason:
                        print(f"    Reason: {result.skip_reason}")
            else:
                if not silent:
                    print(
                        f"  {category}: NeoSQLite avg={neo_stats['avg']:.2f}ms, MongoDB avg={mongo_stats['avg']:.2f}ms"
                    )
    finally:
        # Restore original Connection.__init__
        neosqlite.Connection.__init__ = original_init  # type: ignore[method-assign]
        cleanup_db_files()

    if not silent:
        print("\n" + "=" * 80)
        print("Benchmark Complete - Generating Reports")
        print("=" * 80)

    md_file = bench_reporter.export_markdown()
    csv_file = bench_reporter.export_csv()

    if not silent:
        print(f"Markdown report: {md_file}")
        print(f"CSV report: {csv_file}")
        print("\n" + "=" * 80)
        print("Benchmark Summary")
        print("=" * 80)

    # Calculate totals excluding partial/skipped categories
    total_neo: float = 0
    total_mongo: float = 0
    valid_categories = 0
    partial_categories = 0

    for result in bench_reporter.results.values():
        if result.is_partial():
            partial_categories += 1
        elif not result.is_fully_skipped():
            total_neo += sum(result.neo_timings)
            total_mongo += sum(result.mongo_timings)
            valid_categories += 1

    speedup = total_mongo / total_neo if total_neo > 0 else 0

    if not silent:
        print(f"Total Categories: {len(bench_reporter.results)}")
        print(f"Valid Comparisons: {valid_categories}")
        if partial_categories > 0:
            print(f"Partial (one side skipped): {partial_categories}")
        print(f"Total NeoSQLite Time (valid only): {total_neo:.0f}ms")
        print(f"Total MongoDB Time (valid only): {total_mongo:.0f}ms")
        print(f"NeoSQLite Speedup (valid only): {speedup:.2f}x")
        print("=" * 80)

    if silent:
        sys.stdout = old_stdout
        print(f"Benchmark complete. Reports generated: {md_file}, {csv_file}")

    return bench_reporter


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
            print(f"\n{'=' * 80}")
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
    import neosqlite
    from .utils import (
        register_connection_for_tier_tracking,
    )

    print("NeoSQLite vs PyMongo - Comprehensive API Comparison")
    print("=" * 80)

    # Clean up any leftover test collections first
    cleanup_test_collections()

    # Patch Connection to track tier changes
    _tracker_connections = []
    original_init = neosqlite.Connection.__init__

    def patched_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        # Register this connection for tier tracking
        try:
            register_connection_for_tier_tracking(self)
            _tracker_connections.append(self)
        except Exception:
            pass

    neosqlite.Connection.__init__ = patched_init

    # Enable tier tracking
    enable_tier_tracking()

    try:
        # Run all comparison functions
        for category, func in COMPARISON_FUNCTIONS:
            print(f"\n{'=' * 80}")
            print(f"Running: {category}")
            print("=" * 80)

            # Clear tier changes before each category
            clear_tier_changes()

            try:
                func()
            except Exception as e:
                print(f"Error in {category}: {e}")
                import traceback

                traceback.print_exc()

            # Check for tier changes after each category
            tier_changes = get_tier_changes()
            if tier_changes:
                print(
                    f"\n  [TIER TRACKING] Tier changes detected: {len(tier_changes)}"
                )
                for prev, new, pipeline in tier_changes:
                    prev_str = prev if prev else "None"
                    # Show simplified pipeline
                    pipeline_summary = (
                        str(pipeline)[:60] + "..."
                        if len(str(pipeline)) > 60
                        else str(pipeline)
                    )
                    print(f"    {prev_str} -> {new}: {pipeline_summary}")

        # Print final report - this should be the last output
        from . import reporter as global_reporter

        print("\n")
        global_reporter.print_report(show_passed_results=True)

        if len(global_reporter.failed_tests) > 0:
            return 1
        return 0
    finally:
        # Restore original init and cleanup
        neosqlite.Connection.__init__ = original_init
        disable_tier_tracking()
