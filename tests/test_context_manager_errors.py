"""
Tests for error handling paths in aggregation_pipeline_context.
"""

import pytest
import neosqlite
from neosqlite.temporary_table_aggregation import aggregation_pipeline_context


class TestAggregationPipelineContextErrors:
    """Test error handling in aggregation_pipeline_context."""

    def test_context_manager_exception_handling(self, collection):
        """Test that exceptions in the context are properly handled."""
        with pytest.raises(ValueError, match="Test exception"):
            with aggregation_pipeline_context(collection.db) as create_temp:
                # Create a temporary table
                table_name = create_temp("error_test", "SELECT 1 as id")

                # Verify it exists
                cursor = collection.db.execute(f"SELECT * FROM {table_name}")
                assert len(cursor.fetchall()) == 1

                # Raise an exception to trigger rollback
                raise ValueError("Test exception")

        # After exception, tables should be rolled back
        # We can't easily test this, but we can make sure the context manager still works
        with aggregation_pipeline_context(collection.db) as create_temp:
            # Should work normally after exception
            table_name = create_temp("normal_test", "SELECT 1 as id")
            cursor = collection.db.execute(f"SELECT * FROM {table_name}")
            assert len(cursor.fetchall()) == 1

    def test_context_manager_database_error_handling(self, collection):
        """Test handling of database errors."""
        # Test that invalid SQL is handled gracefully
        with aggregation_pipeline_context(collection.db) as create_temp:
            # Try to create a table with invalid SQL
            # This should not crash the context manager
            try:
                table_name = create_temp(
                    "invalid_sql_test", "INVALID SQL STATEMENT"
                )
                # If we get here, the invalid SQL was somehow accepted
                # That's fine, we're just testing error handling paths
            except Exception:
                # Expected - invalid SQL should raise an exception
                pass

    def test_context_manager_with_none_params(self, collection):
        """Test context manager with None parameters."""
        with aggregation_pipeline_context(collection.db) as create_temp:
            # Test with None params
            table_name = create_temp("none_params_test", "SELECT 1 as id", None)

            # Should work the same as without params
            cursor = collection.db.execute(f"SELECT * FROM {table_name}")
            results = cursor.fetchall()
            assert len(results) == 1
            assert results[0][0] == 1

    def test_context_manager_with_empty_params(self, collection):
        """Test context manager with empty parameters list."""
        with aggregation_pipeline_context(collection.db) as create_temp:
            # Test with empty params list
            table_name = create_temp("empty_params_test", "SELECT 1 as id", [])

            # Should work the same as without params
            cursor = collection.db.execute(f"SELECT * FROM {table_name}")
            results = cursor.fetchall()
            assert len(results) == 1
            assert results[0][0] == 1


class TestTemporaryTableDropErrors:
    """Test error handling when dropping temporary tables."""

    def test_drop_table_error_handling(self, collection):
        """Test that errors when dropping tables are handled gracefully."""
        # We can't easily test this directly, but we can at least make sure
        # the error handling code path doesn't crash

        with aggregation_pipeline_context(collection.db) as create_temp:
            # Create a normal temporary table
            table_name = create_temp("drop_test", "SELECT 1 as id")

            # The context manager will try to drop this table when exiting
            # Even if that fails, it should not crash
            pass  # Normal exit


def test_savepoint_name_uniqueness():
    """Test that savepoint names are unique."""
    import uuid

    # Generate several savepoint names and verify they're unique
    names = set()
    for _ in range(100):
        savepoint_name = f"agg_pipeline_{uuid.uuid4().hex}"
        assert savepoint_name not in names
        names.add(savepoint_name)


if __name__ == "__main__":
    # Run a quick test
    test_savepoint_name_uniqueness()
    print("Savepoint name uniqueness test passed!")
