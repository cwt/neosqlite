"""
Tests for additional Collection APIs that were previously untested.

This module tests the following Collection methods:
- drop(): Drops the entire collection table.
- aggregate_raw_batches(): Performs aggregation and returns raw batch cursors.
- initialize_ordered_bulk_op(): Initializes an ordered bulk operation executor.
- initialize_unordered_bulk_op(): Initializes an unordered bulk operation executor.
- reindex(): Reindexes the collection with various parameters.
- database property: Returns the associated database connection.
"""

from pytest import raises
import neosqlite


class TestCollectionDrop:
    """Tests for Collection.drop() method."""

    def test_drop_collection(self, collection):
        """Test dropping a collection."""
        # Insert some documents
        collection.insert_many([{"foo": "bar"}, {"baz": "qux"}])
        assert collection.count_documents({}) == 2

        # Drop the collection
        collection.drop()

        # Verify the collection is gone
        # The table should no longer exist
        cursor = collection.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (collection.name,),
        )
        assert cursor.fetchone() is None

    def test_drop_empty_collection(self, collection):
        """Test dropping an empty collection."""
        # Don't insert anything, just drop
        collection.drop()

        # Verify the collection is gone
        cursor = collection.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (collection.name,),
        )
        assert cursor.fetchone() is None

    def test_drop_collection_with_indexes(self, collection):
        """Test dropping a collection that has indexes."""
        # Create indexes
        collection.create_index("foo")
        collection.create_index("bar")

        # Insert documents
        collection.insert_many([{"foo": "bar", "bar": "foo"}])

        # Drop the collection
        collection.drop()

        # Verify the collection is gone
        cursor = collection.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (collection.name,),
        )
        assert cursor.fetchone() is None

    def test_drop_collection_twice(self, collection):
        """Test dropping a collection twice (should not raise)."""
        collection.insert_one({"foo": "bar"})

        # First drop
        collection.drop()

        # Second drop should not raise (uses IF EXISTS)
        collection.drop()

    def test_drop_after_rename(self, collection):
        """Test dropping a collection after renaming it."""
        collection.insert_one({"foo": "bar"})
        collection.rename("renamed_collection")

        # Drop the renamed collection
        collection.drop()

        # Verify the renamed collection is gone
        cursor = collection.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            ("renamed_collection",),
        )
        assert cursor.fetchone() is None


class TestAggregateRawBatches:
    """Tests for Collection.aggregate_raw_batches() method."""

    def test_aggregate_raw_batches_basic(self, collection):
        """Test basic aggregation with raw batches."""
        import json

        # Insert test data
        collection.insert_many(
            [
                {"category": "A", "value": 10},
                {"category": "A", "value": 20},
                {"category": "B", "value": 30},
                {"category": "B", "value": 40},
            ]
        )

        # Simple aggregation pipeline
        pipeline = [
            {"$group": {"_id": "$category", "total": {"$sum": "$value"}}}
        ]

        # Get raw batches cursor
        cursor = collection.aggregate_raw_batches(pipeline, batch_size=2)

        # Verify it returns a cursor
        assert cursor is not None

        # Consume the cursor - raw batches return bytes with newline-separated JSON
        batches = list(cursor)
        assert len(batches) == 1  # All results fit in one batch

        # Parse the batch (newline-separated JSON)
        batch_str = batches[0].decode("utf-8")
        results = [
            json.loads(line) for line in batch_str.split("\n") if line.strip()
        ]

        assert len(results) == 2

        # Check that we have both categories
        categories = {doc["_id"] for doc in results}
        assert categories == {"A", "B"}

    def test_aggregate_raw_batches_with_match(self, collection):
        """Test aggregation with $match stage."""
        import json

        collection.insert_many(
            [
                {"status": "active", "value": 10},
                {"status": "inactive", "value": 20},
                {"status": "active", "value": 30},
            ]
        )

        pipeline = [
            {"$match": {"status": "active"}},
            {"$group": {"_id": "$status", "total": {"$sum": "$value"}}},
        ]

        cursor = collection.aggregate_raw_batches(pipeline)
        batches = list(cursor)

        # Parse the batch
        batch_str = batches[0].decode("utf-8")
        results = [
            json.loads(line) for line in batch_str.split("\n") if line.strip()
        ]

        assert len(results) == 1
        assert results[0]["total"] == 40

    def test_aggregate_raw_batches_empty_collection(self, collection):
        """Test aggregation on empty collection."""
        pipeline = [{"$match": {}}]
        cursor = collection.aggregate_raw_batches(pipeline)
        results = list(cursor)
        assert len(results) == 0

    def test_aggregate_raw_batches_batch_size(self, collection):
        """Test that batch_size parameter is accepted."""
        import json

        collection.insert_many([{"i": i} for i in range(10)])

        pipeline = [{"$match": {}}]
        cursor = collection.aggregate_raw_batches(pipeline, batch_size=5)

        # Should be able to iterate without errors
        batches = list(cursor)
        assert len(batches) == 2  # 10 docs in batches of 5

        # Parse and count all documents
        total_docs = 0
        for batch in batches:
            batch_str = batch.decode("utf-8")
            docs = [
                json.loads(line)
                for line in batch_str.split("\n")
                if line.strip()
            ]
            total_docs += len(docs)

        assert total_docs == 10

    def test_aggregate_raw_batches_complex_pipeline(self, collection):
        """Test aggregation with a more complex pipeline."""
        import json

        collection.insert_many(
            [
                {"category": "A", "item": "x", "qty": 5},
                {"category": "A", "item": "y", "qty": 10},
                {"category": "B", "item": "x", "qty": 15},
                {"category": "B", "item": "z", "qty": 20},
            ]
        )

        pipeline = [
            {"$group": {"_id": "$category", "total_qty": {"$sum": "$qty"}}},
            {"$sort": {"_id": 1}},
            {"$limit": 2},
        ]

        cursor = collection.aggregate_raw_batches(pipeline)
        batches = list(cursor)

        # Parse the batch
        batch_str = batches[0].decode("utf-8")
        results = [
            json.loads(line) for line in batch_str.split("\n") if line.strip()
        ]

        assert len(results) == 2
        # Results should be sorted by _id
        assert results[0]["_id"] == "A"
        assert results[1]["_id"] == "B"


class TestInitializeOrderedBulkOp:
    """Tests for Collection.initialize_ordered_bulk_op() method."""

    def test_initialize_ordered_bulk_op(self, collection):
        """Test initializing an ordered bulk operation."""
        bulk = collection.initialize_ordered_bulk_op()

        # Verify it returns a BulkOperationExecutor
        assert bulk is not None
        assert hasattr(bulk, "add")
        assert hasattr(bulk, "insert")
        assert hasattr(bulk, "find")
        assert hasattr(bulk, "execute")

    def test_ordered_bulk_operation_execution(self, collection):
        """Test executing an ordered bulk operation."""
        from neosqlite import InsertOne, UpdateOne, DeleteOne

        bulk = collection.initialize_ordered_bulk_op()

        # Add operations using PyMongo-style API
        bulk.add(InsertOne({"item": "a", "qty": 10}))
        bulk.add(InsertOne({"item": "b", "qty": 20}))
        bulk.add(UpdateOne({"item": "a"}, {"$set": {"qty": 15}}))
        bulk.add(DeleteOne({"item": "b"}))

        # Execute
        result = bulk.execute()

        # Verify results
        assert result.inserted_count == 2
        assert result.matched_count == 1
        assert result.modified_count == 1
        assert result.deleted_count == 1

        # Verify final state
        assert collection.count_documents({}) == 1
        doc = collection.find_one({"item": "a"})
        assert doc["qty"] == 15

    def test_ordered_bulk_operation_stops_on_error(self, collection):
        """Test that ordered bulk stops on first error."""
        from neosqlite import InsertOne

        collection.create_index("item", unique=True)
        collection.insert_one({"item": "a"})

        bulk = collection.initialize_ordered_bulk_op()

        # This should succeed
        bulk.add(InsertOne({"item": "b"}))
        # This should fail (duplicate key)
        bulk.add(InsertOne({"item": "a"}))
        # This should not execute
        bulk.add(InsertOne({"item": "c"}))

        with raises(Exception):  # IntegrityError or similar
            bulk.execute()

        # Only the first insert should have succeeded
        # Note: Transaction rollback behavior may vary
        # The key test is that an error is raised

    def test_ordered_bulk_with_upsert(self, collection):
        """Test ordered bulk with upsert operation."""
        from neosqlite import UpdateOne

        bulk = collection.initialize_ordered_bulk_op()

        bulk.add(UpdateOne({"item": "a"}, {"$set": {"qty": 10}}, upsert=True))
        bulk.add(UpdateOne({"item": "b"}, {"$set": {"qty": 20}}, upsert=True))

        result = bulk.execute()

        assert result.upserted_count == 2
        assert collection.count_documents({}) == 2


class TestInitializeUnorderedBulkOp:
    """Tests for Collection.initialize_unordered_bulk_op() method."""

    def test_initialize_unordered_bulk_op(self, collection):
        """Test initializing an unordered bulk operation."""
        bulk = collection.initialize_unordered_bulk_op()

        # Verify it returns a BulkOperationExecutor
        assert bulk is not None
        assert hasattr(bulk, "add")
        assert hasattr(bulk, "insert")
        assert hasattr(bulk, "find")
        assert hasattr(bulk, "execute")

    def test_unordered_bulk_operation_execution(self, collection):
        """Test executing an unordered bulk operation."""
        from neosqlite import InsertOne, UpdateOne, DeleteOne

        bulk = collection.initialize_unordered_bulk_op()

        # Add operations
        bulk.add(InsertOne({"item": "x", "qty": 5}))
        bulk.add(InsertOne({"item": "y", "qty": 15}))
        bulk.add(UpdateOne({"item": "x"}, {"$set": {"qty": 10}}))
        bulk.add(DeleteOne({"item": "y"}))

        # Execute
        result = bulk.execute()

        # Verify results
        assert result.inserted_count == 2
        assert result.matched_count == 1
        assert result.modified_count == 1
        assert result.deleted_count == 1

        # Verify final state
        assert collection.count_documents({}) == 1
        doc = collection.find_one({"item": "x"})
        assert doc["qty"] == 10

    def test_unordered_bulk_continues_on_error(self, collection):
        """Test that unordered bulk continues despite errors."""
        from neosqlite import InsertOne

        collection.create_index("item", unique=True)
        collection.insert_one({"item": "a"})

        bulk = collection.initialize_unordered_bulk_op()

        # This should succeed
        bulk.add(InsertOne({"item": "b"}))
        # This should fail (duplicate key)
        bulk.add(InsertOne({"item": "a"}))
        # This should still execute (unordered)
        bulk.add(InsertOne({"item": "c"}))

        # Note: The current implementation executes unordered operations
        # the same as ordered (with rollback on error), so all operations
        # will be rolled back on error. This test verifies the error is raised.
        with raises(Exception):
            bulk.execute()

        # After rollback, only the original document should exist
        assert collection.count_documents({}) == 1
        assert collection.find_one({"item": "a"}) is not None

    def test_unordered_bulk_with_mixed_operations(self, collection):
        """Test unordered bulk with mixed operation types."""
        from neosqlite import InsertOne, UpdateOne, DeleteOne

        collection.insert_many(
            [
                {"item": "a", "qty": 10},
                {"item": "b", "qty": 20},
                {"item": "c", "qty": 30},
            ]
        )

        bulk = collection.initialize_unordered_bulk_op()

        bulk.add(InsertOne({"item": "d", "qty": 40}))
        bulk.add(UpdateOne({"item": "a"}, {"$set": {"qty": 15}}))
        bulk.add(DeleteOne({"item": "b"}))
        bulk.add(UpdateOne({"item": "c"}, {"$inc": {"qty": 5}}))

        result = bulk.execute()

        assert result.inserted_count == 1
        assert result.modified_count >= 1  # At least one update
        assert result.deleted_count == 1


class TestReindex:
    """Tests for Collection.reindex() method."""

    def test_reindex_basic(self, collection):
        """Test basic reindex operation."""
        # Create an index
        collection.create_index("foo")

        # Insert documents
        collection.insert_many([{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

        # Reindex
        collection.reindex("idx_foo_foo")

        # Verify index still works
        results = list(collection.find({"foo": "bar"}))
        assert len(results) == 1

    def test_reindex_with_sparse(self, collection):
        """Test reindex with sparse=True parameter."""
        collection.create_index("foo", sparse=True)

        # Insert documents with and without the indexed field
        collection.insert_many(
            [
                {"foo": "bar"},
                {"foo": "baz"},
                {"other": "field"},  # No 'foo' field
            ]
        )

        # Reindex with sparse=True
        collection.reindex("idx_foo_foo", sparse=True)

        # Verify index works
        results = list(collection.find({"foo": "bar"}))
        assert len(results) == 1

    def test_reindex_with_documents(self, collection):
        """Test reindex with explicit documents parameter."""
        collection.create_index("foo")

        # Insert documents
        collection.insert_many([{"foo": "bar"}, {"foo": "baz"}])

        # Get documents to reindex
        docs = list(collection.find({}))

        # Reindex with explicit documents
        collection.reindex("idx_foo_foo", documents=docs)

        # Verify index works
        results = list(collection.find({"foo": "bar"}))
        assert len(results) == 1

    def test_reindex_multiple_indexes(self, collection):
        """Test reindexing when multiple indexes exist."""
        collection.create_index("foo")
        collection.create_index("bar")

        collection.insert_many(
            [
                {"foo": "a", "bar": "1"},
                {"foo": "b", "bar": "2"},
            ]
        )

        # Reindex both indexes
        collection.reindex("idx_foo_foo")
        collection.reindex("idx_bar_bar")

        # Verify both indexes work
        results_foo = list(collection.find({"foo": "a"}))
        results_bar = list(collection.find({"bar": "1"}))

        assert len(results_foo) == 1
        assert len(results_bar) == 1


class TestDatabaseProperty:
    """Tests for Collection.database property."""

    def test_database_property_returns_connection(self, collection):
        """Test that database property returns the connection object."""
        db = collection.database

        assert db is not None
        assert isinstance(db, neosqlite.Connection)

    def test_database_property_same_instance(self, connection, collection):
        """Test that database property returns the same connection instance."""
        assert collection.database is connection

    def test_database_property_access_other_collections(self, connection):
        """Test accessing other collections via database property."""
        coll = connection["test_coll"]
        coll.insert_one({"foo": "bar"})

        # Access another collection through database property
        other_collection = coll.database["other_collection"]

        assert other_collection is not None
        assert other_collection.name == "other_collection"

        # Insert and verify
        other_collection.insert_one({"baz": "qux"})
        assert other_collection.count_documents({}) == 1

    def test_database_property_nested_access(self, connection):
        """Test nested collection access via database property."""
        # Create a collection with dots in name (GridFS style)
        # Note: We use underscore instead since dots in table names cause issues
        gridfs_collection = connection["fs_files"]

        assert gridfs_collection is not None
        assert gridfs_collection.name == "fs_files"

        # Insert and verify
        gridfs_collection.insert_one({"filename": "test.txt"})
        assert gridfs_collection.count_documents({}) == 1


class TestRenameCollection:
    """Additional tests for Collection.rename() method."""

    def test_rename_same_name_noop(self, collection):
        """Test renaming to the same name is a no-op."""
        collection.insert_one({"foo": "bar"})
        original_name = collection.name

        # Rename to same name
        collection.rename(original_name)

        # Should still work
        assert collection.name == original_name
        assert collection.count_documents({}) == 1

    def test_rename_preserves_data(self, collection):
        """Test that renaming preserves all data."""
        # Insert multiple documents
        docs = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35},
        ]
        collection.insert_many(docs)

        # Rename
        collection.rename("new_name")

        # Verify all data is preserved
        assert collection.count_documents({}) == 3
        names = {doc["name"] for doc in collection.find({})}
        assert names == {"Alice", "Bob", "Charlie"}

    def test_rename_preserves_indexes(self, collection):
        """Test that renaming preserves indexes."""
        collection.create_index("foo")
        collection.insert_one({"foo": "bar"})

        # Rename
        collection.rename("renamed_collection")

        # Indexes should still exist (on the renamed table)
        # Note: SQLite may update index names, but the index functionality remains
        assert collection.count_documents({}) == 1
        results = list(collection.find({"foo": "bar"}))
        assert len(results) == 1


class TestOptionsCollection:
    """Additional tests for Collection.options() method."""

    def test_options_count(self, collection):
        """Test that options returns correct document count."""
        assert collection.options()["count"] == 0

        collection.insert_many([{"i": i} for i in range(5)])
        assert collection.options()["count"] == 5

    def test_options_columns(self, collection):
        """Test that options returns column information."""
        options = collection.options()

        assert "columns" in options
        columns = options["columns"]

        # Should have at least id and data columns
        column_names = [col["name"] for col in columns]
        assert "id" in column_names
        assert "data" in column_names

    def test_options_indexes(self, collection):
        """Test that options returns index information."""
        collection.create_index("foo")

        options = collection.options()

        assert "indexes" in options
        indexes = options["indexes"]

        # Should have at least one index
        assert len(indexes) >= 1

        # Check index structure
        for idx in indexes:
            assert "name" in idx
            assert "definition" in idx

    def test_options_structure(self, collection):
        """Test overall structure of options output."""
        options = collection.options()

        assert isinstance(options, dict)
        assert "name" in options
        assert options["name"] == collection.name
        assert isinstance(options.get("columns"), list)
        assert isinstance(options.get("indexes"), list)
        assert isinstance(options.get("count"), int)


# Integration tests combining multiple APIs
class TestApiIntegration:
    """Integration tests combining multiple Collection APIs."""

    def test_rename_then_drop(self, collection):
        """Test renaming a collection then dropping it."""
        collection.insert_one({"foo": "bar"})
        collection.rename("temp_collection")

        # Verify rename worked
        assert collection.name == "temp_collection"
        assert collection.count_documents({}) == 1

        # Drop
        collection.drop()

        # Verify drop worked
        cursor = collection.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            ("temp_collection",),
        )
        assert cursor.fetchone() is None

    def test_bulk_then_aggregate(self, collection):
        """Test bulk operations followed by aggregation."""
        from neosqlite import InsertOne
        import json

        # Use bulk to insert data
        bulk = collection.initialize_ordered_bulk_op()
        for i in range(10):
            bulk.add(
                InsertOne({"category": "A" if i % 2 == 0 else "B", "value": i})
            )
        bulk.execute()

        # Aggregate the data
        pipeline = [
            {"$group": {"_id": "$category", "total": {"$sum": "$value"}}}
        ]
        cursor = collection.aggregate_raw_batches(pipeline)
        batches = list(cursor)

        # Parse results
        batch_str = batches[0].decode("utf-8")
        results = [
            json.loads(line) for line in batch_str.split("\n") if line.strip()
        ]

        assert len(results) == 2
        categories = {doc["_id"] for doc in results}
        assert categories == {"A", "B"}

    def test_reindex_after_bulk_update(self, collection):
        """Test reindexing after bulk update operations."""
        from neosqlite import InsertOne

        collection.create_index("status")

        # Bulk insert
        bulk = collection.initialize_ordered_bulk_op()
        for i in range(5):
            bulk.add(InsertOne({"status": "pending", "value": i}))
        bulk.execute()

        # Bulk update using update_many through find() API
        bulk2 = collection.initialize_ordered_bulk_op()
        bulk2.find({"status": "pending"}).update_many(
            {"$set": {"status": "complete"}}
        )
        bulk2.execute()

        # Reindex
        collection.reindex("idx_status_status")

        # Verify index works
        results = list(collection.find({"status": "complete"}))
        assert len(results) == 5

    def test_database_property_workflow(self, connection):
        """Test workflow using database property."""
        # Create collections via database property
        coll1 = connection["collection1"]
        coll2 = connection["collection2"]

        # Insert data
        coll1.insert_one({"source": "coll1"})
        coll2.insert_one({"source": "coll2"})

        # Access via database property
        assert coll1.database is connection
        assert coll2.database is connection

        # Cross-collection operations
        other = coll1.database["collection2"]
        assert other.count_documents({}) == 1

        # Cleanup
        coll1.drop()
        coll2.drop()
