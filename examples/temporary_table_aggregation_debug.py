#!/usr/bin/env python3
"""
Debug/test file for temporary table aggregation pipeline approach.
This file tests the concept of using temporary tables to process complex aggregation pipelines.
"""

import sys
import os
import uuid
from contextlib import contextmanager

# Add the neosqlite package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import neosqlite


@contextmanager
def aggregation_pipeline_context(db_connection):
    """Context manager for temporary aggregation tables with automatic cleanup."""
    temp_tables = []
    savepoint_name = f"agg_pipeline_{uuid.uuid4().hex}"

    # Create savepoint for atomicity
    db_connection.execute(f"SAVEPOINT {savepoint_name}")

    def create_temp_table(name_suffix, query, params=None):
        """Create a temporary table for pipeline processing."""
        table_name = f"temp_{name_suffix}_{uuid.uuid4().hex}"
        print(f"Creating temporary table: {table_name}")
        if params:
            db_connection.execute(
                f"CREATE TEMP TABLE {table_name} AS {query}", params
            )
        else:
            db_connection.execute(f"CREATE TEMP TABLE {table_name} AS {query}")
        temp_tables.append(table_name)
        return table_name

    try:
        yield create_temp_table
    except Exception as e:
        print(f"Error occurred: {e}")
        # Rollback on error
        db_connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
        raise
    finally:
        # Cleanup
        print(f"Releasing savepoint: {savepoint_name}")
        db_connection.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        # Explicitly drop temp tables
        for table_name in temp_tables:
            try:
                print(f"Dropping temporary table: {table_name}")
                db_connection.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception as e:
                print(f"Error dropping table {table_name}: {e}")


def test_temporary_table_approach():
    """Test the temporary table approach with a simple pipeline."""
    print("=== Testing Temporary Table Aggregation Pipeline Approach ===\n")

    # Create an in-memory database
    with neosqlite.Connection(":memory:") as conn:
        # Get a collection
        users = conn.users

        # Insert sample data
        print("1. Inserting sample data...")
        sample_docs = [
            {
                "name": "Alice",
                "age": 30,
                "tags": ["python", "javascript"],
                "status": "active",
            },
            {
                "name": "Bob",
                "age": 25,
                "tags": ["java", "python"],
                "status": "active",
            },
            {
                "name": "Charlie",
                "age": 35,
                "tags": ["javascript", "go"],
                "status": "inactive",
            },
            {
                "name": "Diana",
                "age": 28,
                "tags": ["python", "rust"],
                "status": "active",
            },
        ]
        users.insert_many(sample_docs)
        print(f"   Inserted {len(sample_docs)} documents\n")

        # Test the temporary table approach with a simple pipeline
        print("2. Testing temporary table approach with $match + $unwind...")
        pipeline = [{"$match": {"status": "active"}}, {"$unwind": "$tags"}]

        # Process using temporary tables
        try:
            with aggregation_pipeline_context(users.db) as create_temp:
                # Step 1: Create base table with all documents
                base_table = create_temp(
                    "base", f"SELECT id, _id, data FROM {users.name}"
                )
                print(f"   Base table created: {base_table}")

                # Step 2: Create filtered table with matched documents
                match_table = create_temp(
                    "matched",
                    f"SELECT * FROM {base_table} WHERE json_extract(data, '$.status') = ?",
                    ["active"],
                )
                print(f"   Matched table created: {match_table}")

                # Step 3: Create unwound table
                unwind_table = create_temp(
                    "unwound",
                    f"""
                    SELECT {users.name}.id,
                           {users.name}._id,
                           json_set({users.name}.data, '$."tags"', je.value) as data
                    FROM {match_table} as {users.name},
                         json_each(json_extract({users.name}.data, '$.tags')) as je
                    """,
                )
                print(f"   Unwound table created: {unwind_table}")

                # Get final results
                print("   Getting final results...")
                cursor = users.db.execute(f"SELECT * FROM {unwind_table}")
                results = []
                for row in cursor.fetchall():
                    doc = users._load_with_stored_id(row[0], row[2], row[1])
                    results.append(doc)

                print(f"   Final results count: {len(results)}")
                for doc in results:
                    print(f"     {doc['name']}: {doc['tags']}")

        except Exception as e:
            print(f"Error in temporary table approach: {e}")
            import traceback

            traceback.print_exc()

        print("\n3. Comparing with standard approach...")
        # Compare with standard approach
        try:
            standard_results = list(users.aggregate(pipeline))
            print(f"   Standard results count: {len(standard_results)}")
            for doc in standard_results:
                print(f"     {doc['name']}: {doc['tags']}")
        except Exception as e:
            print(f"Error in standard approach: {e}")
            import traceback

            traceback.print_exc()

        # Test a more complex pipeline
        print("\n4. Testing more complex pipeline...")
        complex_pipeline = [
            {"$match": {"status": "active"}},
            {"$unwind": "$tags"},
            {"$sort": {"tags": 1}},
            {"$limit": 5},
        ]

        try:
            with aggregation_pipeline_context(users.db) as create_temp:
                # Step 1: Create base table
                base_table = create_temp(
                    "base", f"SELECT id, _id, data FROM {users.name}"
                )

                # Step 2: Create filtered table
                match_table = create_temp(
                    "matched",
                    f"SELECT * FROM {base_table} WHERE json_extract(data, '$.status') = ?",
                    ["active"],
                )

                # Step 3: Create unwound and sorted/limited table
                final_table = create_temp(
                    "final",
                    f"""
                    SELECT {users.name}.id,
                           {users.name}._id,
                           json_set({users.name}.data, '$."tags"', je.value) as data
                    FROM {match_table} as {users.name},
                         json_each(json_extract({users.name}.data, '$.tags')) as je
                    ORDER BY je.value ASC
                    LIMIT 5
                    """,
                )

                # Get final results
                cursor = users.db.execute(f"SELECT * FROM {final_table}")
                results = []
                for row in cursor.fetchall():
                    doc = users._load_with_stored_id(row[0], row[2], row[1])
                    results.append(doc)

                print(f"   Complex pipeline results count: {len(results)}")
                for doc in results:
                    print(f"     {doc['name']}: {doc['tags']}")

        except Exception as e:
            print(f"Error in complex pipeline temporary table approach: {e}")
            import traceback

            traceback.print_exc()

        print("\n5. Comparing complex pipeline with standard approach...")
        try:
            standard_results = list(users.aggregate(complex_pipeline))
            print(f"   Standard complex results count: {len(standard_results)}")
            for doc in standard_results:
                print(f"     {doc['name']}: {doc['tags']}")
        except Exception as e:
            print(f"Error in standard complex approach: {e}")
            import traceback

            traceback.print_exc()


if __name__ == "__main__":
    test_temporary_table_approach()
    print("\n=== Test Complete ===")
