#!/usr/bin/env python3
"""
Simple test to verify the AggregationCursor implementation
"""
import neosqlite


def test_aggregation_cursor():
    """Test the AggregationCursor implementation"""
    print("=== Testing AggregationCursor Implementation ===\n")

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
                "department": "Engineering",
                "salary": 70000,
                "tags": ["python", "sql", "javascript"],
            },
            {
                "name": "Bob",
                "age": 25,
                "department": "Engineering",
                "salary": 60000,
                "tags": ["python", "sql"],
            },
            {
                "name": "Charlie",
                "age": 35,
                "department": "Marketing",
                "salary": 65000,
                "tags": ["marketing", "sql"],
            },
            {
                "name": "Diana",
                "age": 28,
                "department": "Marketing",
                "salary": 62000,
                "tags": ["marketing", "design"],
            },
            {
                "name": "Eve",
                "age": 32,
                "department": "Engineering",
                "salary": 75000,
                "tags": ["python", "ml"],
            },
        ]

        result = users.insert_many(sample_docs)
        print(f"   Inserted {len(result.inserted_ids)} documents\n")

        # Test 1: Basic aggregation with match and sort
        print("2. Test: Basic aggregation with match and sort")
        pipeline1 = [
            {"$match": {"department": "Engineering"}},
            {"$sort": {"salary": -1}},
        ]

        cursor1 = users.aggregate(pipeline1)

        # Test that cursor implements the iterator protocol
        print(f"   Cursor has __iter__: {hasattr(cursor1, '__iter__')}")
        print(f"   Cursor has __next__: {hasattr(cursor1, '__next__')}")

        # Test len
        print(f"   Number of results: {len(cursor1)}")

        # Test iteration
        results1 = list(cursor1)
        print(f"   Results: {len(results1)} documents")
        for doc in results1:
            print(f"     {doc['name']}: ${doc['salary']}")
        print()

        # Test 2: Aggregation with group
        print("3. Test: Aggregation with group")
        pipeline2 = [
            {
                "$group": {
                    "_id": "$department",
                    "avgSalary": {"$avg": "$salary"},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        cursor2 = users.aggregate(pipeline2)
        results2 = list(cursor2)
        print(f"   Results: {len(results2)} departments")
        for doc in results2:
            print(f"     {doc['_id']}: ${doc['avgSalary']:.0f}")
        print()

        # Test 3: Aggregation with unwind
        print("4. Test: Aggregation with unwind")
        pipeline3 = [
            {"$unwind": "$tags"},
            {"$match": {"tags": "python"}},
            {"$limit": 10},
        ]

        cursor3 = users.aggregate(pipeline3)
        results3 = list(cursor3)
        print(f"   Results: {len(results3)} documents with 'python' tag")
        for doc in results3:
            print(f"     {doc['name']}: {doc['tags']}")
        print()

        # Test 4: Test cursor methods
        print("5. Test: Cursor methods")
        cursor4 = users.aggregate([{"$match": {"department": "Engineering"}}])

        # Test indexing
        first_doc = cursor4[0]
        print(f"   First document name: {first_doc['name']}")

        # Test slicing (convert to list first)
        results4 = cursor4.to_list()
        print(f"   Total results: {len(results4)}")

        # Test sort method
        cursor4.sort(key=lambda x: x["name"])
        sorted_results = list(cursor4)
        print(f"   Sorted by name: {[doc['name'] for doc in sorted_results]}")
        print()

        print("=== All tests passed! ===")


if __name__ == "__main__":
    test_aggregation_cursor()
