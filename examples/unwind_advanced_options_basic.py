#!/usr/bin/env python3
"""
Example demonstrating basic usage of advanced $unwind options in NeoSQLite.

This example shows how to use includeArrayIndex and preserveNullAndEmptyArrays
with simple, flat arrays.
"""

import neosqlite


def main():
    print("=== NeoSQLite Advanced $unwind Options - Basic Usage ===\n")

    # Create an in-memory database
    with neosqlite.Connection(":memory:") as conn:
        # Get a collection
        students = conn.students

        # Insert sample data with various array types
        sample_docs = [
            {"_id": 1, "name": "Alice", "scores": [85, 92, 78, 96]},
            {"_id": 2, "name": "Bob", "scores": [88, 84]},  # Shorter array
            {"_id": 3, "name": "Charlie", "scores": []},  # Empty array
            {"_id": 4, "name": "Diana", "scores": None},  # Null value
            {"_id": 5, "name": "Eve"},  # Missing scores field
        ]

        students.insert_many(sample_docs)
        print("1. Inserted sample student data:")
        for doc in students.find():
            print(f"   {doc}")
        print()

        # Example 1: Basic unwind with includeArrayIndex
        print("2. Example: $unwind with includeArrayIndex")
        print("   Pipeline: Unwind scores and include their array indices")
        pipeline1 = [
            {"$unwind": {"path": "$scores", "includeArrayIndex": "scoreIndex"}}
        ]

        result1 = students.aggregate(pipeline1)
        print("   Results (only documents with actual array elements):")
        for doc in result1:
            print(f"     {doc}")
        print()

        # Example 2: Unwind with preserveNullAndEmptyArrays
        print("3. Example: $unwind with preserveNullAndEmptyArrays")
        print("   Pipeline: Unwind scores and preserve null/empty arrays")
        pipeline2 = [
            {"$unwind": {"path": "$scores", "preserveNullAndEmptyArrays": True}}
        ]

        result2 = students.aggregate(pipeline2)
        print("   Results (documents with null/empty arrays preserved):")
        for doc in result2:
            print(f"     {doc}")
        print()

        # Example 3: Combining both options
        print(
            "4. Example: Combining includeArrayIndex and preserveNullAndEmptyArrays"
        )
        print("   Pipeline: Unwind scores with both advanced options")
        pipeline3 = [
            {
                "$unwind": {
                    "path": "$scores",
                    "includeArrayIndex": "scoreIndex",
                    "preserveNullAndEmptyArrays": True,
                }
            }
        ]

        result3 = students.aggregate(pipeline3)
        print("   Results (both options applied):")
        for doc in result3:
            print(f"     {doc}")
        print()

        # Example 4: Comparison with traditional unwind
        print("5. Example: Comparison with traditional $unwind")
        print("   Pipeline: Traditional string-based unwind")
        pipeline4 = [{"$unwind": "$scores"}]

        result4 = students.aggregate(pipeline4)
        print("   Results (traditional unwind - null/empty omitted):")
        for doc in result4:
            print(f"     {doc}")
        print()

        print("=== Key Observations ===")
        print("• includeArrayIndex adds the array index as a new field")
        print(
            "• preserveNullAndEmptyArrays keeps documents with null/empty arrays"
        )
        print(
            "• Missing fields are not preserved even with preserveNullAndEmptyArrays"
        )
        print("• Traditional syntax still works for basic unwinding")
        print(
            "• Advanced options are implemented in Python (not SQL-optimized)"
        )


if __name__ == "__main__":
    main()
    print("\n=== Example Complete ===")
