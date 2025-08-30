#!/usr/bin/env python3
"""
Example demonstrating combined usage of advanced $unwind options in NeoSQLite.

This example shows how to use both includeArrayIndex and preserveNullAndEmptyArrays
together in more complex scenarios.
"""

import neosqlite


def main():
    print("=== NeoSQLite Advanced $unwind Options - Combined Usage ===\n")

    # Create an in-memory database
    with neosqlite.Connection(":memory:") as conn:
        # Get a collection
        employees = conn.employees

        # Insert sample data with nested arrays
        sample_docs = [
            {
                "_id": 1,
                "name": "Alice",
                "projects": [
                    {
                        "name": "Project A",
                        "tasks": ["Design", "Implementation", "Testing"],
                    },
                    {"name": "Project B", "tasks": ["Research", "Prototype"]},
                ],
            },
            {
                "_id": 2,
                "name": "Bob",
                "projects": [
                    {
                        "name": "Project C",
                        "tasks": [
                            "Planning",
                            "Execution",
                            "Review",
                            "Deployment",
                        ],
                    }
                ],
            },
            {
                "_id": 3,
                "name": "Charlie",
                "projects": [],  # Empty projects array
            },
            {
                "_id": 4,
                "name": "Diana",
                "projects": [
                    {"name": "Project D", "tasks": []}  # Empty tasks array
                ],
            },
        ]

        employees.insert_many(sample_docs)
        print("1. Inserted sample employee data:")
        for doc in employees.find():
            print(f"   {doc}")
        print()

        # Example 1: Nested unwind with includeArrayIndex at both levels
        print("2. Example: Nested unwind with includeArrayIndex at both levels")
        print(
            "   Pipeline: Unwind projects and tasks, include indices for both"
        )
        pipeline1 = [
            {
                "$unwind": {
                    "path": "$projects",
                    "includeArrayIndex": "projectIndex",
                }
            },
            {
                "$unwind": {
                    "path": "$projects.tasks",
                    "includeArrayIndex": "taskIndex",
                }
            },
        ]

        cursor1 = employees.aggregate(pipeline1)
        print("   Results (projects and tasks with indices):")
        for doc in cursor1:
            proj_name = doc["projects"]["name"]
            task_name = doc["projects"]["tasks"]
            proj_idx = doc["projectIndex"]
            task_idx = doc["taskIndex"]
            print(
                f"     {doc['name']}: Project '{proj_name}'[{proj_idx}], Task '{task_name}'[{task_idx}]"
            )
        print()

        # Example 2: Nested unwind with preserveNullAndEmptyArrays
        print("3. Example: Nested unwind with preserveNullAndEmptyArrays")
        print("   Pipeline: Unwind projects and tasks, preserve empty arrays")
        pipeline2 = [
            {
                "$unwind": {
                    "path": "$projects",
                    "preserveNullAndEmptyArrays": True,
                }
            },
            {
                "$unwind": {
                    "path": "$projects.tasks",
                    "preserveNullAndEmptyArrays": True,
                }
            },
        ]

        cursor2 = employees.aggregate(pipeline2)
        print("   Results (preserving documents with empty arrays):")
        for doc in cursor2:
            if "projects" in doc and doc["projects"] is not None:
                if (
                    "tasks" in doc["projects"]
                    and doc["projects"]["tasks"] is not None
                ):
                    print(
                        f"     {doc['name']}: Project '{doc['projects']['name']}', Task '{doc['projects']['tasks']}'"
                    )
                else:
                    print(
                        f"     {doc['name']}: Project '{doc['projects']['name']}', Task: null (empty tasks array)"
                    )
            else:
                print(
                    f"     {doc['name']}: Project: null (empty projects array)"
                )
        print()

        # Example 3: Complex pipeline with both options and filtering
        print("4. Example: Complex pipeline with both options and filtering")
        print(
            "   Pipeline: Unwind with both options, filter to show only Alice's data"
        )
        pipeline3 = [
            {"$match": {"name": "Alice"}},
            {
                "$unwind": {
                    "path": "$projects",
                    "includeArrayIndex": "projectIndex",
                    "preserveNullAndEmptyArrays": True,
                }
            },
            {
                "$unwind": {
                    "path": "$projects.tasks",
                    "includeArrayIndex": "taskIndex",
                    "preserveNullAndEmptyArrays": True,
                }
            },
        ]

        cursor3 = employees.aggregate(pipeline3)
        print("   Results (filtered with both options applied):")
        for doc in cursor3:
            name = doc["name"]
            proj_name = doc["projects"]["name"]
            proj_idx = doc["projectIndex"]
            if (
                "tasks" in doc["projects"]
                and doc["projects"]["tasks"] is not None
            ):
                task_name = doc["projects"]["tasks"]
                task_idx = doc["taskIndex"]
                print(
                    f"     {name}: Project '{proj_name}'[{proj_idx}], Task '{task_name}'[{task_idx}]"
                )
            else:
                print(
                    f"     {name}: Project '{proj_name}'[{proj_idx}], Task: null"
                )
        print()

        print("=== Key Observations ===")
        print("• Both advanced options work with nested array unwinding")
        print("• Index fields help track the position of unwound elements")
        print(
            "• preserveNullAndEmptyArrays prevents loss of documents with empty data"
        )
        print("• Options can be combined for complex data processing needs")
        print(
            "• Advanced options are implemented in Python (not SQL-optimized)"
        )


if __name__ == "__main__":
    main()
    print("\n=== Example Complete ===")
