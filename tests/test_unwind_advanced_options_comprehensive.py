# coding: utf-8
"""
Comprehensive test cases for advanced $unwind options (includeArrayIndex and preserveNullAndEmptyArrays)
These tests verify the implementation of advanced $unwind options as described in the roadmap.
"""
import neosqlite
import pytest


def test_unwind_with_include_array_index_on_empty_array():
    """Test $unwind with includeArrayIndex when array is empty and preserveNullAndEmptyArrays is False"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data with empty array
        collection.insert_one({"_id": 1, "name": "Alice", "hobbies": []})

        # Test unwind with includeArrayIndex but preserveNullAndEmptyArrays=False (default)
        pipeline = [
            {"$unwind": {"path": "$hobbies", "includeArrayIndex": "hobbyIndex"}}
        ]
        result = collection.aggregate(pipeline)

        # Should have 0 documents since array is empty and preserveNullAndEmptyArrays is False
        assert len(result) == 0


def test_unwind_with_include_array_index_on_null_field():
    """Test $unwind with includeArrayIndex when field is null and preserveNullAndEmptyArrays is False"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data with null field
        collection.insert_one({"_id": 1, "name": "Alice", "hobbies": None})

        # Test unwind with includeArrayIndex but preserveNullAndEmptyArrays=False (default)
        pipeline = [
            {"$unwind": {"path": "$hobbies", "includeArrayIndex": "hobbyIndex"}}
        ]
        result = collection.aggregate(pipeline)

        # Should have 0 documents since field is null and preserveNullAndEmptyArrays is False
        assert len(result) == 0


def test_unwind_with_include_array_index_on_missing_field():
    """Test $unwind with includeArrayIndex when field is missing and preserveNullAndEmptyArrays is False"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data without the field
        collection.insert_one({"_id": 1, "name": "Alice"})

        # Test unwind with includeArrayIndex but preserveNullAndEmptyArrays=False (default)
        pipeline = [
            {"$unwind": {"path": "$hobbies", "includeArrayIndex": "hobbyIndex"}}
        ]
        result = collection.aggregate(pipeline)

        # Should have 0 documents since field is missing and preserveNullAndEmptyArrays is False
        assert len(result) == 0


def test_unwind_with_preserve_false_on_empty_array():
    """Test $unwind with preserveNullAndEmptyArrays=False on empty array"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data with empty array
        collection.insert_one({"_id": 1, "name": "Alice", "hobbies": []})

        # Test unwind with preserveNullAndEmptyArrays=False
        pipeline = [
            {
                "$unwind": {
                    "path": "$hobbies",
                    "preserveNullAndEmptyArrays": False,
                }
            }
        ]
        result = collection.aggregate(pipeline)

        # Should have 0 documents since array is empty and preserveNullAndEmptyArrays is False
        assert len(result) == 0


def test_unwind_with_preserve_false_on_null_field():
    """Test $unwind with preserveNullAndEmptyArrays=False on null field"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data with null field
        collection.insert_one({"_id": 1, "name": "Alice", "hobbies": None})

        # Test unwind with preserveNullAndEmptyArrays=False
        pipeline = [
            {
                "$unwind": {
                    "path": "$hobbies",
                    "preserveNullAndEmptyArrays": False,
                }
            }
        ]
        result = collection.aggregate(pipeline)

        # Should have 0 documents since field is null and preserveNullAndEmptyArrays is False
        assert len(result) == 0


def test_unwind_with_preserve_false_on_missing_field():
    """Test $unwind with preserveNullAndEmptyArrays=False on missing field"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data without the field
        collection.insert_one({"_id": 1, "name": "Alice"})

        # Test unwind with preserveNullAndEmptyArrays=False
        pipeline = [
            {
                "$unwind": {
                    "path": "$hobbies",
                    "preserveNullAndEmptyArrays": False,
                }
            }
        ]
        result = collection.aggregate(pipeline)

        # Should have 0 documents since field is missing and preserveNullAndEmptyArrays is False
        assert len(result) == 0


def test_unwind_with_both_options_false():
    """Test $unwind with both includeArrayIndex and preserveNullAndEmptyArrays set to False"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data
        collection.insert_many(
            [
                {"_id": 1, "name": "Alice", "hobbies": ["reading", "swimming"]},
                {"_id": 2, "name": "Bob", "hobbies": []},  # Empty array
            ]
        )

        # Test unwind with both options set to False
        pipeline = [
            {
                "$unwind": {
                    "path": "$hobbies",
                    "includeArrayIndex": "hobbyIndex",
                    "preserveNullAndEmptyArrays": False,
                }
            }
        ]
        result = collection.aggregate(pipeline)

        # Should have 2 documents (from Alice only, Bob's empty array should be excluded)
        assert len(result) == 2

        # Check Alice's documents
        alice_docs = [doc for doc in result if doc["name"] == "Alice"]
        assert len(alice_docs) == 2

        # Check that index fields are present with correct values
        alice_indices = [doc["hobbyIndex"] for doc in alice_docs]
        assert sorted(alice_indices) == [0, 1]

        # Check that hobbies are correct
        alice_hobbies = [doc["hobbies"] for doc in alice_docs]
        assert set(alice_hobbies) == {"reading", "swimming"}


def test_unwind_with_both_options_on_complex_data():
    """Test $unwind with both options on complex nested data"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert complex test data
        collection.insert_many(
            [
                {
                    "_id": 1,
                    "name": "Alice",
                    "courses": [
                        {
                            "name": "Math",
                            "assignments": [
                                {"title": "HW1", "score": 95},
                                {"title": "HW2", "score": 87},
                            ],
                        },
                        {
                            "name": "Science",
                            "assignments": [],  # Empty assignments
                        },
                    ],
                },
                {"_id": 2, "name": "Bob", "courses": []},  # Empty courses
                {
                    "_id": 3,
                    "name": "Charlie",
                    # No courses field
                },
            ]
        )

        # Test nested unwind with both options
        pipeline = [
            {
                "$unwind": {
                    "path": "$courses",
                    "includeArrayIndex": "courseIndex",
                    "preserveNullAndEmptyArrays": True,
                }
            },
            {
                "$unwind": {
                    "path": "$courses.assignments",
                    "includeArrayIndex": "assignmentIndex",
                    "preserveNullAndEmptyArrays": True,
                }
            },
        ]
        result = collection.aggregate(pipeline)

        # Should have 3 documents:
        # 1. Alice's Math course HW1
        # 2. Alice's Math course HW2
        # 3. Alice's Science course (empty assignments preserved)
        assert len(result) == 3

        # Check Alice's documents
        alice_docs = [doc for doc in result if doc["name"] == "Alice"]
        assert len(alice_docs) == 3

        # Check specific documents
        math_docs = [
            doc for doc in alice_docs if doc["courses"]["name"] == "Math"
        ]
        assert len(math_docs) == 2

        science_doc = [
            doc for doc in alice_docs if doc["courses"]["name"] == "Science"
        ][0]
        assert "assignmentIndex" in science_doc
        assert science_doc["assignmentIndex"] is None
        assert (
            "assignments" not in science_doc["courses"]
            or science_doc["courses"]["assignments"] is None
        )


def test_unwind_with_include_array_index_on_single_element_array():
    """Test $unwind with includeArrayIndex on single element array"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data with single element array
        collection.insert_one({"_id": 1, "name": "Alice", "scores": [95]})

        # Test unwind with includeArrayIndex
        pipeline = [
            {"$unwind": {"path": "$scores", "includeArrayIndex": "scoreIndex"}}
        ]
        result = collection.aggregate(pipeline)

        # Should have 1 document
        assert len(result) == 1

        # Check that index is 0
        doc = result[0]
        assert doc["scoreIndex"] == 0
        assert doc["scores"] == 95


def test_unwind_with_preserve_on_non_array_value():
    """Test $unwind with preserveNullAndEmptyArrays on non-array value"""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test_collection"]

        # Insert test data with non-array value
        collection.insert_one(
            {
                "_id": 1,
                "name": "Alice",
                "hobbies": "reading",  # String instead of array
            }
        )

        # Test unwind with preserveNullAndEmptyArrays=True
        pipeline = [
            {
                "$unwind": {
                    "path": "$hobbies",
                    "preserveNullAndEmptyArrays": True,
                }
            }
        ]
        result = collection.aggregate(pipeline)

        # Should have 1 document with the value preserved
        assert len(result) == 1

        doc = result[0]
        assert doc["hobbies"] == "reading"


if __name__ == "__main__":
    pytest.main([__file__])
