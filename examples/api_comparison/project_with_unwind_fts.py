"""Module for comparing $project with $unwind and FTS text search between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .timing import (
    end_mongo_timing,
    end_neo_timing,
    set_accumulation_mode,
    start_mongo_timing,
    start_neo_timing,
)
from .utils import get_mongo_client

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_project_unwind_fts():
    """Compare $project stage with $unwind and FTS text search"""
    print("\n=== $project with $unwind and FTS Comparison ===")

    neo_project_simple = None
    neo_project_simple_result = None
    neo_project_field_ref = None
    neo_project_field_ref_result = None
    neo_project_exclusion = None
    neo_project_exclusion_result = None
    neo_project_after_unwind = None
    neo_project_after_unwind_result = None
    neo_project_unwind_fts = None
    neo_project_unwind_fts_result = None

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection

        # ---- Test 1: Simple inclusion ----
        neo_collection.insert_many(
            [
                {"name": "Alice", "age": 30, "city": "NYC"},
                {"name": "Bob", "age": 25, "city": "LA"},
            ]
        )

        set_accumulation_mode(True)

        simple_pipeline = [{"$project": {"name": 1, "age": 1}}]
        start_neo_timing()
        try:
            neo_project_simple_result = list(
                neo_collection.aggregate(simple_pipeline)
            )
            neo_project_simple = len(neo_project_simple_result)
            print(f"Neo $project simple: {neo_project_simple} docs")
        except Exception as e:
            neo_project_simple = f"Error: {e}"
            print(f"Neo $project simple: Error - {e}")
        finally:
            end_neo_timing()

        # ---- Test 2: Field reference projection ----
        neo_collection.delete_many({})
        neo_collection.insert_many(
            [
                {
                    "person": {"first": "Alice", "last": "Smith"},
                    "address": {"city": "NYC"},
                },
                {
                    "person": {"first": "Bob", "last": "Jones"},
                    "address": {"city": "LA"},
                },
            ]
        )

        field_ref_pipeline = [
            {
                "$project": {
                    "firstName": "$person.first",
                    "lastName": "$person.last",
                    "city": "$address.city",
                }
            }
        ]
        start_neo_timing()
        try:
            neo_project_field_ref_result = list(
                neo_collection.aggregate(field_ref_pipeline)
            )
            neo_project_field_ref = len(neo_project_field_ref_result)
            print(f"Neo $project field ref: {neo_project_field_ref} docs")
        except Exception as e:
            neo_project_field_ref = f"Error: {e}"
            print(f"Neo $project field ref: Error - {e}")
        finally:
            end_neo_timing()

        # ---- Test 3: Exclusion mode ----
        neo_collection.delete_many({})
        neo_collection.insert_many(
            [
                {"name": "Alice", "age": 30, "city": "NYC"},
                {"name": "Bob", "age": 25, "city": "LA"},
            ]
        )

        exclusion_pipeline = [{"$project": {"city": 0}}]
        start_neo_timing()
        try:
            neo_project_exclusion_result = list(
                neo_collection.aggregate(exclusion_pipeline)
            )
            neo_project_exclusion = len(neo_project_exclusion_result)
            print(f"Neo $project exclusion: {neo_project_exclusion} docs")
        except Exception as e:
            neo_project_exclusion = f"Error: {e}"
            print(f"Neo $project exclusion: Error - {e}")
        finally:
            end_neo_timing()

        # ---- Test 4: $project after $unwind ----
        neo_collection.delete_many({})
        neo_collection.insert_many(
            [
                {
                    "author": "Alice",
                    "posts": [
                        {"title": "Post 1", "views": 100},
                        {"title": "Post 2", "views": 200},
                    ],
                },
                {
                    "author": "Bob",
                    "posts": [
                        {"title": "Post 3", "views": 300},
                    ],
                },
            ]
        )

        unwind_project_pipeline = [
            {"$unwind": "$posts"},
            {
                "$project": {
                    "author": 1,
                    "title": "$posts.title",
                    "views": "$posts.views",
                }
            },
        ]
        start_neo_timing()
        try:
            neo_project_after_unwind_result = list(
                neo_collection.aggregate(unwind_project_pipeline)
            )
            neo_project_after_unwind = len(neo_project_after_unwind_result)
            print(
                f"Neo $project after $unwind: {neo_project_after_unwind} docs"
            )
        except Exception as e:
            neo_project_after_unwind = f"Error: {e}"
            print(f"Neo $project after $unwind: Error - {e}")
        finally:
            end_neo_timing()

        # ---- Test 5: $project after $unwind + FTS text search ----
        neo_collection.delete_many({})
        neo_collection.insert_many(
            [
                {
                    "_id": 1,
                    "author": "Alice",
                    "posts": [
                        {
                            "title": "Python Performance Tips",
                            "content": "How to optimize Python code",
                        },
                        {
                            "title": "Database Design",
                            "content": "Best practices for database design",
                        },
                        {
                            "title": "Web Development",
                            "content": "Modern web development techniques",
                        },
                    ],
                },
                {
                    "_id": 2,
                    "author": "Bob",
                    "posts": [
                        {
                            "title": "JavaScript Performance",
                            "content": "Optimizing JavaScript applications",
                        },
                        {
                            "title": "Mobile Development",
                            "content": "Building mobile apps with React Native",
                        },
                        {
                            "title": "Backend Systems",
                            "content": "Designing scalable backend systems",
                        },
                    ],
                },
            ]
        )

        # Create FTS index on nested content field
        try:
            neo_collection.create_index("posts.content", fts=True)
        except Exception as e:
            print(f"Neo FTS index creation: Error - {e}")

        unwind_fts_project_pipeline = [
            {"$unwind": "$posts"},
            {"$match": {"$text": {"$search": "performance"}}},
            {
                "$project": {
                    "author": 1,
                    "title": "$posts.title",
                    "content": "$posts.content",
                }
            },
        ]
        start_neo_timing()
        try:
            neo_project_unwind_fts_result = list(
                neo_collection.aggregate(unwind_fts_project_pipeline)
            )
            neo_project_unwind_fts = len(neo_project_unwind_fts_result)
            print(
                f"Neo $project after $unwind+$text: {neo_project_unwind_fts} docs"
            )
            for doc in neo_project_unwind_fts_result:
                print(f"  -> {doc}")
        except Exception as e:
            neo_project_unwind_fts = f"Error: {e}"
            print(f"Neo $project after $unwind+$text: Error - {e}")
        finally:
            end_neo_timing()

    # ---- MongoDB comparison ----
    client = get_mongo_client()
    mongo_collection = None

    mongo_project_simple = None
    mongo_project_simple_result = None
    mongo_project_field_ref = None
    mongo_project_field_ref_result = None
    mongo_project_exclusion = None
    mongo_project_exclusion_result = None
    mongo_project_after_unwind = None
    mongo_project_after_unwind_result = None
    mongo_project_unwind_fts = None
    mongo_project_unwind_fts_result = None

    if client:
        mongo_collection = client.test_database.test_collection
        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {"name": "Alice", "age": 30, "city": "NYC"},
                {"name": "Bob", "age": 25, "city": "LA"},
            ]
        )

        set_accumulation_mode(True)

        start_mongo_timing()
        try:
            mongo_project_simple_result = list(
                mongo_collection.aggregate(simple_pipeline)
            )
            mongo_project_simple = len(mongo_project_simple_result)
            print(f"Mongo $project simple: {mongo_project_simple} docs")
        except Exception as e:
            mongo_project_simple = f"Error: {e}"
            print(f"Mongo $project simple: Error - {e}")
        finally:
            end_mongo_timing()

        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {
                    "person": {"first": "Alice", "last": "Smith"},
                    "address": {"city": "NYC"},
                },
                {
                    "person": {"first": "Bob", "last": "Jones"},
                    "address": {"city": "LA"},
                },
            ]
        )

        start_mongo_timing()
        try:
            mongo_project_field_ref_result = list(
                mongo_collection.aggregate(field_ref_pipeline)
            )
            mongo_project_field_ref = len(mongo_project_field_ref_result)
            print(f"Mongo $project field ref: {mongo_project_field_ref} docs")
        except Exception as e:
            mongo_project_field_ref = f"Error: {e}"
            print(f"Mongo $project field ref: Error - {e}")
        finally:
            end_mongo_timing()

        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {"name": "Alice", "age": 30, "city": "NYC"},
                {"name": "Bob", "age": 25, "city": "LA"},
            ]
        )

        start_mongo_timing()
        try:
            mongo_project_exclusion_result = list(
                mongo_collection.aggregate(exclusion_pipeline)
            )
            mongo_project_exclusion = len(mongo_project_exclusion_result)
            print(f"Mongo $project exclusion: {mongo_project_exclusion} docs")
        except Exception as e:
            mongo_project_exclusion = f"Error: {e}"
            print(f"Mongo $project exclusion: Error - {e}")
        finally:
            end_mongo_timing()

        mongo_collection.delete_many({})
        mongo_collection.insert_many(
            [
                {
                    "author": "Alice",
                    "posts": [
                        {"title": "Post 1", "views": 100},
                        {"title": "Post 2", "views": 200},
                    ],
                },
                {
                    "author": "Bob",
                    "posts": [
                        {"title": "Post 3", "views": 300},
                    ],
                },
            ]
        )

        start_mongo_timing()
        try:
            mongo_project_after_unwind_result = list(
                mongo_collection.aggregate(unwind_project_pipeline)
            )
            mongo_project_after_unwind = len(mongo_project_after_unwind_result)
            print(
                f"Mongo $project after $unwind: {mongo_project_after_unwind} docs"
            )
        except Exception as e:
            mongo_project_after_unwind = f"Error: {e}"
            print(f"Mongo $project after $unwind: Error - {e}")
        finally:
            end_mongo_timing()

        # MongoDB text search on unwound arrays is not natively supported the same way
        # MongoDB requires a text index on the array field, and $unwind + $text works differently
        # We'll skip the FTS comparison as the semantics differ
        mongo_project_unwind_fts = None
        mongo_project_unwind_fts_result = None

    # Record comparisons
    reporter.record_comparison(
        "$project with $unwind+FTS",
        "field inclusion",
        (
            neo_project_simple_result
            if neo_project_simple_result
            else neo_project_simple
        ),
        (
            mongo_project_simple_result
            if mongo_project_simple_result
            else mongo_project_simple
        ),
        skip_reason="MongoDB not available" if not client else None,
    )

    reporter.record_comparison(
        "$project with $unwind+FTS",
        "field reference with alias",
        (
            neo_project_field_ref_result
            if neo_project_field_ref_result
            else neo_project_field_ref
        ),
        (
            mongo_project_field_ref_result
            if mongo_project_field_ref_result
            else mongo_project_field_ref
        ),
        skip_reason="MongoDB not available" if not client else None,
    )

    reporter.record_comparison(
        "$project with $unwind+FTS",
        "field exclusion",
        (
            neo_project_exclusion_result
            if neo_project_exclusion_result
            else neo_project_exclusion
        ),
        (
            mongo_project_exclusion_result
            if mongo_project_exclusion_result
            else mongo_project_exclusion
        ),
        skip_reason="MongoDB not available" if not client else None,
    )

    reporter.record_comparison(
        "$project with $unwind+FTS",
        "project following unwind",
        (
            neo_project_after_unwind_result
            if neo_project_after_unwind_result
            else neo_project_after_unwind
        ),
        (
            mongo_project_after_unwind_result
            if mongo_project_after_unwind_result
            else mongo_project_after_unwind
        ),
        skip_reason="MongoDB not available" if not client else None,
    )

    reporter.record_comparison(
        "$project with $unwind+FTS",
        "project following unwind and text search",
        (
            neo_project_unwind_fts_result
            if neo_project_unwind_fts_result
            else neo_project_unwind_fts
        ),
        (
            mongo_project_unwind_fts_result
            if mongo_project_unwind_fts_result
            else mongo_project_unwind_fts
        ),
        skip_reason=(
            "MongoDB not available"
            if not client
            else "MongoDB $text index semantics differ on unwound arrays"
        ),
    )
