"""Module for comparing additional update operators between NeoSQLite and PyMongo"""

import warnings
from datetime import datetime

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


def compare_additional_update_operators():
    """Compare additional update operators"""
    print("\n=== Additional Update Operators Comparison ===")

    # Check MongoDB availability FIRST to determine if we should time operations
    client = get_mongo_client()
    mongo_available = client is not None

    # Initialize result variables
    neo_push = False
    neo_addtoset = False
    neo_pull = False
    neo_pop = False
    neo_currentdate = False

    mongo_push = None
    mongo_addtoset = None
    mongo_pull = None
    mongo_pop = None
    mongo_currentdate = None

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_one(
            {"name": "Alice", "tags": ["python"], "score": 100}
        )

        set_accumulation_mode(True)

        # Test $push - ONLY time if MongoDB is available
        if mongo_available:
            start_neo_timing()
        try:
            neo_collection.update_one(
                {"name": "Alice"}, {"$push": {"tags": "sql"}}
            )
        finally:
            if mongo_available:
                end_neo_timing()

        doc = neo_collection.find_one({"name": "Alice"})
        neo_push = "sql" in doc.get("tags", [])
        print(f"Neo $push: {'OK' if neo_push else 'FAIL'}")

        # Reset for $addToSet
        neo_collection.update_one(
            {"name": "Alice"}, {"$set": {"tags": ["python", "sql"]}}
        )

        # Test $addToSet - ONLY time if MongoDB is available
        if mongo_available:
            start_neo_timing()
        try:
            neo_collection.update_one(
                {"name": "Alice"}, {"$addToSet": {"tags": "sql"}}
            )
            neo_collection.update_one(
                {"name": "Alice"}, {"$addToSet": {"tags": "mongodb"}}
            )
        finally:
            if mongo_available:
                end_neo_timing()

        doc = neo_collection.find_one({"name": "Alice"})
        tags = doc.get("tags", [])
        neo_addtoset = (
            "sql" in tags and "mongodb" in tags and tags.count("sql") == 1
        )
        print(f"Neo $addToSet: {'OK' if neo_addtoset else 'FAIL'}")

        # Reset for $pull
        neo_collection.update_one(
            {"name": "Alice"}, {"$set": {"tags": ["python", "sql", "mongodb"]}}
        )

        # Test $pull - ONLY time if MongoDB is available
        if mongo_available:
            start_neo_timing()
        try:
            neo_collection.update_one(
                {"name": "Alice"}, {"$pull": {"tags": "sql"}}
            )
        finally:
            if mongo_available:
                end_neo_timing()

        doc = neo_collection.find_one({"name": "Alice"})
        neo_pull = "sql" not in doc.get("tags", [])
        print(f"Neo $pull: {'OK' if neo_pull else 'FAIL'}")

        # Reset for $pop
        neo_collection.update_one(
            {"name": "Alice"}, {"$set": {"tags": ["first", "middle", "last"]}}
        )

        # Test $pop (remove last) - ONLY time if MongoDB is available
        if mongo_available:
            start_neo_timing()
        try:
            neo_collection.update_one({"name": "Alice"}, {"$pop": {"tags": 1}})
        finally:
            if mongo_available:
                end_neo_timing()

        doc = neo_collection.find_one({"name": "Alice"})
        neo_pop = doc.get("tags", []) == ["first", "middle"]
        print(f"Neo $pop (last): {'OK' if neo_pop else 'FAIL'}")

        # Reset for $currentDate
        neo_collection.update_one(
            {"name": "Alice"}, {"$set": {"updated_at": None}}
        )

        # Test $currentDate - ONLY time if MongoDB is available
        if mongo_available:
            start_neo_timing()
        try:
            neo_collection.update_one(
                {"name": "Alice"}, {"$currentDate": {"updated_at": True}}
            )
        finally:
            if mongo_available:
                end_neo_timing()

        doc = neo_collection.find_one({"name": "Alice"})
        updated_at = doc.get("updated_at")
        neo_currentdate = updated_at is not None and isinstance(
            updated_at, datetime
        )
        print(
            f"Neo $currentDate: {'OK' if neo_currentdate else 'FAIL'} (returns {type(updated_at).__name__})"
        )

    if mongo_available:
        client = get_mongo_client()
        if client:
            mongo_db = client.test_database
            mongo_collection = mongo_db.test_collection
            mongo_collection.delete_many({})
            mongo_collection.insert_one(
                {"name": "Alice", "tags": ["python"], "score": 100}
            )

            set_accumulation_mode(True)

            # Test $push
            start_mongo_timing()
            try:
                mongo_collection.update_one(
                    {"name": "Alice"}, {"$push": {"tags": "sql"}}
                )
            finally:
                end_mongo_timing()

            doc = mongo_collection.find_one({"name": "Alice"})
            mongo_push = "sql" in doc.get("tags", [])
            print(f"Mongo $push: {'OK' if mongo_push else 'FAIL'}")

            # Reset for $addToSet
            mongo_collection.update_one(
                {"name": "Alice"}, {"$set": {"tags": ["python", "sql"]}}
            )

            # Test $addToSet
            start_mongo_timing()
            try:
                mongo_collection.update_one(
                    {"name": "Alice"}, {"$addToSet": {"tags": "sql"}}
                )
                mongo_collection.update_one(
                    {"name": "Alice"}, {"$addToSet": {"tags": "mongodb"}}
                )
            finally:
                end_mongo_timing()

            doc = mongo_collection.find_one({"name": "Alice"})
            tags = doc.get("tags", [])
            mongo_addtoset = (
                "sql" in tags and "mongodb" in tags and tags.count("sql") == 1
            )
            print(f"Mongo $addToSet: {'OK' if mongo_addtoset else 'FAIL'}")

            # Reset for $pull
            mongo_collection.update_one(
                {"name": "Alice"},
                {"$set": {"tags": ["python", "sql", "mongodb"]}},
            )

            # Test $pull
            start_mongo_timing()
            try:
                mongo_collection.update_one(
                    {"name": "Alice"}, {"$pull": {"tags": "sql"}}
                )
            finally:
                end_mongo_timing()

            doc = mongo_collection.find_one({"name": "Alice"})
            mongo_pull = "sql" not in doc.get("tags", [])
            print(f"Mongo $pull: {'OK' if mongo_pull else 'FAIL'}")

            # Reset for $pop
            mongo_collection.update_one(
                {"name": "Alice"},
                {"$set": {"tags": ["first", "middle", "last"]}},
            )

            # Test $pop (remove last)
            start_mongo_timing()
            try:
                mongo_collection.update_one(
                    {"name": "Alice"}, {"$pop": {"tags": 1}}
                )
            finally:
                end_mongo_timing()

            doc = mongo_collection.find_one({"name": "Alice"})
            mongo_pop = doc.get("tags", []) == ["first", "middle"]
            print(f"Mongo $pop (last): {'OK' if mongo_pop else 'FAIL'}")

            # Reset for $currentDate
            mongo_collection.update_one(
                {"name": "Alice"}, {"$set": {"updated_at": None}}
            )

            # Test $currentDate
            start_mongo_timing()
            try:
                mongo_collection.update_one(
                    {"name": "Alice"},
                    {"$currentDate": {"updated_at": True}},
                )
            finally:
                end_mongo_timing()

            doc = mongo_collection.find_one({"name": "Alice"})
            updated_at = doc.get("updated_at")
            mongo_currentdate = updated_at is not None
            print(
                f"Mongo $currentDate: {'OK' if mongo_currentdate else 'FAIL'} (returns datetime)"
            )

    # Record comparisons
    ops = [
        ("$push", neo_push, mongo_push),
        ("$addToSet", neo_addtoset, mongo_addtoset),
        ("$pull", neo_pull, mongo_pull),
        ("$pop", neo_pop, mongo_pop),
        ("$currentDate", neo_currentdate, mongo_currentdate),
    ]

    for op_name, neo_res, mongo_res in ops:
        reporter.record_comparison(
            "Update (Array Operators)",
            op_name,
            neo_res,
            mongo_res,
            skip_reason=(
                "MongoDB not available" if not mongo_available else None
            ),
        )
