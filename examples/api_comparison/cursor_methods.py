"""Module for comparing cursor methods between NeoSQLite and PyMongo"""

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
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_cursor_methods():
    """Compare cursor methods"""
    print("\n=== Cursor Methods Comparison ===")

    # Initialize NeoSQLite result variables
    neo_cursor_methods = False
    neo_batch_size = False
    neo_hint = False
    neo_to_list = False
    neo_to_list_length = False
    neo_clone = False
    neo_explain = False
    neo_comment = False
    neo_retrieved = False
    neo_alive = False
    neo_collection_prop = False
    neo_address = False
    neo_min = False
    neo_max = False
    neo_add_option = False
    neo_remove_option = False
    neo_max_await = False
    neo_session_prop = False
    neo_cursor_id_prop = False
    neo_collation = False
    neo_where = False

    # Initialize MongoDB result variables
    mongo_cursor_methods = False
    mongo_batch_size = False
    mongo_hint = False
    mongo_to_list = False
    mongo_to_list_length = False
    mongo_clone = False
    mongo_explain = False
    mongo_comment = False
    mongo_retrieved = False
    mongo_alive = False
    mongo_collection_prop = False
    mongo_address = False
    mongo_min = False
    mongo_max = False
    mongo_add_option = False
    mongo_remove_option = False
    mongo_max_await = False
    mongo_session_prop = False
    mongo_cursor_id_prop = False
    mongo_collation = False
    mongo_where = None

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_cursor
        neo_collection.insert_many(
            [{"name": f"Doc{i}", "value": i} for i in range(20)]
        )
        neo_collection.create_index("value")

        set_accumulation_mode(True)

        # 1. chained_methods
        start_neo_timing()
        try:
            cursor = (
                neo_collection.find({"value": {"$gte": 3}})
                .limit(5)
                .skip(1)
                .sort("value", neosqlite.DESCENDING)
            )
            results = list(cursor)
            neo_cursor_methods = len(results) <= 5
        except Exception as e:
            print(f"Neo cursor chained methods: Error - {e}")
        finally:
            end_neo_timing()

        # 2. batch_size
        start_neo_timing()
        try:
            cursor = neo_collection.find({}).batch_size(3)
            neo_batch_size = cursor is not None
        except Exception as e:
            print(f"Neo batch_size: Error - {e}")
        finally:
            end_neo_timing()

        # 3. hint
        start_neo_timing()
        try:
            cursor = neo_collection.find({"value": 5}).hint([("value", 1)])
            results = list(cursor)
            neo_hint = len(results) >= 0
        except Exception as e:
            print(f"Neo hint: Error - {e}")
        finally:
            end_neo_timing()

        # 4. to_list
        start_neo_timing()
        try:
            cursor = neo_collection.find({"value": {"$gte": 5}})
            results = cursor.to_list()
            neo_to_list = len(results) >= 0
        except Exception as e:
            print(f"Neo to_list(): Error - {e}")
        finally:
            end_neo_timing()

        # 5. to_list_length
        start_neo_timing()
        try:
            cursor = neo_collection.find({})
            results = cursor.to_list(3)
            neo_to_list_length = len(results) == 3
        except Exception as e:
            print(f"Neo to_list(3): Error - {e}")
        finally:
            end_neo_timing()

        # 6. clone
        start_neo_timing()
        try:
            cursor = neo_collection.find({"value": {"$gte": 5}}).limit(3)
            cloned = cursor.clone()
            results_original = list(cursor)
            results_clone = list(cloned)
            neo_clone = len(results_original) == len(results_clone)
        except Exception as e:
            print(f"Neo clone(): Error - {e}")
        finally:
            end_neo_timing()

        # 7. explain
        start_neo_timing()
        try:
            cursor = neo_collection.find({"value": {"$gte": 5}})
            plan = cursor.explain()
            neo_explain = "queryPlanner" in plan
        except Exception as e:
            print(f"Neo explain(): Error - {e}")
        finally:
            end_neo_timing()

        # 8. comment
        start_neo_timing()
        try:
            cursor = neo_collection.find({"value": {"$gte": 5}}).comment(
                "test comment"
            )
            results = list(cursor)
            neo_comment = (
                len(results) >= 0
                and getattr(cursor, "_comment", None) == "test comment"
            )
        except Exception as e:
            print(f"Neo comment(): Error - {e}")
        finally:
            end_neo_timing()

        # 9. retrieved
        start_neo_timing()
        try:
            cursor = neo_collection.find({"value": {"$gte": 5}})
            results = list(cursor)
            neo_retrieved = cursor.retrieved == len(results)
        except Exception as e:
            print(f"Neo retrieved: Error - {e}")
        finally:
            end_neo_timing()

        # 10. alive
        start_neo_timing()
        try:
            cursor = neo_collection.find({"value": {"$gte": 5}})
            alive_initial = cursor.alive is True
            list(cursor)
            alive_after = isinstance(cursor.alive, bool)
            neo_alive = alive_initial and alive_after
        except Exception as e:
            print(f"Neo alive: Error - {e}")
        finally:
            end_neo_timing()

        # 11. collection
        start_neo_timing()
        try:
            cursor = neo_collection.find({})
            neo_collection_prop = cursor.collection is neo_collection
        except Exception as e:
            print(f"Neo collection: Error - {e}")
        finally:
            end_neo_timing()

        # 12. address
        start_neo_timing()
        try:
            cursor = neo_collection.find({})
            address_before = cursor.address is None
            list(cursor)
            address_after = (
                isinstance(cursor.address, tuple)
                and len(cursor.address) == 2
                and str(cursor.address[0]).startswith("sqlite:")
                and cursor.address[1] == 0
            )
            neo_address = address_before and address_after
        except Exception as e:
            print(f"Neo address: Error - {e}")
        finally:
            end_neo_timing()

        # 13. min
        start_neo_timing()
        try:
            cursor = (
                neo_collection.find({})
                .hint([("value", 1)])
                .min([("value", 10)])
            )
            results = list(cursor)
            neo_min = len(results) == 10 and all(
                doc["value"] >= 10 for doc in results
            )
        except Exception as e:
            print(f"Neo min(): Error - {e}")
        finally:
            end_neo_timing()

        # 14. max
        start_neo_timing()
        try:
            cursor = (
                neo_collection.find({})
                .hint([("value", 1)])
                .max([("value", 10)])
            )
            results = list(cursor)
            neo_max = len(results) == 10 and all(
                doc["value"] < 10 for doc in results
            )
        except Exception as e:
            print(f"Neo max(): Error - {e}")
        finally:
            end_neo_timing()

        # 15. add_option
        start_neo_timing()
        try:
            cursor = neo_collection.find({})
            cursor.add_option(1 << 1)
            neo_add_option = (
                hasattr(cursor, "_options")
                and (cursor._options & (1 << 1)) != 0
            )
        except Exception as e:
            print(f"Neo add_option: Error - {e}")
        finally:
            end_neo_timing()

        # 16. remove_option
        start_neo_timing()
        try:
            cursor = neo_collection.find({})
            cursor.add_option(1 << 1)
            cursor.remove_option(1 << 1)
            neo_remove_option = (getattr(cursor, "_options", 0) & (1 << 1)) == 0
        except Exception as e:
            print(f"Neo remove_option: Error - {e}")
        finally:
            end_neo_timing()

        # 17. max_await_time_ms
        start_neo_timing()
        try:
            cursor = neo_collection.find({}).max_await_time_ms(100)
            neo_max_await = getattr(cursor, "_max_await_time_ms", None) == 100
        except Exception as e:
            print(f"Neo max_await_time_ms: Error - {e}")
        finally:
            end_neo_timing()

        # 18. session
        start_neo_timing()
        try:
            cursor = neo_collection.find({})
            neo_session_prop = cursor.session is None
        except Exception as e:
            print(f"Neo session: Error - {e}")
        finally:
            end_neo_timing()

        # 19. cursor_id
        start_neo_timing()
        try:
            cursor = neo_collection.find({})
            neo_cursor_id_prop = cursor.cursor_id == 0
        except Exception as e:
            print(f"Neo cursor_id: Error - {e}")
        finally:
            end_neo_timing()

        # 20. collation
        start_neo_timing()
        try:
            cursor = neo_collection.find({}).collation(
                {"locale": "en_US", "strength": 2}
            )
            neo_collation = getattr(cursor, "_collation", None) == {
                "locale": "en_US",
                "strength": 2,
            }
        except Exception as e:
            print(f"Neo collation(): Error - {e}")
        finally:
            end_neo_timing()

        # 21. where
        start_neo_timing()
        try:
            cursor = neo_collection.find({}).where(lambda doc: True)
            neo_where = "NeoSQLite-specific (Python filter)"
        except Exception as e:
            print(f"Neo where(): Error - {e}")
        finally:
            end_neo_timing()

    client = test_pymongo_connection()
    if client:
        try:
            mongo_db = client.test_database
            mongo_collection = mongo_db.test_cursor
            mongo_collection.delete_many({})
            mongo_collection.insert_many(
                [{"name": f"Doc{i}", "value": i} for i in range(20)]
            )
            mongo_collection.create_index("value")

            set_accumulation_mode(True)

            # 1. chained_methods
            from pymongo import DESCENDING as MONGO_DESCENDING

            start_mongo_timing()
            try:
                cursor = (
                    mongo_collection.find({"value": {"$gte": 3}})
                    .limit(5)
                    .skip(1)
                    .sort("value", MONGO_DESCENDING)
                )
                results = list(cursor)
                mongo_cursor_methods = len(results) <= 5
            except Exception as e:
                print(f"Mongo cursor chained methods: Error - {e}")
            finally:
                end_mongo_timing()

            # 2. batch_size
            start_mongo_timing()
            try:
                cursor = mongo_collection.find({}).batch_size(3)
                mongo_batch_size = cursor is not None
            except Exception as e:
                print(f"Mongo batch_size: Error - {e}")
            finally:
                end_mongo_timing()

            # 3. hint
            start_mongo_timing()
            try:
                cursor = mongo_collection.find({"value": 5}).hint("value_1")
                results = list(cursor)
                mongo_hint = len(results) >= 0
            except Exception as e:
                print(f"Mongo hint: Error - {e}")
            finally:
                end_mongo_timing()

            # 4. to_list
            start_mongo_timing()
            try:
                cursor = mongo_collection.find({"value": {"$gte": 5}})
                results = cursor.to_list()
                mongo_to_list = len(results) >= 0
            except Exception as e:
                print(f"Mongo to_list(): Error - {e}")
            finally:
                end_mongo_timing()

            # 5. to_list_length
            start_mongo_timing()
            try:
                cursor = mongo_collection.find({})
                results = cursor.to_list(3)
                mongo_to_list_length = len(results) == 3
            except Exception as e:
                print(f"Mongo to_list(3): Error - {e}")
            finally:
                end_mongo_timing()

            # 6. clone
            start_mongo_timing()
            try:
                cursor = mongo_collection.find({"value": {"$gte": 5}}).limit(3)
                cloned = cursor.clone()
                results_original = list(cursor)
                results_clone = list(cloned)
                mongo_clone = len(results_original) == len(results_clone)
            except Exception as e:
                print(f"Mongo clone(): Error - {e}")
            finally:
                end_mongo_timing()

            # 7. explain
            start_mongo_timing()
            try:
                cursor = mongo_collection.find({"value": {"$gte": 5}})
                plan = cursor.explain()
                mongo_explain = "queryPlanner" in plan
            except Exception as e:
                print(f"Mongo explain(): Error - {e}")
            finally:
                end_mongo_timing()

            # 8. comment
            start_mongo_timing()
            try:
                cursor = mongo_collection.find({"value": {"$gte": 5}}).comment(
                    "test comment"
                )
                results = list(cursor)
                mongo_comment = len(results) >= 0
            except Exception as e:
                print(f"Mongo comment(): Error - {e}")
            finally:
                end_mongo_timing()

            # 9. retrieved
            start_mongo_timing()
            try:
                cursor = mongo_collection.find({"value": {"$gte": 5}})
                results = list(cursor)
                mongo_retrieved = cursor.retrieved == len(results)
            except Exception as e:
                print(f"Mongo retrieved: Error - {e}")
            finally:
                end_mongo_timing()

            # 10. alive
            start_mongo_timing()
            try:
                cursor = mongo_collection.find({"value": {"$gte": 5}})
                alive_initial = cursor.alive is True
                list(cursor)
                alive_after = isinstance(cursor.alive, bool)
                mongo_alive = alive_initial and alive_after
            except Exception as e:
                print(f"Mongo alive: Error - {e}")
            finally:
                end_mongo_timing()

            # 11. collection
            start_mongo_timing()
            try:
                cursor = mongo_collection.find({})
                mongo_collection_prop = cursor.collection is mongo_collection
            except Exception as e:
                print(f"Mongo collection: Error - {e}")
            finally:
                end_mongo_timing()

            # 12. address
            start_mongo_timing()
            try:
                cursor = mongo_collection.find({})
                address_before = cursor.address is None
                list(cursor)
                address_after = (
                    isinstance(cursor.address, tuple)
                    and len(cursor.address) == 2
                )
                mongo_address = address_before and address_after
            except Exception as e:
                print(f"Mongo address: Error - {e}")
            finally:
                end_mongo_timing()

            # 13. min
            start_mongo_timing()
            try:
                cursor = (
                    mongo_collection.find({})
                    .hint("value_1")
                    .min([("value", 10)])
                )
                results = list(cursor)
                mongo_min = len(results) == 10 and all(
                    doc["value"] >= 10 for doc in results
                )
            except Exception as e:
                print(f"Mongo min(): Error - {e}")
            finally:
                end_mongo_timing()

            # 14. max
            start_mongo_timing()
            try:
                cursor = (
                    mongo_collection.find({})
                    .hint("value_1")
                    .max([("value", 10)])
                )
                results = list(cursor)
                mongo_max = len(results) == 10 and all(
                    doc["value"] < 10 for doc in results
                )
            except Exception as e:
                print(f"Mongo max(): Error - {e}")
            finally:
                end_mongo_timing()

            # 15. add_option
            start_mongo_timing()
            try:
                cursor = mongo_collection.find({})
                cursor.add_option(2)
                mongo_add_option = True
            except Exception as e:
                print(f"Mongo add_option: Error - {e}")
            finally:
                end_mongo_timing()

            # 16. remove_option
            start_mongo_timing()
            try:
                cursor = mongo_collection.find({})
                cursor.add_option(2)
                cursor.remove_option(2)
                mongo_remove_option = True
            except Exception as e:
                print(f"Mongo remove_option: Error - {e}")
            finally:
                end_mongo_timing()

            # 17. max_await_time_ms
            start_mongo_timing()
            try:
                cursor = mongo_collection.find({}).max_await_time_ms(100)
                mongo_max_await = True
            except Exception as e:
                print(f"Mongo max_await_time_ms: Error - {e}")
            finally:
                end_mongo_timing()

            # 18. session
            start_mongo_timing()
            try:
                cursor = mongo_collection.find({})
                mongo_session_prop = cursor.session is None
            except Exception as e:
                print(f"Mongo session: Error - {e}")
            finally:
                end_mongo_timing()

            # 19. cursor_id
            start_mongo_timing()
            try:
                cursor = mongo_collection.find({})
                mongo_cursor_id_prop = isinstance(
                    cursor.cursor_id, (int, type(None))
                )
            except Exception as e:
                print(f"Mongo cursor_id: Error - {e}")
            finally:
                end_mongo_timing()

            # 20. collation
            from pymongo.collation import Collation

            start_mongo_timing()
            try:
                cursor = mongo_collection.find({}).collation(
                    Collation(locale="en_US", strength=2)
                )
                mongo_collation = cursor is not None
            except Exception as e:
                print(f"Mongo collation(): Error - {e}")
            finally:
                end_mongo_timing()

            # 21. where
            start_mongo_timing()
            try:
                # Not supported as a method in PyMongo
                mongo_where = None
            finally:
                end_mongo_timing()

        finally:
            client.close()

    # Final reporting
    reporter.record_comparison(
        "Cursor Methods",
        "chained_methods",
        neo_cursor_methods if neo_cursor_methods else "FAIL",
        mongo_cursor_methods if mongo_cursor_methods else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Cursor Methods",
        "batch_size",
        neo_batch_size if neo_batch_size else "FAIL",
        mongo_batch_size if mongo_batch_size else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Cursor Methods",
        "hint",
        neo_hint if neo_hint else "FAIL",
        mongo_hint if mongo_hint else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Cursor Methods",
        "to_list",
        neo_to_list if neo_to_list else "FAIL",
        mongo_to_list if mongo_to_list else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Cursor Methods",
        "to_list_length",
        neo_to_list_length if neo_to_list_length else "FAIL",
        mongo_to_list_length if mongo_to_list_length else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Cursor Methods",
        "clone",
        neo_clone if neo_clone else "FAIL",
        mongo_clone if mongo_clone else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Cursor Methods",
        "explain",
        neo_explain if neo_explain else "FAIL",
        mongo_explain if mongo_explain else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Cursor Methods",
        "comment",
        neo_comment if neo_comment else "FAIL",
        mongo_comment if mongo_comment else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Cursor Methods",
        "retrieved",
        neo_retrieved if neo_retrieved else "FAIL",
        mongo_retrieved if mongo_retrieved else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Cursor Methods",
        "alive",
        neo_alive if neo_alive else "FAIL",
        mongo_alive if mongo_alive else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Cursor Methods",
        "collection",
        neo_collection_prop if neo_collection_prop else "FAIL",
        mongo_collection_prop if mongo_collection_prop else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Cursor Methods",
        "address",
        neo_address if neo_address else "FAIL",
        mongo_address if mongo_address else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Cursor Methods",
        "min",
        neo_min if neo_min else "FAIL",
        mongo_min if mongo_min else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Cursor Methods",
        "max",
        neo_max if neo_max else "FAIL",
        mongo_max if mongo_max else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Cursor Methods",
        "collation",
        neo_collation if neo_collation else "FAIL",
        mongo_collation if mongo_collation else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Cursor Methods",
        "where",
        neo_where if neo_where else "FAIL",
        mongo_where,
        skip_reason="NeoSQLite uses Python function filter; MongoDB uses JavaScript $where",
    )
    reporter.record_comparison(
        "Cursor Methods",
        "add_option",
        neo_add_option if neo_add_option else "FAIL",
        mongo_add_option if mongo_add_option else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Cursor Methods",
        "remove_option",
        neo_remove_option if neo_remove_option else "FAIL",
        mongo_remove_option if mongo_remove_option else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Cursor Methods",
        "max_await_time_ms",
        neo_max_await if neo_max_await else "FAIL",
        mongo_max_await if mongo_max_await else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Cursor Methods",
        "session",
        neo_session_prop if neo_session_prop else "FAIL",
        mongo_session_prop if mongo_session_prop else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Cursor Methods",
        "cursor_id",
        neo_cursor_id_prop if neo_cursor_id_prop else "FAIL",
        mongo_cursor_id_prop if mongo_cursor_id_prop else None,
        skip_reason="MongoDB not available" if not client else None,
    )
