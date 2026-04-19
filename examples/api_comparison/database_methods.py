"""Module for comparing database methods between NeoSQLite and PyMongo"""

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

# Track if compact test has run (for benchmark - only run once per benchmark session)
_compact_test_run = False


def compare_database_methods():
    """Compare database/connection methods"""
    print("\n=== Database Methods Comparison ===")

    # Check MongoDB availability FIRST to determine if we should time operations
    client = get_mongo_client()
    mongo_available = client is not None

    # Initialize NeoSQLite result variables
    neo_client = False
    neo_db_path = False
    neo_get_collection = False
    neo_create_collection = False
    neo_list_collections = False
    neo_drop_collection = False
    neo_rename_collection = False
    neo_ping_ok = False
    neo_server_status_ok = False
    neo_db_stats_ok = False
    neo_cursor_command = False
    neo_deref = False
    neo_with_options = False

    with neosqlite.Connection(":memory:") as neo_conn:
        set_accumulation_mode(True)

        # 1. Test client property - ONLY time if MongoDB is available
        if mongo_available:
            start_neo_timing()
        try:
            neo_client = neo_conn.client == neo_conn
        except Exception as e:
            print(f"Neo client property: Error - {e}")
        finally:
            if mongo_available:
                end_neo_timing()
        print(f"Neo client property: {'OK' if neo_client else 'FAIL'}")

        # 2. Test db_path property (NeoSQLite specific - SKIPPED from benchmark)
        # Don't time this - MongoDB doesn't have this feature
        try:
            neo_db_path = neo_conn.db_path == ":memory:"
        except Exception as e:
            print(f"Neo db_path property: SKIPPED (NeoSQLite specific) - {e}")
        print(
            f"Neo db_path property: {'OK' if neo_db_path else 'FAIL'} (not timed)"
        )

        # 3. Test get_collection - ONLY time if MongoDB is available
        if mongo_available:
            start_neo_timing()
        try:
            coll = neo_conn.get_collection("test_get_coll")
            neo_get_collection = (
                coll is not None and coll.name == "test_get_coll"
            )
        except Exception as e:
            print(f"Neo get_collection: Error - {e}")
        finally:
            if mongo_available:
                end_neo_timing()
        print(f"Neo get_collection: {'OK' if neo_get_collection else 'FAIL'}")

        # 4. Test create_collection - ONLY time if MongoDB is available
        if mongo_available:
            start_neo_timing()
        try:
            new_coll = neo_conn.create_collection("test_create_coll")
            new_coll.insert_one({"name": "test"})
            neo_create_collection = new_coll is not None
        except Exception as e:
            print(f"Neo create_collection: Error - {e}")
        finally:
            if mongo_available:
                end_neo_timing()
        print(
            f"Neo create_collection: {'OK' if neo_create_collection else 'FAIL'}"
        )

        # 5. Test list_collection_names - ONLY time if MongoDB is available
        if mongo_available:
            start_neo_timing()
        try:
            names = neo_conn.list_collection_names()
            user_collections = [n for n in names if not n.startswith("sqlite_")]
            neo_list_collections = len(user_collections) >= 1
            print(
                f"Neo list_collection_names: {len(user_collections)} user collections"
            )
        except Exception as e:
            print(f"Neo list_collection_names: Error - {e}")
        finally:
            if mongo_available:
                end_neo_timing()

        # 6. Test drop_collection - ONLY time if MongoDB is available
        if mongo_available:
            start_neo_timing()
        try:
            neo_conn.drop_collection("test_create_coll")
            names = neo_conn.list_collection_names()
            neo_drop_collection = "test_create_coll" not in names
        except Exception as e:
            print(f"Neo drop_collection: Error - {e}")
        finally:
            if mongo_available:
                end_neo_timing()
        print(f"Neo drop_collection: {'OK' if neo_drop_collection else 'FAIL'}")

        # 7. Test rename_collection - ONLY time if MongoDB is available
        try:
            neo_coll_rename = neo_conn.create_collection("rename_old")
            neo_coll_rename.insert_one({"name": "rename_test"})
            if mongo_available:
                start_neo_timing()
            try:
                neo_conn.rename_collection("rename_old", "rename_new")
            finally:
                if mongo_available:
                    end_neo_timing()
            names = neo_conn.list_collection_names()
            neo_rename_collection = (
                "rename_new" in names and "rename_old" not in names
            )
        except Exception as e:
            print(f"Neo rename_collection: Error - {e}")
        print(
            f"Neo rename_collection: {'OK' if neo_rename_collection else 'FAIL'}"
        )

        # 8. Test command() - ping - ONLY time if MongoDB is available
        if mongo_available:
            start_neo_timing()
        try:
            neo_ping = neo_conn.command("ping")
            neo_ping_ok = neo_ping.get("ok") == 1.0
        except Exception as e:
            print(f"Neo command('ping'): Error - {e}")
        finally:
            if mongo_available:
                end_neo_timing()
        print(f"Neo command('ping'): {'OK' if neo_ping_ok else 'FAIL'}")

        # 9. Test command() - serverStatus - ONLY time if MongoDB is available
        if mongo_available:
            start_neo_timing()
        try:
            neo_server_status = neo_conn.command("serverStatus")
            neo_server_status_ok = neo_server_status.get("ok") == 1.0
        except Exception as e:
            print(f"Neo command('serverStatus'): Error - {e}")
        finally:
            if mongo_available:
                end_neo_timing()
        print(
            f"Neo command('serverStatus'): {'OK' if neo_server_status_ok else 'FAIL'}"
        )

        # 10. Test command() - dbStats - ONLY time if MongoDB is available
        try:
            neo_coll_for_stats = neo_conn.create_collection("stats_test_coll")
            neo_coll_for_stats.insert_one({"name": "test", "value": 123})
            neo_coll_for_stats.create_index("value")
            if mongo_available:
                start_neo_timing()
            try:
                neo_db_stats = neo_conn.command("dbStats")
            finally:
                if mongo_available:
                    end_neo_timing()
            neo_db_stats_ok = (
                isinstance(neo_db_stats, dict) and neo_db_stats.get("ok") == 1.0
            )
        except Exception as e:
            print(f"Neo command('dbStats'): Error - {e}")
        print(f"Neo command('dbStats'): {'OK' if neo_db_stats_ok else 'FAIL'}")

        # 11. Test cursor_command() - ONLY time if MongoDB is available
        try:
            neo_conn.create_collection("cursor_test_coll").insert_one({"a": 1})
            if mongo_available:
                start_neo_timing()
            try:
                cursor = neo_conn.cursor_command("listCollections")
                results = list(cursor)
                cursor_pragma = neo_conn.cursor_command(
                    "table_info", table="cursor_test_coll"
                )
                results_pragma = list(cursor_pragma)
                neo_cursor_command = (
                    len(results) > 0 and len(results_pragma) > 0
                )
            finally:
                if mongo_available:
                    end_neo_timing()
        except Exception as e:
            print(f"Neo cursor_command(): Error - {e}")
        print(f"Neo cursor_command(): {'OK' if neo_cursor_command else 'FAIL'}")

        # 12. Test dereference() - ONLY time if MongoDB is available
        try:
            coll = neo_conn["deref_test"]
            res = coll.insert_one({"a": 1})
            dbref = {"$ref": "deref_test", "$id": res.inserted_id}
            if mongo_available:
                start_neo_timing()
            try:
                doc = neo_conn.dereference(dbref)
                neo_deref = doc is not None and doc.get("a") == 1
            finally:
                if mongo_available:
                    end_neo_timing()
        except Exception as e:
            print(f"Neo dereference(): Error - {e}")
        print(f"Neo dereference(): {'OK' if neo_deref else 'FAIL'}")

        # 13. Test with_options() - ONLY time if MongoDB is available
        if mongo_available:
            start_neo_timing()
        try:
            neo_db_opts = neo_conn.with_options(write_concern={"w": "majority"})
            neo_with_options = (
                neo_db_opts is not None
                and neo_db_opts.write_concern == {"w": "majority"}
            )
        except Exception as e:
            print(f"Neo with_options(): Error - {e}")
        finally:
            if mongo_available:
                end_neo_timing()
        print(f"Neo with_options(): {'OK' if neo_with_options else 'FAIL'}")

    # Initialize MongoDB result variables
    mongo_client = False
    mongo_get_collection = False
    mongo_create_collection = False
    mongo_list_collections = False
    mongo_drop_collection = False
    mongo_rename_collection = False
    mongo_ping_ok = False
    mongo_server_status_ok = False
    mongo_db_stats_ok = False
    mongo_cursor_command = False
    mongo_deref = False
    mongo_with_options = False

    if mongo_available:
        client = get_mongo_client()
        if client:
            set_accumulation_mode(True)
            mongo_db = client.test_database_methods

            # Cleanup
            for coll_name in [
                "test_get_coll",
                "test_create_coll",
                "rename_old",
                "rename_new",
                "deref_test",
                "stats_test_coll_mongo",
            ]:
                try:
                    mongo_db.drop_collection(coll_name)
                except:
                    pass

            # 1. Test client property
            start_mongo_timing()
            try:
                mongo_client = mongo_db.client == client
            except Exception as e:
                print(f"Mongo client property: Error - {e}")
            finally:
                end_mongo_timing()
            print(f"Mongo client property: {'OK' if mongo_client else 'FAIL'}")

            # 2. Test get_collection
            start_mongo_timing()
            try:
                coll = mongo_db.get_collection("test_get_coll")
                mongo_get_collection = (
                    coll is not None and coll.name == "test_get_coll"
                )
            except Exception as e:
                print(f"Mongo get_collection: Error - {e}")
            finally:
                end_mongo_timing()
            print(
                f"Mongo get_collection: {'OK' if mongo_get_collection else 'FAIL'}"
            )

            # 3. Test create_collection
            start_mongo_timing()
            try:
                new_coll = mongo_db.create_collection("test_create_coll")
                new_coll.insert_one({"name": "test"})
                mongo_create_collection = new_coll is not None
            except Exception as e:
                print(f"Mongo create_collection: Error - {e}")
            finally:
                end_mongo_timing()
            print(
                f"Mongo create_collection: {'OK' if mongo_create_collection else 'FAIL'}"
            )

            # 4. Test list_collection_names
            start_mongo_timing()
            try:
                names = mongo_db.list_collection_names()
                mongo_list_collections = len(names) >= 1
                print(f"Mongo list_collection_names: {len(names)} collections")
            except Exception as e:
                print(f"Mongo list_collection_names: Error - {e}")
            finally:
                end_mongo_timing()

            # 5. Test drop_collection
            start_mongo_timing()
            try:
                mongo_db.drop_collection("test_create_coll")
                names = mongo_db.list_collection_names()
                mongo_drop_collection = "test_create_coll" not in names
            except Exception as e:
                print(f"Mongo drop_collection: Error - {e}")
            finally:
                end_mongo_timing()
            print(
                f"Mongo drop_collection: {'OK' if mongo_drop_collection else 'FAIL'}"
            )

            # 6. Test rename_collection (via collection.rename)
            try:
                mongo_coll_rename = mongo_db.create_collection("rename_old")
                mongo_coll_rename.insert_one({"name": "rename_test"})
                start_mongo_timing()
                try:
                    mongo_coll_rename.rename("rename_new")
                finally:
                    end_mongo_timing()
                names = mongo_db.list_collection_names()
                mongo_rename_collection = (
                    "rename_new" in names and "rename_old" not in names
                )
            except Exception as e:
                print(f"Mongo collection.rename(): Error - {e}")
            print(
                f"Mongo collection.rename(): {'OK' if mongo_rename_collection else 'FAIL'}"
            )

            # 7. Test command() - ping
            start_mongo_timing()
            try:
                mongo_ping = mongo_db.command("ping")
                mongo_ping_ok = mongo_ping.get("ok") == 1.0
            except Exception as e:
                print(f"Mongo command('ping'): Error - {e}")
            finally:
                end_mongo_timing()
            print(f"Mongo command('ping'): {'OK' if mongo_ping_ok else 'FAIL'}")

            # 8. Test command() - serverStatus
            start_mongo_timing()
            try:
                mongo_server_status = mongo_db.command("serverStatus")
                mongo_server_status_ok = mongo_server_status.get("ok") == 1.0
            except Exception as e:
                print(f"Mongo command('serverStatus'): Error - {e}")
            finally:
                end_mongo_timing()
            print(
                f"Mongo command('serverStatus'): {'OK' if mongo_server_status_ok else 'FAIL'}"
            )

            # 9. Test command() - dbStats
            try:
                mongo_coll_for_stats = mongo_db["stats_test_coll_mongo"]
                mongo_coll_for_stats.insert_one({"name": "test", "value": 123})
                start_mongo_timing()
                try:
                    mongo_db_stats = mongo_db.command("dbStats")
                finally:
                    end_mongo_timing()
                mongo_db_stats_ok = (
                    isinstance(mongo_db_stats, dict)
                    and mongo_db_stats.get("ok") == 1.0
                )
            except Exception as e:
                print(f"Mongo command('dbStats'): Error - {e}")
            print(
                f"Mongo command('dbStats'): {'OK' if mongo_db_stats_ok else 'FAIL'}"
            )

            # 10. Test cursor_command()
            try:
                from pymongo.command_cursor import CommandCursor

                start_mongo_timing()
                try:
                    cursor = mongo_db.cursor_command("listCollections")
                    mongo_cursor_command = isinstance(
                        cursor, CommandCursor
                    ) or (isinstance(cursor, dict) and "cursor" in cursor)
                finally:
                    end_mongo_timing()
            except Exception as e:
                print(f"Mongo cursor_command(): Error - {e}")
            print(
                f"Mongo cursor_command(): {'OK' if mongo_cursor_command else 'FAIL'}"
            )

            # 11. Test dereference()
            try:
                from bson.dbref import DBRef

                coll = mongo_db["deref_test"]
                res = coll.insert_one({"a": 1})
                dbref = DBRef("deref_test", res.inserted_id)
                start_mongo_timing()
                try:
                    doc = mongo_db.dereference(dbref)
                    mongo_deref = doc is not None and doc.get("a") == 1
                finally:
                    end_mongo_timing()
            except Exception as e:
                print(f"Mongo dereference(): Error - {e}")
            print(f"Mongo dereference(): {'OK' if mongo_deref else 'FAIL'}")

            # 12. Test with_options()
            try:
                from pymongo import WriteConcern

                start_mongo_timing()
                try:
                    mongo_db_opts = mongo_db.with_options(
                        write_concern=WriteConcern(w="majority")
                    )
                    mongo_with_options = mongo_db_opts is not None
                finally:
                    end_mongo_timing()
            except Exception as e:
                print(f"Mongo with_options(): Error - {e}")
            print(
                f"Mongo with_options(): {'OK' if mongo_with_options else 'FAIL'}"
            )

            # Cleanup
            for coll_name in [
                "test_get_coll",
                "rename_new",
                "deref_test",
                "stats_test_coll_mongo",
            ]:
                try:
                    mongo_db.drop_collection(coll_name)
                except:
                    pass

    # Record comparisons - only for operations that were actually timed
    reporter.record_comparison(
        "Database Methods",
        "client",
        neo_client if neo_client else "FAIL",
        mongo_client if mongo_client else None,
        skip_reason="MongoDB not available" if not mongo_available else None,
    )
    reporter.record_comparison(
        "Database Methods",
        "db_path",
        neo_db_path if neo_db_path else "FAIL",
        None,
        skip_reason="NeoSQLite specific (SQLite database path) - not timed in benchmark",
    )
    reporter.record_comparison(
        "Database Methods",
        "get_collection",
        neo_get_collection if neo_get_collection else "FAIL",
        mongo_get_collection if mongo_get_collection else None,
        skip_reason="MongoDB not available" if not mongo_available else None,
    )
    reporter.record_comparison(
        "Database Methods",
        "create_collection",
        neo_create_collection if neo_create_collection else "FAIL",
        mongo_create_collection if mongo_create_collection else None,
        skip_reason="MongoDB not available" if not mongo_available else None,
    )
    reporter.record_comparison(
        "Database Methods",
        "list_collection_names",
        neo_list_collections if neo_list_collections else "FAIL",
        mongo_list_collections if mongo_list_collections else None,
        skip_reason="MongoDB not available" if not mongo_available else None,
    )
    reporter.record_comparison(
        "Database Methods",
        "drop_collection",
        neo_drop_collection if neo_drop_collection else "FAIL",
        mongo_drop_collection if mongo_drop_collection else None,
        skip_reason="MongoDB not available" if not mongo_available else None,
    )
    reporter.record_comparison(
        "Database Methods",
        "rename_collection",
        neo_rename_collection if neo_rename_collection else "FAIL",
        mongo_rename_collection if mongo_rename_collection else None,
        skip_reason="MongoDB not available" if not mongo_available else None,
    )
    reporter.record_comparison(
        "Database Methods",
        "command_ping",
        neo_ping_ok if neo_ping_ok else "FAIL",
        mongo_ping_ok if mongo_ping_ok else None,
        skip_reason="MongoDB not available" if not mongo_available else None,
    )
    reporter.record_comparison(
        "Database Methods",
        "command_server_status",
        neo_server_status_ok if neo_server_status_ok else "FAIL",
        mongo_server_status_ok if mongo_server_status_ok else None,
        skip_reason="MongoDB not available" if not mongo_available else None,
    )
    reporter.record_comparison(
        "Database Methods",
        "command_db_stats",
        neo_db_stats_ok if neo_db_stats_ok else "FAIL",
        mongo_db_stats_ok if mongo_db_stats_ok else None,
        skip_reason="MongoDB not available" if not mongo_available else None,
    )
    reporter.record_comparison(
        "Database Methods",
        "cursor_command",
        neo_cursor_command if neo_cursor_command else "FAIL",
        mongo_cursor_command if mongo_cursor_command else None,
        skip_reason="MongoDB not available" if not mongo_available else None,
    )
    reporter.record_comparison(
        "Database Methods",
        "dereference",
        neo_deref if neo_deref else "FAIL",
        mongo_deref if mongo_deref else None,
        skip_reason="MongoDB not available" if not mongo_available else None,
    )
    reporter.record_comparison(
        "Database Methods",
        "with_options",
        neo_with_options if neo_with_options else "FAIL",
        mongo_with_options if mongo_with_options else None,
        skip_reason="MongoDB not available" if not mongo_available else None,
    )
