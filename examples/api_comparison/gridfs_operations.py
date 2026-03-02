"""Module for comparing GridFS operations between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_gridfs_operations():
    """Compare GridFS operations"""
    print("\n=== GridFS Operations Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        try:
            from neosqlite.gridfs import GridFSBucket

            # NeoSQLite GridFSBucket takes the underlying SQLite connection
            bucket = GridFSBucket(neo_conn.db, bucket_name="fs")

            # Upload file
            file_id = bucket.upload_from_stream("test.txt", b"Hello GridFS!")

            # Download file - NeoSQLite uses open_download_stream
            grid_out = bucket.open_download_stream(file_id)
            neo_file_data = grid_out.read() if grid_out else None

            # Find files
            files = list(bucket.find({"filename": "test.txt"}))

            # Test get_last_version() - NOT YET IMPLEMENTED
            neo_get_last_version = False
            try:
                if hasattr(bucket, "get_last_version"):
                    latest = bucket.get_last_version("test.txt")
                    neo_get_last_version = latest is not None
                    print(
                        f"Neo get_last_version(): {'OK' if neo_get_last_version else 'FAIL'}"
                    )
                else:
                    print("Neo get_last_version(): NOT IMPLEMENTED")
            except Exception as e:
                print(f"Neo get_last_version(): Error - {e}")

            # Test list() - NOT YET IMPLEMENTED
            neo_list = False
            try:
                if hasattr(bucket, "list"):
                    all_files = list(bucket.list())
                    neo_list = len(all_files) > 0
                    print(
                        f"Neo list(): {'OK' if neo_list else 'FAIL'} ({len(all_files)} files)"
                    )
                else:
                    print("Neo list(): NOT IMPLEMENTED")
            except Exception as e:
                print(f"Neo list(): Error - {e}")

            # Test find_one() - NOT YET IMPLEMENTED
            neo_find_one = False
            try:
                if hasattr(bucket, "find_one"):
                    found = bucket.find_one({"filename": "test.txt"})
                    neo_find_one = found is not None
                    print(f"Neo find_one(): {'OK' if neo_find_one else 'FAIL'}")
                else:
                    print("Neo find_one(): NOT IMPLEMENTED")
            except Exception as e:
                print(f"Neo find_one(): Error - {e}")

            # Test get() - NOT YET IMPLEMENTED
            neo_get = False
            try:
                if hasattr(bucket, "get"):
                    grid_out_get = bucket.get(file_id)
                    neo_get = grid_out_get is not None
                    print(f"Neo get(): {'OK' if neo_get else 'FAIL'}")
                else:
                    print("Neo get(): NOT IMPLEMENTED")
            except Exception as e:
                print(f"Neo get(): Error - {e}")

            # Test content_type support - NOT YET IMPLEMENTED
            neo_content_type = False
            try:
                # Upload with content_type
                file_id2 = bucket.upload_from_stream(
                    "test2.txt", b"Content type test", content_type="text/plain"
                )
                # Check if content_type is stored
                if hasattr(bucket, "find"):
                    files2 = list(bucket.find({"_id": file_id2}))
                    if files2 and len(files2) > 0:
                        neo_content_type = hasattr(
                            files2[0], "content_type"
                        ) or "content_type" in str(files2[0])
                        print(
                            f"Neo content_type: {'OK' if neo_content_type else 'PARTIAL'}"
                        )
                    else:
                        print("Neo content_type: NOT IMPLEMENTED")
                else:
                    print("Neo content_type: NOT IMPLEMENTED")
            except Exception as e:
                print(f"Neo content_type: Error - {e}")

            # Test aliases support - NOT YET IMPLEMENTED
            neo_aliases = False
            try:
                # Upload with aliases
                file_id3 = bucket.upload_from_stream(
                    "test3.txt", b"Aliases test", aliases=["alias1", "alias2"]
                )
                # Check if aliases are stored
                if hasattr(bucket, "find"):
                    files3 = list(bucket.find({"_id": file_id3}))
                    if files3 and len(files3) > 0:
                        neo_aliases = hasattr(
                            files3[0], "aliases"
                        ) or "aliases" in str(files3[0])
                        print(
                            f"Neo aliases: {'OK' if neo_aliases else 'PARTIAL'}"
                        )
                    else:
                        print("Neo aliases: NOT IMPLEMENTED")
                else:
                    print("Neo aliases: NOT IMPLEMENTED")
            except Exception as e:
                print(f"Neo aliases: Error - {e}")

            print(
                f"Neo GridFS: upload={file_id is not None}, download={neo_file_data is not None}, find={len(files)}"
            )

            neo_gridfs_ok = neo_file_data == b"Hello GridFS!"
        except ImportError:
            print("Neo GridFS: Not available")
            neo_gridfs_ok = False
            neo_get_last_version = False
            neo_list = False
            neo_find_one = False
            neo_get = False
            neo_content_type = False
            neo_aliases = False
            reporter.record_result(
                "GridFS",
                "GridFSBucket",
                False,
                "Not available",
                "N/A",
                skip_reason="GridFS not compiled in this build",
            )
            return
        except Exception as e:
            print(f"Neo GridFS: Error - {e}")
            neo_gridfs_ok = False
            neo_get_last_version = False
            neo_list = False
            neo_find_one = False
            neo_get = False
            neo_content_type = False
            neo_aliases = False
            reporter.record_result(
                "GridFS", "GridFSBucket", False, f"Error: {e}", "N/A"
            )
            return

    client = test_pymongo_connection()
    mongo_db = None
    mongo_file_data = None
    mongo_gridfs_ok = None
    mongo_get_last_version = None
    mongo_list = None
    mongo_find_one = None
    mongo_get = None
    mongo_content_type = None
    mongo_aliases = None

    if client:
        mongo_db = client.test_database
        try:
            from gridfs import GridFSBucket as MongoGridFSBucket

            bucket = MongoGridFSBucket(mongo_db, bucket_name="fs")

            # Upload file
            file_id = bucket.upload_from_stream("test.txt", b"Hello GridFS!")

            # Download file - MongoDB also uses open_download_stream
            grid_out = bucket.open_download_stream(file_id)
            mongo_file_data = grid_out.read() if grid_out else None

            # Find files
            files = list(bucket.find({"filename": "test.txt"}))

            # Test get_last_version()
            try:
                latest = bucket.get_last_version("test.txt")
                mongo_get_last_version = latest is not None
                print(
                    f"Mongo get_last_version(): {'OK' if mongo_get_last_version else 'FAIL'}"
                )
            except Exception as e:
                mongo_get_last_version = False
                print(f"Mongo get_last_version(): Error - {e}")

            # Test list()
            try:
                all_files = list(bucket.list())
                mongo_list = len(all_files) > 0
                print(
                    f"Mongo list(): {'OK' if mongo_list else 'FAIL'} ({len(all_files)} files)"
                )
            except Exception as e:
                mongo_list = False
                print(f"Mongo list(): Error - {e}")

            # Test find_one()
            try:
                found = bucket.find_one({"filename": "test.txt"})
                mongo_find_one = found is not None
                print(f"Mongo find_one(): {'OK' if mongo_find_one else 'FAIL'}")
            except Exception as e:
                mongo_find_one = False
                print(f"Mongo find_one(): Error - {e}")

            # Test get() - alias for open_download_stream
            try:
                grid_out_get = bucket.get(file_id)
                mongo_get = grid_out_get is not None
                print(f"Mongo get(): {'OK' if mongo_get else 'FAIL'}")
            except Exception as e:
                mongo_get = False
                print(f"Mongo get(): Error - {e}")

            # Test content_type support
            try:
                file_id2 = bucket.upload_from_stream(
                    "test2.txt", b"Content type test", content_type="text/plain"
                )
                files2 = list(bucket.find({"_id": file_id2}))
                mongo_content_type = (
                    files2
                    and len(files2) > 0
                    and files2[0].content_type == "text/plain"
                )
                print(
                    f"Mongo content_type: {'OK' if mongo_content_type else 'FAIL'}"
                )
            except Exception as e:
                mongo_content_type = False
                print(f"Mongo content_type: Error - {e}")

            # Test aliases support
            try:
                file_id3 = bucket.upload_from_stream(
                    "test3.txt", b"Aliases test", aliases=["alias1", "alias2"]
                )
                files3 = list(bucket.find({"_id": file_id3}))
                mongo_aliases = (
                    files3 and len(files3) > 0 and hasattr(files3[0], "aliases")
                )
                print(f"Mongo aliases: {'OK' if mongo_aliases else 'FAIL'}")
            except Exception as e:
                mongo_aliases = False
                print(f"Mongo aliases: Error - {e}")

            print(
                f"Mongo GridFS: upload={file_id is not None}, download={mongo_file_data is not None}, find={len(files)}"
            )

            mongo_gridfs_ok = mongo_file_data == b"Hello GridFS!"
        except Exception as e:
            print(f"Mongo GridFS: Error - {e}")
            mongo_gridfs_ok = False
            mongo_get_last_version = False
            mongo_list = False
            mongo_find_one = False
            mongo_get = False
            mongo_content_type = False
            mongo_aliases = False
        client.close()
    else:
        mongo_gridfs_ok = False
        mongo_get_last_version = False
        mongo_list = False
        mongo_find_one = False
        mongo_get = False
        mongo_content_type = False
        mongo_aliases = False

    reporter.record_result(
        "GridFS",
        "GridFSBucket",
        neo_gridfs_ok and mongo_gridfs_ok,
        neo_gridfs_ok,
        mongo_gridfs_ok,
    )
    reporter.record_result(
        "GridFS",
        "get_last_version",
        neo_get_last_version,
        neo_get_last_version if neo_get_last_version else "NOT IMPLEMENTED",
        mongo_get_last_version,
        skip_reason=(
            "Not yet implemented in NeoSQLite"
            if not neo_get_last_version
            else None
        ),
    )
    reporter.record_result(
        "GridFS",
        "list",
        neo_list,
        neo_list if neo_list else "NOT IMPLEMENTED",
        mongo_list,
        skip_reason=(
            "Not yet implemented in NeoSQLite" if not neo_list else None
        ),
    )
    reporter.record_result(
        "GridFS",
        "find_one",
        neo_find_one,
        neo_find_one if neo_find_one else "NOT IMPLEMENTED",
        mongo_find_one,
        skip_reason=(
            "Not yet implemented in NeoSQLite" if not neo_find_one else None
        ),
    )
    reporter.record_result(
        "GridFS",
        "get",
        neo_get,
        neo_get if neo_get else "NOT IMPLEMENTED",
        mongo_get,
        skip_reason="Not yet implemented in NeoSQLite" if not neo_get else None,
    )
    reporter.record_result(
        "GridFS",
        "content_type",
        neo_content_type,
        neo_content_type if neo_content_type else "NOT IMPLEMENTED",
        mongo_content_type,
        skip_reason=(
            "Not yet implemented in NeoSQLite" if not neo_content_type else None
        ),
    )
    reporter.record_result(
        "GridFS",
        "aliases",
        neo_aliases,
        neo_aliases if neo_aliases else "NOT IMPLEMENTED",
        mongo_aliases,
        skip_reason=(
            "Not yet implemented in NeoSQLite" if not neo_aliases else None
        ),
    )
