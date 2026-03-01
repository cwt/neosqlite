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

            print(
                f"Neo GridFS: upload={file_id is not None}, download={neo_file_data is not None}, find={len(files)}"
            )

            neo_gridfs_ok = neo_file_data == b"Hello GridFS!"
        except ImportError:
            print("Neo GridFS: Not available")
            neo_gridfs_ok = False
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
            reporter.record_result(
                "GridFS", "GridFSBucket", False, f"Error: {e}", "N/A"
            )
            return

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_db = None

    mongo_file_data = None

    mongo_gridfs_ok = None

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

            print(
                f"Mongo GridFS: upload={file_id is not None}, download={mongo_file_data is not None}, find={len(files)}"
            )

            mongo_gridfs_ok = mongo_file_data == b"Hello GridFS!"
        except Exception as e:
            print(f"Mongo GridFS: Error - {e}")
            mongo_gridfs_ok = False
        client.close()
    else:
        mongo_gridfs_ok = False

    reporter.record_result(
        "GridFS",
        "GridFSBucket",
        neo_gridfs_ok and mongo_gridfs_ok,
        neo_gridfs_ok,
        mongo_gridfs_ok,
    )
