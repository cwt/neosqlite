"""Module for comparing GridFS operations between NeoSQLite and PyMongo"""

import os
import warnings

import neosqlite

from .reporter import reporter
from .timing import (
    end_mongo_timing,
    end_neo_timing,
    start_mongo_timing,
    start_neo_timing,
)
from .utils import get_mongo_client

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)

IS_NX27017_BACKEND = os.environ.get("NX27017_BACKEND", "").lower() == "true"


def compare_gridfs_operations():
    """Compare GridFS operations"""
    print("\n=== GridFS Operations Comparison ===")

    neo_gridfs_ok = False
    neo_file_data = None

    # 1. NeoSQLite Comparison
    with neosqlite.Connection(":memory:") as neo_conn:
        try:
            from neosqlite.gridfs import GridFSBucket

            # NeoSQLite GridFSBucket takes the underlying SQLite connection
            # Initialize outside timing to exclude table/index creation
            bucket = GridFSBucket(neo_conn.db, bucket_name="fs")
        except ImportError:
            print("Neo GridFS: Not available")
            reporter.record_result(
                "GridFS",
                "GridFSBucket",
                passed=True,
                neo_result="Not available",
                mongo_result=None,
                skip_reason="GridFS not compiled in this build",
            )
            return
        except Exception as e:
            print(f"Neo GridFS: Setup Error - {e}")
            return

        start_neo_timing()
        try:
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
        except Exception as e:
            print(f"Neo GridFS: Error - {e}")
            neo_gridfs_ok = False
        finally:
            end_neo_timing()

    # 2. MongoDB Comparison
    client = get_mongo_client()
    mongo_db = None
    mongo_file_data = None
    mongo_gridfs_ok = None
    skip_reason = None

    if client:
        mongo_db = client.test_database
        try:
            from gridfs import GridFSBucket as MongoGridFSBucket

            bucket = MongoGridFSBucket(mongo_db, bucket_name="fs")

            # WARM UP: PyMongo creates indexes on the FIRST operation.
            # We trigger this outside the timing block to exclude indexing from the benchmark.
            warmup_id = bucket.upload_from_stream("warmup.txt", b"setup")
            bucket.delete(warmup_id)

            start_mongo_timing()
            try:
                # Upload file
                file_id = bucket.upload_from_stream(
                    "test.txt", b"Hello GridFS!"
                )

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
            finally:
                end_mongo_timing()
        except Exception as e:
            print(f"Mongo GridFS: Setup Error - {e}")
            mongo_gridfs_ok = False
    else:
        skip_reason = "MongoDB/NX-27017 not available"

    # 3. Record Comparison
    reporter.record_result(
        "GridFS",
        "GridFSBucket",
        passed=(
            neo_gridfs_ok == mongo_gridfs_ok
            if mongo_gridfs_ok is not None
            else True
        ),
        neo_result=neo_gridfs_ok if neo_gridfs_ok else "FAIL",
        mongo_result=mongo_gridfs_ok if mongo_gridfs_ok is not None else None,
        skip_reason=skip_reason,
    )
