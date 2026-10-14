"""Module for comparing GridFS operations between NeoSQLite and PyMongo"""

from __future__ import annotations

import os
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

IS_NX27017_BACKEND = os.environ.get("NX27017_BACKEND", "").lower() == "true"


def compare_gridfs_operations():
    """Compare GridFS operations against PyMongo"""
    print("\n=== GridFS Operations Comparison ===")

    client = get_mongo_client()
    mongo_available = client is not None

    neo_results = {}
    with neosqlite.Connection(":memory:") as neo_conn:
        try:
            from neosqlite.gridfs import GridFSBucket

            # GridFSBucket accepts Connection directly (PyMongo parity)
            bucket = GridFSBucket(neo_conn, bucket_name="fs")
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

        set_accumulation_mode(True)
        if mongo_available:
            start_neo_timing()
        try:
            # 1. Upload & Download stream
            file_id = bucket.upload_from_stream("test.txt", b"Hello GridFS!")
            grid_out = bucket.open_download_stream(file_id)
            neo_results["upload_download"] = (
                grid_out.read() if grid_out else None
            )

            # Seed additional files with metadata
            for i in range(5):
                bucket.upload_from_stream(
                    f"doc_{i:02d}.txt",
                    f"content {i}".encode(),
                    metadata={"order": i, "category": "report"},
                )

            # 2. GridOutCursor pagination and sorting
            cursor = (
                bucket.find({"metadata.category": "report"})
                .sort("metadata.order", -1)
                .skip(1)
                .limit(2)
            )
            neo_results["cursor_sort_skip_limit"] = [
                (f.filename, f.metadata.get("order")) for f in cursor
            ]

            # 3. GridOutCursor to_list()
            cursor2 = bucket.find({"metadata.category": "report"}).sort(
                "metadata.order", 1
            )
            neo_results["cursor_to_list"] = [
                f.filename for f in cursor2.to_list(3)
            ]

            # 4. Collection count_documents on fs.files
            count_filtered = neo_conn.fs.files.count_documents(
                {"metadata.category": "report"}
            )
            count_total = neo_conn["fs.files"].count_documents({})
            neo_results["files_count_documents"] = (count_filtered, count_total)

            # 5. Collection count_documents on fs.chunks
            chunks_attr = neo_conn.fs.chunks.count_documents({})
            chunks_bracket = neo_conn["fs.chunks"].count_documents({})
            neo_results["chunks_count_documents"] = (
                chunks_attr == chunks_bracket and chunks_attr >= 6
            )

            print(
                f"Neo GridFS: upload_download={neo_results['upload_download'] == b'Hello GridFS!'}, "
                f"sort_skip_limit={len(neo_results['cursor_sort_skip_limit'])}, "
                f"to_list={len(neo_results['cursor_to_list'])}, "
                f"files_count={neo_results['files_count_documents']}"
            )
        except Exception as e:
            print(f"Neo GridFS: Error - {e}")
        finally:
            if mongo_available:
                end_neo_timing()

    mongo_results = {}
    if mongo_available:
        try:
            from gridfs import GridFSBucket as MongoGridFSBucket

            mongo_db = client.test_database
            for coll_name in ("fs.files", "fs.chunks"):
                try:
                    mongo_db.drop_collection(coll_name)
                except Exception:
                    pass

            bucket = MongoGridFSBucket(mongo_db, bucket_name="fs")

            # Warmup: PyMongo creates indexes on the first operation.
            warmup_id = bucket.upload_from_stream("warmup.txt", b"setup")
            bucket.delete(warmup_id)

            set_accumulation_mode(True)
            start_mongo_timing()
            try:
                # 1. Upload & Download stream
                file_id = bucket.upload_from_stream("test.txt", b"Hello GridFS!")
                grid_out = bucket.open_download_stream(file_id)
                mongo_results["upload_download"] = (
                    grid_out.read() if grid_out else None
                )

                # Seed additional files with metadata
                for i in range(5):
                    bucket.upload_from_stream(
                        f"doc_{i:02d}.txt",
                        f"content {i}".encode(),
                        metadata={"order": i, "category": "report"},
                    )

                # 2. GridOutCursor pagination and sorting
                cursor = (
                    bucket.find({"metadata.category": "report"})
                    .sort("metadata.order", -1)
                    .skip(1)
                    .limit(2)
                )
                mongo_results["cursor_sort_skip_limit"] = [
                    (f.filename, f.metadata.get("order")) for f in cursor
                ]

                # 3. GridOutCursor to_list()
                cursor2 = bucket.find({"metadata.category": "report"}).sort(
                    "metadata.order", 1
                )
                mongo_results["cursor_to_list"] = [
                    f.filename for f in cursor2.to_list(3)
                ]

                # 4. Collection count_documents on fs.files
                count_filtered = mongo_db.fs.files.count_documents(
                    {"metadata.category": "report"}
                )
                count_total = mongo_db["fs.files"].count_documents({})
                mongo_results["files_count_documents"] = (
                    count_filtered,
                    count_total,
                )

                # 5. Collection count_documents on fs.chunks
                chunks_attr = mongo_db.fs.chunks.count_documents({})
                chunks_bracket = mongo_db["fs.chunks"].count_documents({})
                mongo_results["chunks_count_documents"] = (
                    chunks_attr == chunks_bracket and chunks_attr >= 6
                )

                print(
                    f"Mongo GridFS: upload_download={mongo_results['upload_download'] == b'Hello GridFS!'}, "
                    f"sort_skip_limit={len(mongo_results['cursor_sort_skip_limit'])}, "
                    f"to_list={len(mongo_results['cursor_to_list'])}, "
                    f"files_count={mongo_results['files_count_documents']}"
                )
            except Exception as e:
                print(f"Mongo GridFS: Error - {e}")
            finally:
                end_mongo_timing()
        except Exception as e:
            print(f"Mongo GridFS: Setup Error - {e}")

    skip = "MongoDB/NX-27017 not available" if not mongo_available else None

    # Record comparisons for each GridFS capability
    reporter.record_comparison(
        "GridFS",
        "upload_download_stream",
        neo_results.get("upload_download"),
        mongo_results.get("upload_download"),
        skip_reason=skip,
    )
    reporter.record_comparison(
        "GridFS",
        "cursor_sort_skip_limit",
        neo_results.get("cursor_sort_skip_limit"),
        mongo_results.get("cursor_sort_skip_limit"),
        ignore_order=False,
        skip_reason=skip,
    )
    reporter.record_comparison(
        "GridFS",
        "cursor_to_list",
        neo_results.get("cursor_to_list"),
        mongo_results.get("cursor_to_list"),
        ignore_order=False,
        skip_reason=skip,
    )
    reporter.record_comparison(
        "GridFS",
        "files_count_documents",
        neo_results.get("files_count_documents"),
        mongo_results.get("files_count_documents"),
        skip_reason=skip,
    )
    reporter.record_comparison(
        "GridFS",
        "chunks_count_documents",
        neo_results.get("chunks_count_documents"),
        mongo_results.get("chunks_count_documents"),
        skip_reason=skip,
    )
