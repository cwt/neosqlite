#!/usr/bin/env python3
"""
NX-27017 vs Real MongoDB wire-vs-wire comparison (Async API).

Both endpoints are exercised exclusively through the MongoDB wire protocol
using ``pymongo.AsyncMongoClient`` — no direct ``neosqlite.Connection`` use.
This is the counterpart of ``api_comparison_main.py`` (direct vs NX);
here the axis is NX (SQLite backend) vs real MongoDB 8.2.12.

Lenient mode (default, ``NX_COMPAT_LENIENT=true``): kernel/host/version
field noise never fails the build. Only functional shape is compared:
``ok/n/nModified/insertedIds/values/cursor.firstBatch`` after ObjectId
and datetime normalization.

Usage:
    NX_URI=mongodb://127.0.0.1:27017/ \\
    REAL_MONGO_URI=mongodb://127.0.0.1:27018/ \\
    python3 api_nx_vs_mongo_main.py
"""

from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, "..")

FAILED: list[str] = []
PASSED: list[str] = []

NX_URI = os.environ.get("NX_URI", "mongodb://127.0.0.1:27017/")
REAL_MONGO_URI = os.environ.get(
    "REAL_MONGO_URI", "mongodb://127.0.0.1:27018/"
)

# Volatile / kernel-specific response keys: compared leniently (ignored).
IGNORED_KEYS = frozenset(
    {
        "host",
        "process",
        "pid",
        "localTime",
        "uptime",
        "uptimeMillis",
        "uptimeEstimate",
        "mem",
        "connections",
        "asserts",
        "globalLock",
        "storageEngine",
        "wiredTiger",
        "version",
        "gitVersion",
        "modules",
        "setName",
        "topologyVersion",
        "connectionId",
        "logicalSessionTimeoutMinutes",
        "maxBsonObjectSize",
        "maxMessageSizeBytes",
        "maxWriteBatchSize",
        "connectionCount",
        "operationTime",
        "clusterTime",
        "electionId",
        "lastWrite",
    }
)


def normalize(value):
    """Recursively normalize docs for functional comparison."""
    if value is None:
        return None
    cls_name = value.__class__.__name__
    if cls_name == "datetime":
        if getattr(value, "tzinfo", None) is not None:
            from datetime import timezone

            return value.astimezone(timezone.utc).replace(tzinfo=None)
        return value
    if cls_name == "ObjectId":
        return str(value)
    if isinstance(value, dict):
        if len(value) == 1 and "$oid" in value:
            return str(value["$oid"])
        return {
            key: normalize(item)
            for key, item in value.items()
            if key not in IGNORED_KEYS and key != "_id"
        }
    if isinstance(value, list):
        return [normalize(item) for item in value]
    return value


def check(name, nx_result, real_result):
    """Compare normalized functional results; record pass/fail."""
    if normalize(nx_result) == normalize(real_result):
        PASSED.append(name)
        print(f"  PASS {name}")
    else:
        FAILED.append(name)
        print(f"  FAIL {name}")
        print(f"    NX:   {nx_result!r}")
        print(f"    real: {real_result!r}")


async def compare_core(nx_db, real_db):
    """Core CRUD + query surface exercised on both endpoints."""
    coll_nx = nx_db["compat_core"]
    coll_real = real_db["compat_core"]
    await coll_nx.delete_many({})
    await coll_real.delete_many({})

    docs = [
        {"name": "alice", "age": 30, "tags": ["a", "b"]},
        {"name": "bob", "age": 25, "tags": ["b"]},
    ]
    res_nx = await coll_nx.insert_many(docs)
    res_real = await coll_real.insert_many(
        [{"name": "alice", "age": 30, "tags": ["a", "b"]},
         {"name": "bob", "age": 25, "tags": ["b"]}]
    )
    check("insert_many.n", len(res_nx.inserted_ids), len(res_real.inserted_ids))

    found_nx = await coll_nx.find({"age": {"$gte": 26}}).to_list(None)
    found_real = await coll_real.find({"age": {"$gte": 26}}).to_list(None)
    check("find.filter", found_nx, found_real)

    one_nx = await coll_nx.find_one({"name": "bob"})
    one_real = await coll_real.find_one({"name": "bob"})
    check("find_one", one_nx, one_real)

    upd_nx = await coll_nx.update_one(
        {"name": "bob"}, {"$set": {"age": 26}}
    )
    upd_real = await coll_real.update_one(
        {"name": "bob"}, {"$set": {"age": 26}}
    )
    check(
        "update_one",
        (upd_nx.matched_count, upd_nx.modified_count),
        (upd_real.matched_count, upd_real.modified_count),
    )

    check(
        "count_documents",
        await coll_nx.count_documents({}),
        await coll_real.count_documents({}),
    )
    check(
        "distinct",
        sorted(await coll_nx.distinct("tags")),
        sorted(await coll_real.distinct("tags")),
    )

    # NOTE: AsyncCollection.aggregate is itself a coroutine in PyMongo Async.
    cur_nx = await coll_nx.aggregate([{"$group": {"_id": None, "n": {"$sum": 1}}}])
    cur_real = await coll_real.aggregate(
        [{"$group": {"_id": None, "n": {"$sum": 1}}}]
    )
    check(
        "aggregate.$group",
        await cur_nx.to_list(None),
        await cur_real.to_list(None),
    )

    del_nx = await coll_nx.delete_many({"age": {"$gte": 0}})
    del_real = await coll_real.delete_many({"age": {"$gte": 0}})
    check(
        "delete_many",
        del_nx.deleted_count,
        del_real.deleted_count,
    )


async def compare_admin(nx_client, real_client, nx_db, real_db):
    """Collection/index admin commands on both endpoints."""
    check(
        "ping",
        (await nx_client.admin.command("ping"))["ok"],
        (await real_client.admin.command("ping"))["ok"],
    )
    for _db in (nx_db, real_db):
        try:
            await _db.drop_collection("compat_admin")
        except Exception:
            pass
    await nx_db.create_collection("compat_admin")
    await real_db.create_collection("compat_admin")
    check(
        "list_collection_names",
        "compat_admin" in await nx_db.list_collection_names(),
        "compat_admin" in await real_db.list_collection_names(),
    )
    await nx_db["compat_admin"].create_index([("age", 1)])
    await real_db["compat_admin"].create_index([("age", 1)])
    # NOTE: AsyncCollection.list_indexes is a coroutine in PyMongo Async.
    nx_indexes = await (
        await nx_db["compat_admin"].list_indexes()
    ).to_list(None)
    real_indexes = await (
        await real_db["compat_admin"].list_indexes()
    ).to_list(None)
    check("list_indexes.nonempty", len(nx_indexes) >= 1, len(real_indexes) >= 1)
    await nx_db["compat_admin"].drop()
    await real_db["compat_admin"].drop()


async def run_all():
    from pymongo import AsyncMongoClient

    print(f"NX endpoint:   {NX_URI}")
    print(f"Real endpoint: {REAL_MONGO_URI} (8.2.12, lenient kernel check)")
    nx_client = AsyncMongoClient(NX_URI, serverSelectionTimeoutMS=5000)
    real_client = AsyncMongoClient(
        REAL_MONGO_URI, serverSelectionTimeoutMS=5000
    )
    try:
        await nx_client.admin.command("ping")
    except Exception as exc:
        print(f"NX-27017 unreachable at {NX_URI}: {exc}")
        return 2
    try:
        await real_client.admin.command("ping")
    except Exception as exc:
        print(f"Real MongoDB unreachable at {REAL_MONGO_URI}: {exc}")
        print("Hint: rerun the shell wrapper with --with-podman.")
        return 2

    nx_db = nx_client["compat"]
    real_db = real_client["compat"]
    print("== core ==")
    await compare_core(nx_db, real_db)
    print("== admin ==")
    await compare_admin(nx_client, real_client, nx_db, real_db)

    print(f"\nPassed: {len(PASSED)}, failed: {len(FAILED)}")
    for name in FAILED:
        print(f"  FAILED: {name}")
    await nx_client.close()
    await real_client.close()
    return 1 if FAILED else 0


def main():
    return asyncio.run(run_all())


if __name__ == "__main__":
    sys.exit(main())
