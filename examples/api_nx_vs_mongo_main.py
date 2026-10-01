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


async def _bulk_details(coll, models, ordered):
    """Run bulk models, returning comparable (counts, errors) details."""
    from pymongo.errors import BulkWriteError

    try:
        result = await coll.bulk_write(models, ordered=ordered)
        return {
            "inserted": result.inserted_count,
            "matched": result.matched_count,
            "modified": result.modified_count,
            "upserted": result.upserted_count,
            "errors": [],
        }
    except BulkWriteError as exc:
        details = exc.details
        return {
            "inserted": details.get("nInserted"),
            "matched": details.get("nMatched"),
            "modified": details.get("nModified"),
            "upserted": details.get("nUpserted"),
            "errors": [
                (w.get("index"), w.get("code"))
                for w in details.get("writeErrors", [])
            ],
        }


async def compare_bulk(nx_db, real_db):
    """Bulk write semantics: ordered/unordered errors, upserts."""
    from pymongo import DeleteOne, InsertOne, UpdateOne

    for coll in (nx_db["compat_bulk"], real_db["compat_bulk"]):
        try:
            await coll.drop()
        except Exception:
            pass
    check(
        "bulk.ordered-dup",
        await _bulk_details(
            nx_db["compat_bulk"],
            [
                InsertOne({"_id": 1}),
                InsertOne({"_id": 1}),
                UpdateOne({"_id": 1}, {"$set": {"v": 1}}),
            ],
            True,
        ),
        await _bulk_details(
            real_db["compat_bulk"],
            [
                InsertOne({"_id": 1}),
                InsertOne({"_id": 1}),
                UpdateOne({"_id": 1}, {"$set": {"v": 1}}),
            ],
            True,
        ),
    )
    for coll in (nx_db["compat_bulk"], real_db["compat_bulk"]):
        await coll.drop()
    check(
        "bulk.unordered-dup",
        await _bulk_details(
            nx_db["compat_bulk"],
            [
                InsertOne({"_id": 1}),
                InsertOne({"_id": 1}),
                UpdateOne({"_id": 1}, {"$set": {"v": 5}}),
            ],
            False,
        ),
        await _bulk_details(
            real_db["compat_bulk"],
            [
                InsertOne({"_id": 1}),
                InsertOne({"_id": 1}),
                UpdateOne({"_id": 1}, {"$set": {"v": 5}}),
            ],
            False,
        ),
    )
    for coll in (nx_db["compat_bulk"], real_db["compat_bulk"]):
        await coll.drop()
    check(
        "bulk.upsert",
        await _bulk_details(
            nx_db["compat_bulk"],
            [UpdateOne({"_id": 42}, {"$set": {"v": 1}}, upsert=True)],
            True,
        ),
        await _bulk_details(
            real_db["compat_bulk"],
            [UpdateOne({"_id": 42}, {"$set": {"v": 1}}, upsert=True)],
            True,
        ),
    )
    check(
        "estimated_document_count",
        await nx_db["compat_bulk"].estimated_document_count(),
        await real_db["compat_bulk"].estimated_document_count(),
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


async def _tx_outcome(coll):
    """Commit-then-abort observable outcome on one collection."""
    await coll.drop()
    async with coll.database.client.start_session() as session:
        await session.start_transaction()
        await coll.insert_one({"_id": 1}, session=session)
        await session.commit_transaction()
        committed = await coll.count_documents({})
        await session.start_transaction()
        await coll.insert_one({"_id": 2}, session=session)
        await session.abort_transaction()
        aborted = await coll.count_documents({})
    return {"committed": committed, "aborted": aborted}


async def _watch_events(coll, payloads):
    """Open a change stream, apply payloads, collect operation types."""
    import asyncio

    events = []
    stream = await coll.watch(max_await_time_ms=2000)
    try:
        writer = asyncio.create_task(_write_payloads(coll, payloads))
        try:
            async with asyncio.timeout(15):
                async for event in stream:
                    events.append(
                        (
                            event.get("operationType"),
                            (event.get("fullDocument") or {}).get("v"),
                        )
                    )
                    if len(events) >= len(payloads):
                        break
        except TimeoutError:
            pass
        await writer
    finally:
        await stream.close()
    return events


async def _write_payloads(coll, payloads):
    import asyncio

    await asyncio.sleep(0.5)
    for payload in payloads:
        await coll.insert_one(payload)


async def compare_tx_cs(nx_db, real_db, real_is_rs):
    """Transactions + change streams (real side needs a replica set)."""
    check(
        "tx.commit-abort",
        await _tx_outcome(nx_db["compat_tx"]),
        await _tx_outcome(real_db["compat_tx"])
        if real_is_rs
        else {"committed": 1, "aborted": 1},
    )
    if not real_is_rs:
        print("  SKIP changestream-vs-real (standalone has no change streams)")
        nx_events = await _watch_events(
            nx_db["compat_cs"], [{"v": "a"}, {"v": "b"}]
        )
        check(
            "changestream.nx-only",
            nx_events,
            [("insert", "a"), ("insert", "b")],
        )
        return
    check(
        "changestream.insert",
        await _watch_events(nx_db["compat_cs"], [{"v": "a"}, {"v": "b"}]),
        await _watch_events(real_db["compat_cs"], [{"v": "a"}, {"v": "b"}]),
    )


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
    try:
        hello = await real_client.admin.command("hello")
    except Exception:
        hello = {}
    real_is_rs = bool(hello.get("setName"))
    print(f"Real replica set: {hello.get('setName', 'standalone')}")

    nx_db = nx_client["compat"]
    real_db = real_client["compat"]
    print("== core ==")
    await compare_core(nx_db, real_db)
    print("== bulk ==")
    await compare_bulk(nx_db, real_db)
    print("== tx+changestream ==")
    await compare_tx_cs(nx_db, real_db, real_is_rs)
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
