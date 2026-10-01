"""P0 differential check: NX-27017 vs real MongoDB 8.2.12 wire-vs-wire.

Runs only when ``NX_REAL_MONGO_URI`` is set (e.g. CI job with docker
``mongo:8.2.12`` on ``localhost:27018``). Both endpoints are exercised
exclusively through ``AsyncMongoClient``. Kernel/host/version field noise
never fails: only functional shape is compared.
"""

import os
import socket
import threading
import time

import pytest

REAL_URI = os.environ.get("NX_REAL_MONGO_URI", "")

pytestmark = pytest.mark.skipif(
    not REAL_URI, reason="NX_REAL_MONGO_URI not set (needs real MongoDB)"
)

IGNORED_KEYS = frozenset(
    {
        "host", "process", "pid", "localTime", "uptime", "uptimeMillis",
        "uptimeEstimate", "mem", "connections", "asserts", "globalLock",
        "storageEngine", "wiredTiger", "version", "gitVersion", "modules",
        "setName", "topologyVersion", "connectionId",
        "logicalSessionTimeoutMinutes", "maxBsonObjectSize",
        "maxMessageSizeBytes", "maxWriteBatchSize", "operationTime",
        "clusterTime", "electionId", "lastWrite",
    }
)


def normalize(value):
    cls_name = value.__class__.__name__
    if cls_name == "datetime":
        if getattr(value, "tzinfo", None) is not None:
            from datetime import timezone

            return value.astimezone(timezone.utc).replace(tzinfo=None)
        return value
    if cls_name == "ObjectId":
        return str(value)
    if isinstance(value, dict):
        if set(value) == {"$oid"}:
            return str(value["$oid"])
        return {
            key: normalize(item)
            for key, item in value.items()
            if key not in IGNORED_KEYS and key != "_id"
        }
    if isinstance(value, list):
        return [normalize(item) for item in value]
    return value


def _free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture
def nx_uri(tmp_path):
    from nx_27017.handler import NeoSQLiteHandler
    from nx_27017.server import run_server_threaded

    handler = NeoSQLiteHandler(str(tmp_path / "nx.db"))
    port = _free_port()
    thread = threading.Thread(
        target=run_server_threaded,
        args=("127.0.0.1", port, handler),
        kwargs={"use_threading": True},
        daemon=True,
    )
    thread.start()
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                break
        except OSError:
            time.sleep(0.1)
    else:
        raise RuntimeError("NX test server did not start")
    yield f"mongodb://127.0.0.1:{port}/"
    handler.close_all()


@pytest.mark.asyncio
async def test_wire_compat_core(nx_uri):
    from pymongo import AsyncMongoClient

    nx_client = AsyncMongoClient(nx_uri, serverSelectionTimeoutMS=5000)
    real_client = AsyncMongoClient(REAL_URI, serverSelectionTimeoutMS=5000)
    try:
        assert (await nx_client.admin.command("ping"))["ok"] == 1
        assert (await real_client.admin.command("ping"))["ok"] == 1
        nx_coll = nx_client["compat"]["core"]
        real_coll = real_client["compat"]["core"]
        await nx_coll.delete_many({})
        await real_coll.delete_many({})

        await nx_coll.insert_many([{"n": 1}, {"n": 2}])
        await real_coll.insert_many([{"n": 1}, {"n": 2}])
        assert normalize(
            await nx_coll.find({}).to_list(None)
        ) == normalize(await real_coll.find({}).to_list(None))
        assert await nx_coll.count_documents({}) == (
            await real_coll.count_documents({})
        )
        await nx_coll.update_one({"n": 1}, {"$set": {"n": 10}})
        await real_coll.update_one({"n": 1}, {"$set": {"n": 10}})
        assert normalize(
            await nx_coll.find_one({"n": 10})
        ) == normalize(await real_coll.find_one({"n": 10}))
        assert (await nx_coll.delete_many({})).deleted_count == (
            await real_coll.delete_many({})
        ).deleted_count
    finally:
        await nx_client.close()
        await real_client.close()
