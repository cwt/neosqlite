---
type: api_spec
title: "Change Streams with watch() in NeoSQLite"
description: "Change stream implementation using SQLite triggers and changelog tables for collection.watch() without replica sets."
tags:
  - change-streams
  - watch
  - triggers
  - events
timestamp: 2026-09-13T00:00:00Z
version: "1.16.0"
lifecycle: active
---

# Change Streams with watch() in NeoSQLite

NeoSQLite provides change stream functionality, similar to PyMongo's, through the `collection.watch()` method. This allows you to listen for data changes (inserts, updates, and deletes) in a collection.

## How It Works

The `watch()` feature is implemented using SQLite triggers. When you start a change stream, triggers are created on the collection's underlying table. These triggers capture any changes and record them in a dedicated `_neosqlite_changestream` table. The `ChangeStream` object then polls this table for new events.

> **v1.15.0 behavior:** Triggers are now **shared per collection** (reference-counted)
> and events are consumed via **per-stream watermarks** instead of being deleted on
> read — so concurrent streams on the same collection each see every event, and
> closing one stream no longer affects others. A new stream starts from "now" (it
> does not replay pre-open events) unless a resume token is given.
>
> **v1.15.2 behavior:** Streams support **`resume_after` / `start_after`** resume
> tokens, **library-side `$match` pipeline filtering**, and a **`resume_token`**
> property. When the last stream for a collection closes, triggers are dropped but
> the most recent changelog rows (bounded, currently 1000) are retained so a later
> `watch(resume_after=token)` can replay crash-restart gaps.

## Usage

The `watch()` method returns a `ChangeStream` object, which is an iterator. The recommended way to use it is with a `with` statement to ensure resources are properly cleaned up.

```python
with collection.watch() as change_stream:
    # Perform some operations on the collection
    collection.insert_one({'name': 'Alice'})
    collection.update_one({'name': 'Alice'}, {'$set': {'age': 30}})
    collection.delete_one({'name': 'Alice'})

    # Iterate over the changes
    for change in change_stream:
        print(change)
```

### Change Event Structure
The change events are designed to be similar to MongoDB's:

```json
{
    "_id": {"id": 1}, 
    "operationType": "insert", 
    "clusterTime": "2025-08-11 09:36:25", 
    "ns": {"db": "default", "coll": "users"}, 
    "documentKey": {"_id": 1}, 
    "fullDocument": {"name": "Alice", "age": 30, "_id": 1}
}
```

### Options
- **`full_document="updateLookup"`**: By default, `update` events only include the changes. Set this option to include the full document after the update. Filtering on `fullDocument.*` fields requires this option.
- **`max_await_time_ms`**: The maximum time to wait for new changes before the iterator raises `StopIteration`.
- **`pipeline`**: Library-side filtering. Only `$match` stages are evaluated (equality plus `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$exists` on dotted paths such as `operationType`, `ns.coll`, `fullDocument.status`, plus top-level `$and`/`$or`/`$nor`). Other stages are accepted and ignored.
- **`resume_after` / `start_after`**: Resume tokens (`change["_id"]`, i.e. `{"id": N}`). Replays changelog rows after the token instead of starting from "now". Mutually exclusive; malformed tokens raise `ValueError`.
- **`resume_token` property**: Token of the most recently returned event (`None` before the first event). Persist it to resume after a worker restart with zero repeats and zero misses within the retention window.

## Job-Queue Recipe (Celery Replacement)

Single worker process (not one per gunicorn worker) to avoid double delivery.
Triggers exist only while at least one stream is open (graceful close drops
them; a crash leaves them in place), so always drain on boot: the drain covers
events from stream-less windows, and the resume token replays everything
committed since the last processed event.

```python
from datetime import datetime, timezone

now = datetime.now(timezone.utc)
jobs.insert_one(
    {
        "type": "send",
        "iri": iri,
        "attempts": 0,
        "next_run": now,
        "status": "pending",
    }
)

# Worker boot: drain jobs that arrived before the stream opened.
due = jobs.find({"status": "pending", "next_run": {"$lte": now}})
for job in due:
    handle(job)

# Then tail new arrivals; restart with the last token after a crash.
stream = jobs.watch(
    pipeline=[{"$match": {"operationType": "insert"}}],
    full_document="updateLookup",
    resume_after=saved_token,  # or omit on first boot
    max_await_time_ms=5000,
)
for change in stream:
    saved_token = change["_id"]  # persist (same as stream.resume_token)
    handle(change["fullDocument"])
```

Backoff/retry stays app-side: `update_one(..., {"$inc": {"attempts": 1}, "$set": {"next_run": backoff, "status": "pending"}})`. See `examples/watch_job_queue.py` for a runnable version.

## Testing

The `watch()` feature is covered by change-stream, audit-isolation, and job-queue suites spanning:
- Basic insert, update, and delete operations.
- Full document lookups.
- Timeout and batching mechanisms.
- Context manager usage and resource cleanup.
- Error handling and edge cases.
- Resume-token restart (zero repeats, zero misses) and `$match` filtering.

## Limitations
- **Library-side filtering**: `$match` is evaluated in Python after polling, not pushed into SQL. Fine for job-queue throughput, not for high-volume streams.
- **Bounded resume window**: Only recent history (currently the last 1000 changelog rows per collection) survives the last close. Tokens older than that replay from the oldest retained row (possible duplicates); combine with the startup-drain recipe.
- **No pre-open replay by default**: New streams without a token start from "now".
- **Polling, not push**: Latency follows the poll interval; no cluster-wide resume, invalidate events, or operation-time semantics (`start_at_operation_time` is accepted and ignored).
- **Limited advanced options**: Features like `collation` and `session` are not implemented.
