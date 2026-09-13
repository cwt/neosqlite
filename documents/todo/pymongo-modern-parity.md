---
type: roadmap
title: "PyMongo Modern API Parity for micronote.pub"
description: "Remaining modern PyMongo gaps blocking micronote.pub after app-side modernization: find options kwargs, index options, drop_database."
tags:
  - pymongo
  - compatibility
  - roadmap
  - micronote
timestamp: 2026-09-13T00:00:00Z
version: "1.16.0"
lifecycle: proposed
status: implemented
sources:
  - neosqlite/collection/__init__.py
  - neosqlite/collection/cursor.py
  - neosqlite/connection.py
verified: machine-confirmed
stale_after: 2027-03-13T00:00:00Z
---

# PyMongo Modern API Parity for micronote.pub

## Overview

`micronote.pub` is being modernized to modern `PyMongo` / `GridFSBucket` usage, so no deprecated-API shims (`count`, `remove`, `update`, legacy `GridFS` field layout) are needed in `NeoSQLite`. This document tracks only the **modern-API** gaps that still block the migration.

## Gaps

| Missing API | micronote.pub call sites | Notes |
| :--- | :--- | :--- |
| `find(filter, limit=N, skip=M, sort=...)` kwargs | `activitypub.py` `build_inbox_json_feed`, `utils/query.py` `paginated_query` | Modern PyMongo accepts these; NeoSQLite `find()` swallows them in `**kwargs` |
| `create_index(key, expireAfterSeconds=...)` + other index options | `config.py` `cache2` TTL index | `TypeError` — option not accepted (expiry engine: [ttl-index-support.md](./ttl-index-support.md)) |
| `drop_database(name)` / `get_database(name)` | `config.py` `_drop_db`, `create_db_client` | `Connection` is both client and DB; needs compat helper for test/debug parity |

## Proposed Design

- Extend `find()` to honor `limit`, `skip`, `sort` kwargs by applying them to the returned `Cursor`.
- Extend `create_index()` / `IndexModel` to accept and store `expireAfterSeconds` (behavior per [ttl-index-support.md](./ttl-index-support.md)).
- Add `Connection.drop_database(name=None)` (drop all tables for test/debug) and `get_database(name=None)` (return `self`).

## Acceptance Criteria

- `micronote.pub` `config.create_indexes()` runs without `TypeError`.
- `find(q, limit=50).sort(...)` paginates identically to `mongo:8.2.12`.
- Differential test for `drop_database` round-trip in debug/test flow.

## References

- [TTL Index Support](./ttl-index-support.md)
- [Watch Job-Queue Hardening](./watch-job-queue-hardening.md)
- [PyMongo API Comparison](../pymongo-api-comparison.md)
