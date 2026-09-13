---
type: roadmap
title: "TTL Index and Document Expiry Support"
description: "MongoDB-compatible TTL index declaration (expireAfterSeconds) plus background expiry semantics needed by micronote.pub cache2."
tags:
  - ttl
  - indexing
  - expiry
  - roadmap
  - micronote
timestamp: 2026-09-13T00:00:00Z
version: "1.16.0"
lifecycle: proposed
status: implemented
sources:
  - neosqlite/collection/index_manager.py
  - neosqlite/collection/__init__.py
verified: machine-confirmed
stale_after: 2027-03-13T00:00:00Z
---

# TTL Index and Document Expiry Support

## Overview

`micronote.pub` caches rendered pages in `cache2` with a MongoDB TTL index (`config.py`: `create_index("date", expireAfterSeconds=3600 * 12)`). `NeoSQLite` has no TTL concept: `create_index()` rejects the option and nothing ever expires, so the cache grows forever and serves stale pages.

> Note: `micronote.pub` modernizes its PyMongo/GridFS usage but keeps the
> modern TTL declaration, so this proposal stays. Short-term the app can
> work around it with an explicit
> `delete_many({"date": {"$lt": cutoff}})` sweep (see `docs/migration.md`
> in micronote.pub); long-term the engine should own expiry.

## Gaps

| Need | Current state |
| :--- | :--- |
| Declare `create_index("date", expireAfterSeconds=N)` | `TypeError` — option not accepted |
| Background deletion of expired docs | No sweeper, no trigger, no lazy expiry |
| `index_information()` TTL visibility | No TTL metadata stored |
| `micronote.pub` `invalidate_cache()` interaction | Manual `delete_many({})` full wipes still work, but time-based expiry does not |

## Proposed Design

- **Declaration compat:**
  - Accept `expireAfterSeconds` in `create_index()` / `IndexModel`, persist `{field, expireAfterSeconds}` in `_neosqlite_indexes` metadata. No behavior change by itself.
- **Expiry engine (pick one, smallest first):**
  - Phase 1 — Lazy + explicit: `purge_expired()` helper per collection; auto-purge on `find`/`find_one` touching a TTL collection (best-effort, single `delete_many({field: {"$lt": now - ttl}})`).
  - Phase 2 — Background sweeper: opt-in `Connection(..., ttl_sweep_interval_s=60)` thread that scans TTL metadata and deletes expired batches.
- **Semantics:**
  - Second precision, UTC datetimes and numeric epoch both supported for the indexed field.
  - Deletions are plain deletes (no change-stream special casing beyond normal `watch()` events).
  - Document the difference: MongoDB guarantees ~60s background granularity; NeoSQLite guarantees lazy + sweeper-tick granularity.

## Acceptance Criteria

- `create_index("date", expireAfterSeconds=43200)` no longer raises; TTL metadata visible via `index_information()`.
- Expired `cache2` docs disappear within one sweep tick / on next read after expiry.
- Benchmark: sweep of 100k docs does not block writers beyond one WAL checkpoint.
- Differential test: `cache2` insert → sleep/advance clock → `find_one` returns `None`.

## References

- [PyMongo Modern API Parity](./pymongo-modern-parity.md)
- [Database Maintenance Guide](../database-maintenance.md)
- [Change Streams with watch()](../watch.md)
