---
type: api_spec
title: "TTL Indexes and Document Expiry in NeoSQLite"
description: "MongoDB-compatible expireAfterSeconds index declaration with lazy and background document expiry."
tags:
  - ttl
  - indexing
  - expiry
timestamp: 2026-09-13T00:00:00Z
version: "1.15.2"
lifecycle: active
---

# TTL Indexes and Document Expiry in NeoSQLite

MongoDB-compatible time-to-live declaration (`expireAfterSeconds`) with
best-effort expiry, for caches like `cache2` (`create_index("date",
expireAfterSeconds=3600 * 12)`).

## Declaration

```python
collection.create_index("date", expireAfterSeconds=43200)
collection.create_indexes(
    [IndexModel("date", expireAfterSeconds=43200)]
)
```

- Stored in `_neosqlite_ttl_indexes` metadata; visible via
  `index_information()["idx_<coll>_date"]["expireAfterSeconds"]`.
- Single-field indexes only (compound, FTS, and datetime-field indexes raise
  `ValueError`); range is `0`–`2147483647` seconds like MongoDB.
- Other index options (`name`, `background`, ...) are accepted and ignored.
- Short-term app workaround remains valid: explicit
  `delete_many({"date": {"$lt": cutoff}})` sweeps.

## Expiry Engine

- **Lazy + explicit (Phase 1):** `collection.purge_expired(now=None)` deletes
  documents where `now >= field_value + expireAfterSeconds` and returns the
  count. `find()` / `find_one()` on a TTL collection auto-purge best-effort
  before reading (never breaks reads). Pass an explicit `now` in tests instead
  of sleeping.
- **Background sweeper (Phase 2, opt-in):**
  `Connection(path, ttl_sweep_interval_s=60)` starts a daemon thread that
  sweeps all TTL collections each tick via a short-lived helper connection
  (the main `sqlite3` handle is never shared across threads). Disabled for
  `:memory:` databases; `sweep_ttl_once()` runs one manual pass.
- **Field types:** UTC datetimes (ISO strings) and numeric epoch seconds are
  both supported for the indexed field.
- **Semantics:** Deletions are plain deletes and fire normal `watch()` events.
  MongoDB guarantees ~60s background granularity; NeoSQLite guarantees lazy
  (on next read) plus sweeper-tick granularity.

## Maintenance Notes

- Dropping an index/collection clears its TTL metadata; renaming a collection
  carries it over.
- Expiry deletes free pages inside the file; reclaim disk space per the
  [Database Maintenance Guide](./database-maintenance.md) (auto-vacuum /
  `compact`).

## References

- [PyMongo API Comparison](./pymongo-api-comparison.md)
- [Change Streams with watch()](./watch.md)
- [Database Maintenance Guide](./database-maintenance.md)
