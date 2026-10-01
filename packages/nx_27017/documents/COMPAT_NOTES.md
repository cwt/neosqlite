# NX-27017 Wire Compat Notes

Mapping table and unsupported/lenient behavior list. This document is
the acceptance deliverable from `IMPROVEMENT_PLAN.md` §5 ("mapping
table + unsupported-ops list in `documents/`"); `README.md` links to
it instead of duplicating the details.

## 1. Namespace mapping

| Wire namespace | Storage |
|----------------|---------|
| `client.<db>.<coll>` | `<data-dir>/<safe_db>.db` table `<coll>` (files mode) |
| `client.<db>.<coll>` | isolated `Connection(":memory:")` per db (memory mode) |
| `client.<db>.<coll>` | one shared file/`:memory:` conn, all logical dbs alias it (single-file compat mode) |
| `admin` | always the connection backing `db_path`/`admin.db` |
| `db.<bucket>.files` / `db.<bucket>.chunks` | `<safe_db>.db` tables `<bucket>_files` / `<bucket>_chunks` (GridFS) |

- Logical db names must match `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`;
  anything else gets `ok:0` (`_sanitize_db_name`).
- Direct-user parity: `Connection("<data-dir>/test.db")["coll"]` sees
  exactly what `client.test.coll` sees (files mode).
- Sessions are keyed `(lsid, db)` in files/memory mode and shared in
  single-file compat mode. Transactions are single-database: an lsid
  with an open transaction gets an explicit `ok:0` (cross-DB error)
  when touching another database.
- Files mode caps simultaneously open per-file connections at 100
  (`_MAX_OPEN_DBS`); least-recently-used databases are closed on
  demand and reopened transparently. `admin` is never evicted, and
  databases with an open transaction or an active change stream are
  skipped. Memory mode never evicts (closing `:memory:` destroys
  data).
- `dropDatabase` closes and deletes `<safe_db>.db` (files mode);
  `listDatabases` scans `*.db` and reports real `sizeOnDisk`/`empty`.

## 2. Wire commands

| Command | Status |
|---------|--------|
| CRUD (`insert/find/update/delete/count/distinct/aggregate`) | Full, with real cursor pagination (`batchSize`, `getMore`, `killCursors`, 16MB batch cap) |
| `bulkWrite` (client-level + collection `bulk_write`) | Ordered/unordered, per-op `writeErrors`, `upserted` |
| `estimated_document_count` | Served by the `count` verb with no filter (exact count; SQLite has no cardinality estimator) |
| `createIndexes/dropIndexes/listIndexes` | Full, including `expireAfterSeconds` TTL round-trip |
| `createSearchIndexes/updateSearchIndex/dropSearchIndex` | Mapped onto NeoSQLite FTS; `$listSearchIndexes` served from FTS metadata |
| `findAndModify`, `renameCollection`, `create/drop`, `dropDatabase` | Full |
| `vacuum`, `compact` (`dryRun`, `freeSpaceTargetMB`), `validate`, `reIndex`/`reindex` | Explicit routes onto NeoSQLite `Connection.command` |
| `startSession/endSessions/commitTransaction/abortTransaction` | Single-database ACID; commit/abort resolve across db scopes (drivers route them to `admin.$cmd`) |
| `writeConcern` | Mapped to SQLite PRAGMA synchronous per database: `w:0`→OFF, `w:1`→NORMAL, `j:true`→FULL; `w:majority`/`w:2+`/`wtimeoutMS` accepted without effect |
| `readConcern` | Accepted (any level); single-node SQLite always reads latest committed data |
| `$changeStream` | Trigger-backed changelog, deterministic `{"_data": "nx:<rowid>"}` tokens, `resumeAfter`/`startAfter` replay, `$match`, `fullDocument` modes |
| GridFS | Full legacy (`GridFS.put/get_version/exists/new_file/list/delete`) and bucket API (`upload/download/rename/delete/delete_by_name/rename_by_name/upload_with_id/list`) over `fs.files`/`fs.chunks` wire ops |
| `OP_QUERY` | Frozen legacy fallback (find-only); modern drivers use OP_MSG |
| `explain` | Find-only approximation (`COLLSCAN` shape) |

## 3. Accepted-but-lenient options

Same posture as the live differential: accept-and-ignore parity with
real MongoDB unless noted.

| Option | Behavior |
|--------|----------|
| `collation` | Accepted; non-default-locale ordering not guaranteed |
| `comment`, `maxTimeMS` | Accepted; `maxTimeMS` is not enforced |
| `hint` | Valid hints forwarded; invalid hints ignored (real MongoDB errors) |
| `allowDiskUse`, `let`, `bypassDocumentValidation` | Accepted, no effect (SQLite has no spill/OCC semantics to tune) |
| `arrayFilters` | Forwarded on update/findAndModify/bulkWrite; nested-field filters remain a core gap (documented in core tests) |
| Invalid hints, >16MB single documents | Returned whole / lenient where real MongoDB errors (documented leniencies) |

## 4. Unsupported (over the wire)

- `find_raw_batches` / `aggregate_raw_batches` — no wire verb exists on
  either side (client-side only); N/A.
- Generic dotted collection names (`a.b`) — core `quote_table_name`
  rejects dots; only GridFS `fs.files`/`fs.chunks` are mapped.
- Cross-database transactions — explicit `ok:0` error (see §1).
- Auth/RBAC, sharding, replica-set elections — non-goals
  (`IMPROVEMENT_PLAN.md` §4).
- Legacy `neosqlite.aio` proxy — superseded by NX (non-goal).
