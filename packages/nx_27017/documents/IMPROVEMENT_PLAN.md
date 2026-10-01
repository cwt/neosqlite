# NX-27017 Improvement Plan

First document for `packages/nx_27017`. Goal: make NX the single async story for NeoSQLite, no separate `neosqlite.aio` wrapper.

## 0. Decisions (locked)

1. **No `neosqlite.aio` / `neosqlite.asyncio` wrapper.** Rejected after review of real PyMongo.
   - Motor (old, deprecated 2026-05-14) = thread-pool wrapper. PyMongo Async (4.18, current) = native `asyncio` (`loop.sock_sendall/sock_recv`, `pymongo/network_layer.py`), `pymongo/asynchronous/*` is source of truth, `pymongo/synchronous/*` generated. Only `run_in_executor` left is SSL handshake + OIDC.
   - SQLite has no non-blocking socket API. `aiosqlite/core.py` is one `Thread` + `SimpleQueue` per connection — same "fake" as `asyncio.to_thread`. Native rewrite buys nothing for embedded SQLite.
   - Consequence: async comes free via `AsyncMongoClient -> NX-27017 -> NeoSQLite`. We maintain one translation surface instead of two APIs.
2. **NX multi-DB = one SQLite file per database**, opened on demand, switched by dict lookup. Fixes `db1.coll == db2.coll` collision (`handler.py:get_database` currently aliases all names to one `Connection`).
3. **NeoSQLite is truth.** NX tracks it. New NeoSQLite API without NX route + test = incomplete feature.

## 1. Gap summary (handler.py:349-1697 vs neosqlite/)

Covered: `insert/find/update/delete/count/distinct/aggregate/findAndModify/create/drop/renameCollection/createIndexes/dropIndexes/listCollections/listDatabases/listIndexes/explain(find-only)/startSession/endSessions/commit/abortTransaction`, GridFS basics, changestream via in-memory `ChangeStreamManager`.

Missed / broken (priority order):

- P0 correctness: multi-DB collision, `dropDatabase` only pops dict (`handler.py:470`, never drops tables), no real cursors (`id:0 + firstBatch:all`, `getMore` changestream-only), changestream divergence (custom manager vs `Collection.watch()` triggers in `neosqlite/collection/__init__.py:1606`).
- P1 parity: `bulkWrite` semantics, `estimated_document_count`, `find_raw_batches/aggregate_raw_batches`, `create_search_indexes(pl)/update_search_index/drop_search_index/reindex`, TTL (`expireAfterSeconds` accepted on create only; `get_ttl_specs/purge_expired/sweep_ttl_once` have no wire op), cursor opts dropped (`collation/comment/maxTimeMS/allowDiskUse/batchSize`), `update` drops `hint/collation/let`, GridFS `rename/delete_by_name/drop/list/download_by_name/upload_with_id` + legacy `GridFS.put/get_version/exists/new_file` (`neosqlite/gridfs/gridfs_bucket.py:634-936`, `gridfs_legacy.py:39-202`).
- P2 compat: `OP_QUERY handle_query:1632` find-only, `writeConcern/readConcern` PRAGMA mapping, auth (out of scope), `vacuum/compact/validate` only via `db.command` fallback (`handler.py:1127` -> `connection.py:1603` dispatch).

## 2. Multi-DB design (P0)

- CLI: `--data-dir /data` replaces single `--db nx-27017.db` (`nx_27017.py:70`, `daemon.py:229`). Compat: `--single-db-compat` keeps old single-file behavior for existing deployments.
- Mapping: `client.<db>.<coll>` <=> `<data-dir>/<safe_db>.db` table `<coll>`. Direct user parity: `Connection("/data/test.db")["coll"]` sees same data as `client.test.coll`.
- `get_database(db_name)`: sanitize `^[A-Za-z0-9_-]+`, reject `../,/,NUL`, lazy `Connection(path, check_same_thread=False, journal_mode=..., tokenizers=...)`, cache in `self._conns`, per-DB `RLock` (replace global `_serialize` in `handler.py:37`). `:memory:`: one isolated `Connection(":memory:")` per db, not shared cache.
- `dropDatabase`: close + unlink file (close first for Windows). `listDatabases`: `*.db` scan + `stat size`, real `empty` flag. Cap open DBs (~100, LRU-close). No cross-DB transactions (return error, document it).
- Effort: ~150 lines + tests. Risks: fd exhaustion, per-DB TTL threads, backup consistency across files.

## 3. Phased plan

### Phase 1 — Correctness (P0)

1. Multi-DB files as above + `dropDatabase` fix + `listDatabases` real sizes.
2. Real cursor pagination: non-zero `cursor id`, `getMore` for `find/aggregate` (spill to temp table or re-run with `skip/limit`, enforce 16MB `firstBatch` split). `killCursors` for all cursors, not just streams.
3. Changestream unification: back `aggregate + $changeStream` by `Collection.watch()` (triggers + resume tokens + `fullDocument:updateLookup`) instead of parallel in-memory manager, or delete one path. Cover `resume_after/start_after`, `getMore/nextBatch/postBatchResumeToken`.
4. Conformance harness: run `tests/test_*.py` twice (direct vs `AsyncMongoClient->localhost`), gate CI on parity. Extend `tests/test_handler_commands.py` beyond current ~30 cases.
5. Wire-protocol differential check against real MongoDB `8.2.12` (no strict kernel check): same `pymongo`/`AsyncMongoClient` script runs against `NX localhost:27017` and `real 8.2.12 localhost:27018` (docker `mongo:8.2.12`), compares logical command/response docs, not raw bytes. Normalize away volatile/kernel fields before diff: `host/process/pid/localTime/uptime*/mem/connections/asserts/globalLock/storageEngine/wiredTiger/version/gitVersion/modules/setName/topologyVersion/connectionId/logicalSessionTimeoutMinutes/maxBsonObjectSize` and any `opTime/clusterTime/electionId`. Assert on functional shape only: `ok/n/nModified/insertedIds/values/cursor.firstBatch docs (after ObjectId/date normalization)/writeErrors/errmsg codes`. CI job `nx-compat-mongo-8.2.12` is opt-in (skips without docker); kernel/version mismatch never fails the build.

### Phase 2 — Parity (P1)

5. `bulkWrite` ordered/unordered + `writeErrors` mapping, `estimated_document_count`, raw-batch cursors (document as unsupported over wire if kept).
6. Search/TTL: `create_search_indexes(pl)/update/drop/reindex`, expose TTL specs/purge (or document sweeper-only behavior).
7. Cursor/update option forwarding: `collation/batchSize/maxTimeMS/comment/allowDiskUse`, `hint/let/arrayFilters/bypassDocumentValidation` — either forward or return explicit error instead of silent drop.
8. GridFS full: `rename/delete_by_name/drop/list` via `fs.files/fs.chunks` ops; decide legacy `GridFS` API support scope.

### Phase 3 — Hardening (P2)

9. `OP_QUERY` parity or deprecation, `writeConcern->PRAGMA synchronous` mapping per DB, `with_transaction` retry docs, single-event-loop caveats in README.
10. Perf pass: per-DB locks allow parallel DBs; keep single-file RLock semantics documented. Benchmark in-process vs TCP+BSON to set expectations.

## 4. Non-goals

- Native `aiosqlite`-style rewrite of NeoSQLite. Wontfix.
- `neosqlite.aio` magic proxy (`__new__/metaclass __call__` auto-wrap). Superseded by this plan.
- Auth/RBAC, sharding, replica-set elections.

## 5. Acceptance

- `test_handler_commands.py` + new `test_multidb_files.py`, `test_cursor_pagination.py`, `test_changestream_parity.py` green in both modes.
- `test_wire_compat_mongo_8_2_12.py` green against real `8.2.12` under lenient normalization above; any diff is functional (`ok/n/cursor` shape), never kernel/version/host field noise.
- `client.db1.coll` and `client.db2.coll` isolated on disk (`<db>.db` files), switchable back and forth, `dropDatabase` deletes file, direct `Connection(path)` sees identical collections.
- Docs: mapping table + unsupported-ops list in `documents/` (this folder replaces scattered root notes).

## 6. P0 implementation notes (done 2026-10-01)

- Multi-DB: `NeoSQLiteHandler(..., data_dir=..., single_db_compat=...)`;
  directory `--db` or `--data-dir` enables per-file mode, `:memory:` is
  per-db isolated, plain file paths stay legacy-shared (opt-in migration).
  CLI: `--data-dir`, `--single-db-compat` (`nx_27017.py`, `daemon.py`).
  Sessions keyed `(lsid, db)` in isolated modes, shared scope in legacy.
  `_sessions_lock` is now an `RLock` (`_find_session` re-enters it).
- Cursors: `find`/`aggregate` honor `batchSize` / `cursor.batchSize`
  (default 101), remainder stored server-side, `getMore`/`killCursors`
  serve data cursors too; per-connection cleanup on disconnect. 16MB
  first-batch splitting still open.
- Changestreams: pull-based over the shared `_neosqlite_changestream`
  trigger log (persistent `Collection.watch()` per db/coll, no threads).
  Covers bulk/TTL/direct-SQL writes the old push fan-out missed.
  Wire tokens are deterministic `{"_data": "nx:<rowid>"}`; `resumeAfter`
  / `startAfter` replay from the log, `$match` and `fullDocument: off`
  honored, listeners namespaced per db. Known upstream quirks inherited:
  int `_id`s stringify in `documentKey` (native behavior) and JSONB
  update rows need `json()` conversion on read (done here; native
  `ChangeStream.__next__` skips such rows instead).
- Harness: `scripts/run-api-nx-vs-mongo.sh` +
  `examples/api_nx_vs_mongo_main.py` (async wire-vs-wire) and gated
  `tests/test_wire_compat_mongo.py` (`NX_REAL_MONGO_URI`, skips without).
  Container runtime is podman-first (`CONTAINER_RUNTIME`, `--with-podman`;
  `--with-docker` kept as alias). New suites: `test_handler_multidb.py`,
  `test_handler_cursors.py`, extended `test_change_stream_improved.py`.
- Live differential vs real MongoDB `8.2.12` (podman,
  `docker.io/library/mongo:8.2.12`): **11/11 PASS** (core CRUD +
  admin). Found and fixed on the way: wire `ns` must use the logical db
  name, never `Connection.name` (a file path — broke `getMore`
  round-trips once real cursor ids existed); `commit/abortTransaction`
  resolve across session scopes (drivers commit on `admin.$cmd` while
  the tx lives on the data db); `Collection.drop()` now evicts the
  connection cache like `Connection.drop_collection` (#132 follow-up —
  drop-then-recreate via PyMongo left a table-less object), with
  `tests/test_collection.py::test_drop_then_recreate_collection`.
  Replica-set features (tx/changestreams on the real side) need a
  single-node RS with host-visible hostname: covered by
  `run-api-nx-vs-mongo.sh --with-podman --replset` (host-networked
  podman container, verified electing primary), pending only the
  tx/changestream differential cases themselves.
