---
type: log
title: "NeoSQLite Knowledge Base Bundle Log"
description: "Chronological update log tracking structural modifications, additions, and refactoring within the NeoSQLite OKF knowledge bundle."
tags:
  - log
  - bundle
  - changelog
timestamp: 2026-11-14T10:48:46Z
version: "1.18.0"
lifecycle: active
---

# NeoSQLite Knowledge Base Bundle Log

This document records the chronological history of structural changes, additions, deprecations, and refactoring operations performed on the NeoSQLite Open Knowledge Format (OKF) bundle.

---

## 2026-11-15 — SQLite 3.26 / AlmaLinux 8 Compatibility

### Summary
Fixed the failures seen when running the suite on AlmaLinux 8 with Python 3.12 and a source-built `pysqlite3` linked against SQLite 3.26.0. The root causes were the test harness connecting through the wrong DB-API module, and SQL features newer than SQLite 3.26 being emitted without feature-gated fallbacks.

### Key Modifications
1. **Test harness**: Track connections per DB-API module (`stdlib sqlite3` vs `pysqlite3`) so neosqlite uses the same module and exception classes as the test environment. Updated tests that hard-coded stdlib `sqlite3` exceptions/connection types.
2. **SQLite feature fallbacks**:
   - `VACUUM INTO` → `VACUUM` + file copy on SQLite < 3.27.
   - Aggregate `FILTER` → correlated subquery on SQLite < 3.30.
   - `[#]` JSON append and aggregate `ORDER BY` paths → Python Tier 3 on older SQLite.
   - Registered Python fallbacks for the optional SQLite 3.35+ math functions.
3. **Data-shape fixes**:
   - Preserve scalar `_id` types on non-JSONB schemas by using `BLOB` affinity and binding values without stringifying them.
   - Include foreign `_id` values in Python `$lookup` results.
   - Wrap JSON array aggregates before embedding them in `$group` output and preserve array/object group keys.
   - Fall back from `INDEXED BY` / `EXPLAIN QUERY PLAN` when SQLite reports `no query solution`.
4. **Test runner**: `scripts/runtest.sh` now installs the local NeoSQLite tree plus `pytest`, `pytest-asyncio`, `pytest-cov`, and `pytest-xdist` in the nx-27017 Poetry environment.
5. **Migration CLI**: Added the `neosqlite-migration` console script to copy a database to a new file and rebuild legacy `_id TEXT` collection tables with `_id BLOB`, recovering integer/string/float/ObjectId values from the document JSON while preserving indexes and triggers.

### Verification
- AlmaLinux 8 (SQLite 3.26.0, Python 3.12.15, source-built pysqlite3): **3,068 passed, 32 skipped** with 80.70% coverage; nx-27017 **180 passed, 1 skipped**.
- AlmaLinux 8 stdlib SQLite 3.26.0 via `runtest.sh`: **3,078 passed, 22 skipped**, 80.68% coverage.
- macOS Python 3.14.10 / SQLite 3.54.1: **3,100 passed**; Python 3.12.14 and 3.15.2: **3,090 passed, 10 skipped**.
- Ruff, mypy, and Black clean.

## 2026-11-14 — Release v1.18.0 / nx-27017 v0.9.0

### Summary
Released NeoSQLite v1.18.0 and NX-27017 v0.9.0 as a cumulative correctness, SQL-tier parity, and Python 3.12 modernization release. It rolls up every change since v1.17.4, including kill-switch completeness, Tier-1/Tier-2 parity hardening, expanded Tier-3 promotion, write parity, and coverage-driven bug fixes. This is a breaking runtime change: Python 3.10 and 3.11 are no longer supported.

### Key Modifications
1. **Version and dependency alignment**:
   - Bumped `neosqlite` to `1.18.0` and `nx-27017` to `0.9.0`.
   - Widened the root `nx-27017` extra range to `>=0.8.2,<0.10.0` so it accepts v0.9.0; nx remains compatible with `neosqlite ^1.17.4` and is tested with v1.18.0.
2. **Release notes and README**:
   - Added [releases/v1.18.0.md](releases/v1.18.0.md) and nx-27017 `documents/releases/v0.9.0.md`.
   - Added the v1.18.0 release index entry, bumped OKF frontmatter versions, and replaced the README latest-release section with the cumulative-fixes and Python 3.12 breaking-change summary.
   - Documented the cumulative v1.17.4 → v1.18.0 changes: kill-switch completeness, Section 1/2 SQL tier parity and promotion, coverage-driven fixes, dependency refreshes, and helper-script updates.
3. **Documentation of compatibility changes**:
   - Documented the Python 3.12 minimum, the removal of `serverStatus.python_sqlite_version`, the corrected extras, and the Python 3.15 `jsonb` behavior.
4. **Verification**:
   - `neosqlite`: 3,100 passed on Python 3.14.10; 3,090 passed with 10 optional-Quez skips on Python 3.12.14 and 3.15.2.
   - `nx-27017`: 180 passed, 1 skipped.
   - Ruff, mypy, Black, `poetry check --lock`, and wheel metadata checks are clean.

## 2026-11-14 — Hardened Python 3.12 Migration and Repository-Wide Cleanup

### Summary
Closed the gaps left by the three modernization phases before the next release: enforced Python 3.12 lint rules, removed a deprecation that is gone in Python 3.14/3.15, finished typing/import cleanup, and migrated tests, examples, and helper scripts (`3100 passed` in `neosqlite`, `180 passed, 1 skipped` in `nx_27017`; `ruff`, `mypy`, and `black` clean for the shipped code).

### Key Modifications
1. **Tooling and correctness**:
   - Added `[tool.ruff] target-version = "py312"`, `line-length = 80`, and `pythonVersion = "3.12"` for Pyright.
   - Expanded `scripts/lint-check-and-fix.sh` to enforce `UP`/`FURB` repository-wide and `PERF` on `neosqlite` and `packages/nx_27017/src`.
   - Fixed the remaining `PERF403`/`FURB122` findings and removed the deprecated `sqlite3.version` field from `serverStatus`.
   - Replaced the blanket project `DeprecationWarning` suppression with `error` filters for `neosqlite` and `sqlite3` so future removals surface immediately.
2. **Typing and imports**:
   - Completed `typing.Self` adoption for `Binary`, `ObjectId`, `Connection`, `BulkOperationExecutor`, and `AggregationCursor`.
   - Hoisted the remaining hot-path stdlib imports while keeping intentional local imports for monkeypatching, optional platform modules, and the stdlib-vs-pysqlite3 version report.
3. **Repository-wide cleanup**:
   - Migrated tests, examples, and helper scripts to Python 3.12 idioms (`UP006`, `UP017`, `UP035`, `UP009`, `FURB110`) and removed the remaining UTF-8 coding headers.
   - Kept `from __future__ import annotations` where it still protects `TYPE_CHECKING`-only imports.
4. **Documentation**:
   - Corrected the lowercase `z` claim and stale paths in [./python-312-modernization-plan.md](./python-312-modernization-plan.md), and recorded the post-completion hardening pass.

## 2026-11-14 — Completed Phase 3 & Archived Python 3.11 / 3.12 Modernization Plan

### Summary
Completed Phase 3 (`itertools.pairwise`, PEP 701 quote reuse, `FURB188`/`FURB110`/`PERF102`/`UP009`/`UP034`, audited `UP028` `yield from`) and promoted the finished roadmap out of `documents/todo/` to `documents/python-312-modernization-plan.md` (`3100 passed` in `neosqlite`, `180 passed, 1 skipped` in `nx_27017`, `mypy` & `black` clean).

### Key Modifications
1. **Promoted `documents/python-312-modernization-plan.md`** (moved from `documents/todo/python-312-modernization-plan.md`):
   - Marked Phase 3 as `(Completed)` and transitioned frontmatter from `type: roadmap` / `status: proposed` to `type: design_doc`, `lifecycle: completed`, `status: stable`, updating the log link to `[./log.md](./log.md)`.
2. **Updated Bundle Indexes (`documents/index.md`, `documents/todo/index.md`)**:
   - Moved the plan entry from `documents/todo/index.md` into `documents/index.md` under Internal Implementation & Subsystems.

## 2026-11-14 — Completed Phase 2 of Python 3.11 / 3.12 Modernization Plan

### Summary
Implemented Phase 2 (Typing Modernization: `Self`, `@override`, PEP 695 `type`, `UP035`/`UP037`) of `documents/todo/python-312-modernization-plan.md` (`3100 passed` in `neosqlite`, `180 passed, 1 skipped` in `nx_27017`, `mypy` clean).

### Key Modifications
1. **Updated `documents/todo/python-312-modernization-plan.md`**:
   - Marked Phase 2 as `(Completed)` and synced the fluent-method inventory across `Cursor`, `AggregationCursor`, and `BulkOperationContext`.

---

## 2026-11-14 — Completed Phase 1 of Python 3.11 / 3.12 Modernization Plan

### Summary
Implemented Phase 1 (Runtime Performance & Python 3.11/3.12 Stdlib Upgrades) of `documents/todo/python-312-modernization-plan.md` across `neosqlite/` and `packages/nx_27017/` (`3100 passed` in `neosqlite`, `180 passed, 1 skipped` in `nx_27017`).

### Key Modifications
1. **Updated `documents/todo/python-312-modernization-plan.md`**:
   - Marked Phase 1 as `(Completed)` and aligned the `PERF401`/`PERF403` target list with `query_methods.py`, `grid_file.py`, `gridfs_legacy.py`, and `index_model.py`.

---

## 2026-11-14 — Added Python 3.11 / 3.12 Modernization Plan

### Summary
Added a phased Python 3.11 / 3.12 modernization roadmap after raising the minimum supported Python version to `^3.12, <3.16`.

### Key Modifications
1. **Added `documents/todo/python-312-modernization-plan.md`**:
   - Phase 1: Runtime & stdlib optimizations (`datetime.fromisoformat` native `Z`, `datetime.UTC` + hoisting local imports, `itertools.batched`, `math.sumprod`, `asyncio.run(..., loop_factory=...)`, PEP 709 comprehensions, `slots=True`).
   - Phase 2: Typing modernization (`typing.Self`, `@typing.override`, PEP 695 `type` aliases, `UP035`/`UP037`).
   - Phase 3: Readability idioms (`itertools.pairwise`, PEP 701 quote nesting, `FURB`/`UP` cleanups, plus `UP028` verification gate).
2. **Updated Roadmap Index (`documents/todo/index.md`)**:
   - Registered `python-312-modernization-plan.md` with status `Proposed`.

---

## 2026-11-12 — Release v1.17.4

### Summary
Critical correctness and SQLite engine compatibility release: resolved scalar subquery JSON subtype loss in SQLite ≥ 3.54 (where subqueries dropped the internal `'J'` subtype and caused `json_set`, `jsonb_set`, and `json_object` to serialize arrays/objects as string literals) by wrapping scalar subqueries in `json((SELECT ...))`, eliminated connection `ResourceWarning` leaks, and updated `nx-27017` dependencies.

### Key Modifications
1. **Added `releases/v1.17.4.md` Release Notes**:
   - Detailed documentation on SQLite ≥ 3.54 subquery subtype propagation, array updates ($push, $pull, $pullAll), aggregation SQL converters, resource management, and multi-engine test verification.
2. **Updated Release Notes Index (`documents/releases/index.md`)**:
   - Added `v1.17.4` entry and bumped frontmatter version to `1.17.4`.
3. **Updated Root Knowledge Base Index (`documents/index.md`)**:
   - Bumped frontmatter version to `1.17.4` and extended release notes range to `(v1.0.0 through v1.17.4)`.
4. **Updated `README.md`**:
   - Updated Latest Release section to v1.17.4 detailing SQLite ≥ 3.54 compatibility, array update hardening, and clean resource management.
5. **Bumped version to v1.17.4**:
   - `pyproject.toml` bumped to `1.17.4`, `packages/nx_27017/pyproject.toml` updated to `neosqlite = "^1.17.4"`.

---

## 2026-11-11 — Release v1.17.3

### Summary
Major correctness, MongoDB API parity, and runtime compatibility release: complete MongoDB semantics for `{"$ne": null}` (excluding missing and null fields), `{"$exists": true}` (matching present-but-null fields), existential array comparison semantics (`$gt`, `$gte`, `$lt`, `$lte`), cross-type query operators (`$ne`, `$in`, `$nin`), strict `$size` validation, and canonical BSON type ranking in `$expr` evaluations (`missing < null < numbers < strings < objects < arrays < bools`), alongside NX-27017 GridFS aggregation support (for PyMongo `count_documents`) and official Python 3.15 support (`^3.10, <3.16`).

### Key Modifications
1. **Added `releases/v1.17.3.md` Release Notes**:
   - Comprehensive documentation covering MongoDB operator semantic parity, existential array comparisons, `$expr` BSON ordering, NX-27017 GridFS aggregation, and Python 3.15 compatibility.
2. **Updated Release Notes Index (`documents/releases/index.md`)**:
   - Added `v1.17.3` entry and bumped frontmatter version to `1.17.3`.
3. **Updated Root Knowledge Base Index (`documents/index.md`)**:
   - Bumped frontmatter version to `1.17.3` and extended release notes subdirectories range to `(v1.0.0 through v1.17.3)`.
4. **Updated `README.md`**:
   - Updated Latest Release section to v1.17.3 detailing MongoDB parity enhancements, existential array comparisons, NX-27017 wire protocol updates, and Python 3.15 compatibility.
5. **Bumped version to v1.17.3**:
   - `pyproject.toml`, `packages/nx_27017/pyproject.toml`, and lockfile refreshed for Python 3.15.

---

## 2026-10-14 — Release v1.17.2

### Summary
Correctness and PyMongo API parity release: native FTS5 BM25 text relevance scoring in `find()`, multi-field text index creation syntax, fluent GridFS cursor pagination and sorting, direct `Connection` support in `GridFSBucket`, safe collection counting on GridFS system tables, and expanded unit test coverage reaching 81.18%.

### Key Modifications
1. **Added `releases/v1.17.2.md` Release Notes**:
   - Comprehensive documentation of FTS5 BM25 text scoring (`find()` projection via `{"$meta": "textScore"}` and sorting), multi-field text index creation (`create_index([("title", "text"), ("body", "text")])`), fluent `GridOutCursor` chaining (`sort`, `skip`, `limit`, `batch_size`, `to_list`, `clone`, `rewind`), direct `Connection` binding for `GridFSBucket`, and relational counting on `fs.files`.
2. **Updated Release Notes Index (`documents/releases/index.md`)**:
   - Added `v1.17.2` entry and bumped frontmatter version to `1.17.2`.
3. **Updated Root Knowledge Base Index (`documents/index.md`)**:
   - Bumped frontmatter version to `1.17.2` and extended release notes subdirectories range to `(v1.0.0 through v1.17.2)`.
4. **Updated PyMongo API Comparison (`documents/pymongo-api-comparison.md`)**:
   - Bumped frontmatter version and header version to `v1.17.2` (100.0% compatibility, 404 tests: 384 passed, 20 skipped by design, 0 failed).
5. **Updated GridFS Documentation (`documents/gridfs.md`)**:
   - Documented direct `Connection` instantiation for `GridFSBucket`, fluent `GridOutCursor` methods, and `count_documents` handling on `fs.files`.
6. **Updated Text Search Documentation (`documents/text-search.md`)**:
   - Documented native FTS5 BM25 text relevance scoring, score projection via `{"score": {"$meta": "textScore"}}`, sorting by `textScore`, and multi-field text index definitions.
7. **Bumped version to v1.17.2**:
   - `pyproject.toml` and `README.md` (Latest Release).

---

## 2026-10-01 — Release v1.17.1

### Summary
Correctness and API parity release: Tier-2 aggregation serialization discrepancies resolved, 100% NX-27017 wire protocol compatibility, and test ResourceWarnings eliminated.

### Key Modifications
1. **Added `releases/v1.17.1.md` Release Notes**:
   - Tier-2 aggregation serialization parity (`$sortArray` dictionary retention, `$unwind` scalar preservation, `$project` missing-field omission), 100% NX-27017 wire protocol compatibility, and clean test suite resources.
2. **Updated Release Notes Index (`documents/releases/index.md`)**:
   - Added `v1.17.1` entry.

---

## 2026-09-30 — Release v1.17.0

### Summary
Feature and parity release: transaction context managers, full bulk write semantics, aggregation system variables, data integrity hardening, and 100% differential API compatibility across 42 resolved findings.

### Key Modifications
1. **Added `releases/v1.17.0.md` Release Notes**:
   - Comprehensive documentation of transaction context managers, `BulkWriteError` and unordered execution, aggregation system variables (`$$ROOT`/`$$CURRENT`), composable `$dateDiff`, 24-character string `_id` preservation, timezone-aware `ObjectId.generation_time`, `$currentDate` datetime parity, storage integrity (WAL checkpointing during auto-vacuum migration), and measured test metrics (3,009 passed, 100% differential parity).
2. **Updated Release Notes Index (`documents/releases/index.md`)**:
   - Added `v1.17.0` entry to the chronological table and bumped frontmatter version to `1.17.0`.
3. **Updated Root Knowledge Base Index (`documents/index.md`)**:
   - Bumped frontmatter version to `1.17.0` and extended release notes subdirectories range to `(v1.0.0 through v1.17.0)`.
4. **Updated PyMongo API Comparison (`documents/pymongo-api-comparison.md`)**:
   - Bumped frontmatter version and header version to `v1.17.0` (100% compatibility, 375 passed, 20 skipped by design, 0 failed).
5. **Updated ObjectId Specification (`documents/objectid-implementation.md`)**:
   - Bumped frontmatter version to `1.17.0` and documented `generation_time` property returning a timezone-aware UTC datetime.
6. **Bumped version to v1.17.0**:
   - `pyproject.toml` and `README.md` (Latest Release).

---

## 2026-09-15 — Release v1.16.3

### Summary
Correctness patch: datetime queries returned field-less documents for rows stored as JSONB.

### Key Modifications
1. **Added `releases/v1.16.3.md` Release Notes**:
   - Documents the raw `data` SELECT in `DateTimeQueryProcessor` (SQL tier and temporary-table tier), the resulting `Skipping corrupted document` warnings and `{_id, __neosqlite_corrupted__}` stubs, and the `json_data_column()` fix with migration guidance.
2. **Bumped version to v1.16.3**:
   - `pyproject.toml`, `README.md` (Latest Release), `documents/index.md`, `documents/releases/index.md`, and `documents/pymongo-api-comparison.md`.
3. **No spec document changes required**:
   - The fix is read-path-only and reuses the `json_data_column` conversion contract already described in `documents/temp-table-breakdown.md`.

---

## 2026-09-13 — Release v1.16.2

### Summary
Correctness patch: dotted GridFS metadata filters plus `fs.chunks` delegation.

### Key Modifications
1. **Added `releases/v1.16.2.md` Release Notes**:
   - Documents the `GridOutCursor` dotted `metadata.*`/`aliases.*` fix, the unknown-filter `1=0` guard, and the `fs.chunks.find()` direct-query fix with migration guidance.
2. **Bumped version to v1.16.2**:
   - `pyproject.toml`, `README.md` (Latest Release), `documents/index.md`, `documents/releases/index.md`, `documents/gridfs.md`, and `documents/pymongo-api-comparison.md`.
3. **Updated GridFS spec**:
   - Frontmatter version bump; dotted `metadata.*` find patterns now actually filter via `json_extract` instead of returning the whole bucket.

---

## 2026-09-13 — Release v1.16.1

### Summary
Correctness patch: Python-level ObjectId ordering plus `_id` range fallback.

### Key Modifications
1. **Added `releases/v1.16.1.md` Release Notes**:
   - Documents ObjectId ordering dunders, the `_id` range find/count fixes, and cursor-pagination notes with migration guidance.
2. **Bumped version to v1.16.1**:
   - `pyproject.toml`, `README.md` (Latest Release), `documents/index.md`, `documents/releases/index.md`, `documents/objectid-implementation.md`, and `documents/pymongo-api-comparison.md`.
3. **Updated ObjectId spec**:
   - Operators bullet now lists ordering dunders alongside equality/hashing.

---

## 2026-09-13 — Release v1.16.0

### Summary
Minor feature release: PyMongo modern parity, TTL index expiry, and change-stream hardening for job queues.

### Key Modifications
1. **Added `releases/v1.16.0.md` Release Notes**:
   - Documents `find()` option kwargs, TTL declaration/expiry, and `watch()` resume/filtering with migration notes.
2. **Bumped version to v1.16.0**:
   - `pyproject.toml`, `README.md` (Latest Release + TTL feature bullet), `documents/index.md`, `documents/releases/index.md`, `documents/pymongo-api-comparison.md`, and the feature docs shipping in this release (`watch.md`, `ttl-indexes.md`, `todo/` proposals).
3. **Updated comparison matrix**:
   - `find()` kwargs, TTL declaration/`purge_expired()`/`get_ttl_specs()`, `get_database()`/`drop_database()`, and `watch()` resume/`$match` rows.

---

## 2026-09-13 — Implement micronote.pub Migration TODOs

### Summary
Implemented all three `micronote.pub` migration proposals with per-feature unit tests; full suite (2917 passed), mypy, ruff, and black clean.

### Key Modifications
1. **PyMongo modern parity** (`tests/test_pymongo_modern_parity.py`):
   - `find()` honors `limit`/`skip`/`sort` kwargs; `create_index()` accepts `expireAfterSeconds`/`name`/extra options with TTL metadata in `_neosqlite_ttl_indexes`; `Connection.get_database()`/`drop_database()` compat helpers.
2. **TTL index support** (`tests/test_ttl_index_support.py`, `documents/ttl-indexes.md`):
   - `purge_expired()` plus auto-purge on `find`/`find_one`; opt-in `Connection(ttl_sweep_interval_s=...)` background sweeper and `sweep_ttl_once()`.
3. **Watch job-queue hardening** (`tests/test_watch_job_queue.py`, `examples/watch_job_queue.py`):
   - `resume_after`/`start_after` replay with `resume_token` property and bounded retention; library-side `$match` pipeline filtering; documented drain-then-tail recipe in `documents/watch.md`.
4. **Marked proposals implemented** in `documents/todo/` and refreshed `documents/pymongo-api-comparison.md`.

---

## 2026-09-13 — Trim TODOs After micronote.pub Modernization Decision

### Summary
`micronote.pub` will modernize to modern PyMongo/`GridFSBucket` usage, so deprecated-API shims are dropped from the NeoSQLite roadmap. Removed two proposals, added one trimmed modern-parity proposal.

### Key Modifications
1. **Removed `pymongo-legacy-compat.md`** (superseded):
   - Deprecated shims (`count`, `remove`, `update`, legacy `GridFS` layout) move to the app.
2. **Removed `gridfs-legacy-query-compat.md`** (superseded):
   - App adopts `GridFSBucket` with `metadata={url,kind,size}` natively.
3. **Added `pymongo-modern-parity.md`**:
   - Tracks only modern gaps: `find(limit/skip/sort)` kwargs, index options, `drop_database`/`get_database`.
4. **Updated `ttl-index-support.md`, `todo/index.md`**.

---

## 2026-09-13 — micronote.pub Migration TODOs

### Summary
Added four OKF `roadmap` proposals under `documents/todo/` covering the missing pieces required to migrate `micronote.pub` from MongoDB/Celery/GridFS to NeoSQLite, and updated the todo index.

### Key Modifications
1. **Added `pymongo-legacy-compat.md`**:
   - Shims for `count`, `Cursor.count`, `remove`, `update`, `find(limit/skip/sort)` kwargs, `create_index(expireAfterSeconds)`, `drop_database`/`get_database`.
2. **Added `ttl-index-support.md`**:
   - `expireAfterSeconds` declaration plus lazy/background expiry for `cache2`-style caches.
3. **Added `gridfs-legacy-query-compat.md`**:
   - Top-level `url/size/kind` → `metadata.*` query translation and `uploadDate` alias for `MediaCache`.
4. **Added `watch-job-queue-hardening.md`**:
   - Resume tokens, pipeline filtering, and a documented `watch()` job-queue recipe to replace Celery.
5. **Updated `todo/index.md`**:
   - Cataloged the four new proposals, bumped bundle metadata to v1.15.2.

---

## 2026-09-08 — Release v1.15.2

### Summary
Published release notes for v1.15.2 (`documents/releases/v1.15.2.md`), updated release history index, and documented MongoDB container image pinning to `mongo:8.2.12` for Linux kernel $\ge$ 6.19 compatibility across testing guides.

### Key Modifications
1. **Added `v1.15.2.md` Release Notes**:
   - Comprehensive documentation of SQL-tier fixes (`$lookup` string foreign field extraction, negative `$slice` array update ordering).
   - Documented NX-27017 v0.6.3 server hardening fixes.
   - Added detailed Correctness Transparency Note with verification scripts and honest remediation instructions.
2. **Updated Documentation Metadata**:
   - Bumped OKF bundle metadata to v1.15.2 in `documents/index.md`, `documents/releases/index.md`, and `documents/pymongo-api-comparison.md`.
   - Updated root `README.md` Latest Release section.

---

## 2026-09-07 — Migration to Google Open Knowledge Format (OKF) v0.2

### Summary
Migrated the entire `documents/` tree into a formal **Google Open Knowledge Format (OKF) v0.2** bundle, retired the legacy monolithic `CHANGELOG.md`, modernized all filenames to lowercase kebab-case, and established directory indexing with GitHub symlinks.

### Key Modifications
1. **Removed `CHANGELOG.md`**:
   - Retired the 3,086-line redundant root `CHANGELOG.md` file.
   - Preserved full historical release details in version-specific release notes under `documents/releases/` (v1.0.0 through v1.15.1).
   - Created `documents/releases/index.md` listing all 52 releases in reverse chronological order with direct links, dates, and summaries.

2. **Renamed ALL-CAPS Files to Lowercase Kebab-Case**:
   - Replaced all uppercase and underscore-delimited filenames across `documents/` with modern lowercase kebab-case naming.
   - Renamed `documents/TODO/` directory to `documents/todo/`.
   - Recorded all renames via Mercurial (`hg rename`) to preserve full file commit history.

3. **Injected OKF v0.2 YAML Frontmatter**:
   - Added metadata blocks (`type`, `title`, `description`, `tags`, `timestamp`, `version`, `lifecycle`) to all 24 concept and roadmap documents.
   - Added metadata blocks (`type: release_notes`) with extracted descriptions and commit timestamps to all 52 release notes.

4. **Created Directory Indexes & Progressive Disclosure Maps**:
   - `documents/index.md`: Root bundle catalog organizing documents into Architecture, Core Features/APIs, Internal Subsystems, Operations/Testing, and Subdirectories.
   - `documents/releases/index.md`: Chronological table of all release notes.
   - `documents/todo/index.md`: Catalog of future roadmap proposals (geospatial, vector search).

5. **Configured GitHub Rendering Symlinks**:
   - Added relative symbolic links `README.md -> index.md` in `documents/`, `documents/releases/`, and `documents/todo/` for automatic directory rendering on GitHub.

6. **Updated Cross-References**:
   - Updated root `README.md` documentation table and changelog link.
   - Updated internal relative links across all concept documents to point to the new lowercase kebab-case paths.
