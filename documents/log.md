---
type: log
title: "NeoSQLite Knowledge Base Bundle Log"
description: "Chronological update log tracking structural modifications, additions, and refactoring within the NeoSQLite OKF knowledge bundle."
tags:
  - log
  - bundle
  - changelog
timestamp: 2026-09-13T00:00:00Z
version: "1.16.1"
lifecycle: active
---

# NeoSQLite Knowledge Base Bundle Log

This document records the chronological history of structural changes, additions, deprecations, and refactoring operations performed on the NeoSQLite Open Knowledge Format (OKF) bundle.

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
