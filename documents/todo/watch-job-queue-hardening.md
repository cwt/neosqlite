---
type: roadmap
title: "Change-Stream Hardening for SQLite-Backed Job Queues"
description: "Resume tokens, server-side pipeline filtering, and a documented watch()-based job-queue recipe so micronote.pub can replace Celery."
tags:
  - change-streams
  - watch
  - task-queue
  - roadmap
  - micronote
timestamp: 2026-09-13T00:00:00Z
version: "1.16.0"
lifecycle: proposed
status: implemented
sources:
  - neosqlite/changestream.py
  - neosqlite/collection/__init__.py
verified: machine-confirmed
stale_after: 2027-03-13T00:00:00Z
---

# Change-Stream Hardening for SQLite-Backed Job Queues

## Overview

`micronote.pub` wants to drop Celery/RabbitMQ (9 tasks with `max_retries=9` exponential backoff in `tasks.py`) in favor of a `watch()`-driven `jobs` collection (`{type, iri/payload/to, attempts, next_run, status}`). Current `watch()` semantics block a correct worker: streams start from `now` (pre-open jobs missed), no resume tokens, no server-side pipeline filtering.

## Gaps

| Need | Current state (`watch.md`) |
| :--- | :--- |
| Resume after worker restart (`resume_after` / `start_after`) | Accepted for API compat, ignored |
| Server-side `pipeline` filtering (e.g. `status=pending`, `next_run<=now`) | Accepted, not implemented — worker must fetch-and-filter client-side |
| Pre-open events | New stream sees nothing before open; worker needs manual startup drain |
| `max_await_time_ms` / batching | Implemented — keep |
| Backoff/retry bookkeeping | App-side (`update_one` `$inc` attempts + `next_run`) — needs a recipe, not engine work |

## Proposed Design

- **Resume tokens (minimal):**
  - Token = `{_id of last changelog row}`; `watch(resume_after=token)` / `start_after` replays rows after the token instead of starting from `now`.
  - Keep per-stream watermarks; purge rule unchanged (purge when last stream closes, but never before the oldest outstanding resume token — bound table growth).
- **Pipeline filtering (minimal useful subset):**
  - Support `$match` on top-level change fields (`operationType`, `ns.coll`, `fullDocument.status`, `fullDocument.next_run`) in Python post-filter at minimum; SQL push-down only if cheap.
- **Documented job-queue recipe (no engine change):**
  - `jobs.insert_one({type, iri, attempts: 0, next_run: now, status: "pending"})`.
  - Worker boot: `find({status: "pending", next_run: {"$lte": now}})` drain, then `watch()` tail with `max_await_time_ms`.
  - Single worker process (not per-gunicorn-worker) to avoid double delivery; `find_one_and_update` claim pattern for future multi-worker.

## Acceptance Criteria

- Worker restart with a resume token processes zero jobs twice and misses zero committed jobs (crash test).
- `watch(pipeline=[{"$match": {"fullDocument.status": "pending"}}])` filters server- or library-side (test asserts no non-matching events yielded).
- Recipe example in `watch.md` or `examples/` runs against `micronote.pub` `jobs` schema with backoff.

## References

- [Change Streams with watch()](../watch.md)
- [PyMongo Modern API Parity](./pymongo-modern-parity.md)
- [TTL Index Support](./ttl-index-support.md)
