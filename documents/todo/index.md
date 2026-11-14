---
type: index
title: "NeoSQLite Future Plans & Roadmap Index"
description: "Index of planning documents and roadmaps for future NeoSQLite capabilities."
tags:
  - index
  - roadmap
  - planning
  - todo
timestamp: 2026-11-14T09:23:24Z
version: "1.17.4"
lifecycle: active
---

# NeoSQLite Roadmap & Planning

This directory tracks research, architecture proposals, and implementation roadmaps for future NeoSQLite features.

## Planned Capabilities

| Capability | Document | Status | Summary |
| :--- | :--- | :--- | :--- |
| **Python 3.11 / 3.12 Modernization** | [python-312-modernization-plan.md](./python-312-modernization-plan.md) | Proposed | Three-phase migration plan adopting 3.11/3.12 performance optimizations, `datetime.UTC`, native ISO `Z` parsing, `batched`, `sumprod`, `Self`, `@override`, PEP 695 `type`, and safe Ruff `UP`/`PERF`/`FURB` cleanups. |
| **Geospatial Queries** | [geospatial-implementation-plan.md](./geospatial-implementation-plan.md) | Proposed | Roadmap for 2D and 2Dsphere geospatial query support via SpatiaLite with pure-Python fallbacks. |
| **Vector Search** | [vector-search-plan.md](./vector-search-plan.md) | Proposed | Architectural plan for vector similarity search using SQLite-vec and embedding integration. |
| **PyMongo Modern Parity** | [pymongo-modern-parity.md](./pymongo-modern-parity.md) | Implemented | Modern-API gaps only (`find` limit kwargs, index options, `drop_database`); deprecated shims dropped after micronote.pub modernization. |
| **TTL Index Support** | [ttl-index-support.md](./ttl-index-support.md) | Implemented | `expireAfterSeconds` declaration plus lazy/background expiry for `cache2`-style caches. |
| **Watch Job-Queue Hardening** | [watch-job-queue-hardening.md](./watch-job-queue-hardening.md) | Implemented | Resume tokens, pipeline filtering, and a documented `watch()` job-queue recipe to replace Celery. |
