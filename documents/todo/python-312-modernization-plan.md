---
type: roadmap
title: "Python 3.11 / 3.12 Modernization & Performance Plan"
description: "Phased plan to adopt Python 3.11 and 3.12 runtime optimizations, stdlib APIs, typing features, and readability idioms across neosqlite and packages/nx_27017."
tags:
  - python-3.12
  - modernization
  - performance
  - typing
  - refactoring
generated:
  by: antigravity
  at: 2026-11-14T09:23:24Z
version: "1.17.4"
status: proposed
sources:
  - pyproject.toml
  - mypy.ini
  - packages/nx_27017/pyproject.toml
  - neosqlite/collection/json_helpers.py
  - neosqlite/collection/expr_evaluator/python_evaluators/date_ops.py
  - neosqlite/collection/raw_batch_cursor.py
  - neosqlite/collection/query_helper/aggregation.py
  - neosqlite/collection/query_helper/window_operators.py
  - neosqlite/collection/jsonb_support.py
  - neosqlite/collection/cursor.py
  - neosqlite/connection.py
  - packages/nx_27017/src/nx_27017/server.py
  - packages/nx_27017/src/nx_27017/handler.py
verified: machine-confirmed
stale_after: 2027-05-14T00:00:00Z
---

# Python 3.11 / 3.12 Modernization & Performance Plan

## Overview

With minimum Python bumped to `^3.12, <3.16` in `pyproject.toml`, `mypy.ini` (`python_version = 3.12`), and `packages/nx_27017/pyproject.toml` (`python = "^3.12, <3.16"`), both `neosqlite/` and `packages/nx_27017/` can drop Python 3.10 workarounds and adopt Python 3.11/3.12 runtime and typing improvements.

This plan separates changes into three safe, independently committable phases so each implementation commit can reference a specific phase (for example, `Ref: documents/todo/python-312-modernization-plan.md (Phase 1)`).

---

## Phase 1: Runtime Performance & Python 3.11/3.12 Stdlib Upgrades

Focuses on hot paths and C-implemented stdlib replacements with zero external API breakage.

### 1.1 Native ISO-8601 `Z` Parsing in `datetime.fromisoformat()` (Python 3.11+)

Python 3.11+ handles trailing `"Z"` / `"z"` directly inside C `fromisoformat()`, eliminating intermediate string allocations from `.replace("Z", "+00:00")` and avoiding corruption of malformed inputs that contain `"Z"` mid-string.

| Location | Current Pattern | Target Replacement |
| :--- | :--- | :--- |
| `neosqlite/collection/json_helpers.py#L125-L127` | `datetime.fromisoformat(dct["$date"].replace("Z", "+00:00"))` | `datetime.fromisoformat(dct["$date"])` |
| `neosqlite/collection/expr_evaluator/python_evaluators/date_ops.py#L117-L119` | `datetime.fromisoformat(value.replace("Z", "+00:00"))` | `datetime.fromisoformat(value)` |
| `neosqlite/collection/expr_evaluator/python_evaluators/date_ops.py#L201-L208` | `start.replace("Z", "+00:00")` & `end.replace("Z", "+00:00")` | Pass `start` and `end` directly to `datetime.fromisoformat(...)` |
| `neosqlite/collection/expr_evaluator/python_evaluators/date_ops.py#L291-L292` | `date_string = date_string.replace("Z", "+00:00")` before `fromisoformat` | `datetime.fromisoformat(date_string)` |
| `neosqlite/collection/expr_evaluator/python_evaluators/date_ops.py#L357-L359` | `datetime.fromisoformat(date_val.replace("Z", "+00:00"))` | `datetime.fromisoformat(date_val)` |
| `neosqlite/collection/expr_evaluator/python_evaluators/date_ops.py#L472-L474` | `datetime.fromisoformat(date_val.replace("Z", "+00:00"))` | `datetime.fromisoformat(date_val)` |
| `neosqlite/collection/expr_evaluator/python_evaluators/date_ops.py#L555-L557` | `datetime.fromisoformat(date_val.replace("Z", "+00:00"))` | `datetime.fromisoformat(date_val)` |

### 1.2 `datetime.UTC` & Hoisting Function-Local Imports (Python 3.11+)

Replace `timezone.utc` / `tz.utc` with `datetime.UTC` and move function-local `from datetime import timezone` statements to module scope (notably inside `object_hook` in `neosqlite_json_loads`, which executes per dict).

| Package | Target Locations |
| :--- | :--- |
| `neosqlite/` (local import removal) | `neosqlite/collection/json_helpers.py#L128-L132`, `neosqlite/collection/expr_evaluator/python_evaluators/date_ops.py#L298-L316` |
| `neosqlite/` (module imports) | `neosqlite/collection/__init__.py#L1463-L1465`, `neosqlite/collection/query_helper/_sql_updates.py#L2204`, `neosqlite/collection/query_helper/update_operations.py#L445`, `neosqlite/objectid.py#L300`, `neosqlite/gridfs/grid_file.py#L222`, `neosqlite/gridfs/gridfs_bucket.py#L336`, `neosqlite/gridfs/gridfs_bucket.py#L723` |
| `packages/nx_27017/` | `packages/nx_27017/src/nx_27017/changestream.py#L421`, `packages/nx_27017/src/nx_27017/gridfs_adapter.py#L269-L271`, `packages/nx_27017/src/nx_27017/gridfs_adapter.py#L388-L398`, `packages/nx_27017/src/nx_27017/handler.py#L938`, `packages/nx_27017/src/nx_27017/handler.py#L2732`, `packages/nx_27017/tests/test_wire_compat_mongo.py#L59-L61` |

### 1.3 `itertools.batched()` for Sequence Chunking (Python 3.12+)

Replace `for i in range(0, len(seq), batch_size): batch = seq[i : i + batch_size]` with C-implemented `itertools.batched(seq, batch_size)`.

| Location | Target Change |
| :--- | :--- |
| `neosqlite/collection/raw_batch_cursor.py#L82-L88` | `for batch in batched(results, self._batch_size):` |
| `neosqlite/collection/raw_batch_cursor.py#L212-L218` | `for batch in batched(docs, self._batch_size):` |
| `neosqlite/collection/query_helper/aggregation.py#L471-L481` | `for batch in batched(docs, batch_size):` + list comprehension for `docs_to_insert` |

> **Explicit Exclusion:** Keep slice-based chunking in `neosqlite/gridfs/gridfs_bucket.py#L385-L392` because `batched(bytes, n)` yields `tuple[int, ...]` rather than `bytes`, which would add `bytes(...)` conversion overhead.

### 1.4 `math.sumprod()` for Covariance Accumulation (Python 3.12+)

Replace the generator-in-`sum(zip(...))` inner loop in `$covariancePop` / `$covarianceSamp` with `math.sumprod((v1 - mean1 for v1 in v1_list), (v2 - mean2 for v2 in v2_list))` at `neosqlite/collection/query_helper/window_operators.py#L235-L237`.

### 1.5 Loop-Factory Startup Without Global Policy Mutation (Python 3.12+)

Replace `uvloop.install()` + `asyncio.run(run_server(...))` in `packages/nx_27017/src/nx_27017/server.py#L303-L305` with:

```python
asyncio.run(
    run_server(host, port, handler),
    loop_factory=uvloop.new_event_loop if uvloop is not None else None,
)
```

### 1.6 PEP 709 Inlined Comprehensions & `@dataclass(slots=True)`

Python 3.12 inlines list and dict comprehensions into the enclosing frame, making them faster than repeated `.append(...)` / key-assignment loops. Add `slots=True` on dataclasses missing it.

| Optimization | Target Locations |
| :--- | :--- |
| `@dataclass(frozen=True, slots=True)` | `neosqlite/collection/jsonb_support.py#L19-L20` (`JSONBContext`) |
| Hot-path list comprehensions (`PERF401`) | `neosqlite/connection.py#L200-L208` (`_regexp_find_all`), `neosqlite/collection/expr_evaluator/python_evaluators/date_ops.py#L370-L373`, `neosqlite/collection/expr_evaluator/sql_converters/date.py#L21-L24` |
| `nx_27017` response comprehensions (`PERF401`) | `packages/nx_27017/src/nx_27017/gridfs_adapter.py#L576-L584`, `packages/nx_27017/src/nx_27017/gridfs_adapter.py#L622-L630`, `packages/nx_27017/src/nx_27017/handler.py#L1725-L1731`, `packages/nx_27017/src/nx_27017/handler.py#L2835-L2839`, `packages/nx_27017/src/nx_27017/handler.py#L2855-L2867` |
| Dict comprehensions (`PERF403`) | `neosqlite/gridfs/gridfs_legacy.py#L57-L61`, `neosqlite/index_model.py#L49-L50` |

---

## Phase 2: Typing Modernization (`Self`, `@override`, PEP 695 `type`)

Improves static type safety under `mypy` (`python_version = 3.12`) and eliminates redundant forward-reference strings.

### 2.1 `typing.Self` (PEP 673, Python 3.11+)

Adopt `Self` for fluent builder methods, `clone()`, `@classmethod` constructors returning the class, and `__enter__()` / `__iter__()` returning `self`:

| File | Methods |
| :--- | :--- |
| `neosqlite/collection/cursor.py` | `limit` (`L149`), `skip` (`L162`), `sort` (`L175`), `batch_size` (`L197`), `hint` (`L218`), `max` (`L280-L285`), `collation` (`L287`), `where` (`L321`), `comment` (`L349`), `clone` (`L394`), `__enter__` (`L1759`) |
| `neosqlite/collection/aggregation_cursor.py` | `sort` (`L243`), `batch_size` (`L365`), `allow_disk_use` (`L378`) |
| `neosqlite/collection/raw_batch_cursor.py` | `batch_size` (`L56`) |
| `neosqlite/gridfs/grid_file.py` | `GridIn.__enter__` (`L406`), `GridOut.__enter__` (`L773`), `GridOutCursor.sort`/`skip`/`limit`/`batch_size`/`clone`/`rewind`/`__iter__` (`L1460-L1582`) |
| `neosqlite/collection/pipeline_context.py` | `PipelineContext.clone` (`L78`, replaces `"PipelineContext"`) |
| `neosqlite/collection/expr_evaluator/context.py` | `AggregationContext.clone` (`L102`) |
| `neosqlite/collection/jsonb_support.py` | `JSONBContext.from_db` (`L34-L35`) |
| `neosqlite/client_session.py` | `ClientSession.__enter__` (`L181`; keep `_TransactionContext.__enter__` at `L22` as `-> ClientSession`) |
| `neosqlite/connection.py` | `Connection.__enter__` (`L492`) |
| `neosqlite/changestream.py` | `ChangeStream.__iter__` (`L447`), `ChangeStream.__enter__` (`L692`) |
| `neosqlite/bulk_operations.py` | `BulkOperationContext.upsert` (`L80`) |
| `packages/nx_27017/src/nx_27017/handler.py` | `NeoSQLiteHandler.__enter__` (`L434`, replaces `"NeoSQLiteHandler"`) |

### 2.2 `@typing.override` (PEP 698) & `type` Aliases (PEP 695, Python 3.12+)

- Add `@override` (`from typing import override`) on `NeoSQLiteJSONEncoder.default` in `neosqlite/collection/json_helpers.py#L22`.
- Convert `TierChangeCallback = Callable[[str | None, str, list], None]` in `neosqlite/collection/query_engine/__init__.py#L31` to `type TierChangeCallback = Callable[[str | None, str, list[Any]], None]`.

### 2.3 Ruff `UP035` / `UP037` Typing Cleanups

- Move `Callable` and `Iterator` imports from `typing` to `collections.abc` (`UP035`) across `neosqlite/collection/` and `packages/nx_27017/src/nx_27017/changestream.py#L23`.
- Remove unnecessary string quotes from annotations in files using `from __future__ import annotations` (`UP037`) across `sql_converters/`, `query_engine/`, `query_helper/`, `operators_base.py`, and `pipeline_context.py`.

---

## Phase 3: Readability Idioms & Safe Ruff Cleanups

### 3.1 `itertools.pairwise()` for Adjacent-Boundary Loops (Python 3.10+)

Replace `for i in range(len(xs) - 1)` / `for i in range(1, len(xs))` indexing with `pairwise(xs)`:

- `neosqlite/collection/_stage_builders.py#L620-L624` (`$bucket` SQL tier)
- `neosqlite/collection/temporary_table_aggregation/operators_group.py#L910-L915` (`$bucket` temp-table tier)
- `neosqlite/collection/query_engine/python_aggregation_engine.py#L613-L622` (`$bucket` Python fallback tier)
- `neosqlite/collection/query_helper/window_operators.py#L301-L306` (`$integral` adjacent frame pairs)

### 3.2 PEP 701 F-String Quote Reuse (Python 3.12+)

Simplify nested f-strings that currently switch outer/inner quotes solely due to pre-3.12 parser rules:

- `neosqlite/collection/index_manager.py#L299`
- `neosqlite/gridfs/gridfs_bucket.py#L232`
- `neosqlite/gridfs/grid_file.py#L472`

> **Explicit Exclusion:** Keep intentional SQL identifier/string quotes such as `AS \"{field}\"` (`neosqlite/collection/query_helper/_sql_aggregation.py#L425`) where both quote characters are part of the generated SQL text.

### 3.3 Other Low-Risk `UP` / `FURB` / `PERF` Cleanups

- **`str.removeprefix()` (`FURB188`):** `neosqlite/collection/__init__.py#L281-L282`, `neosqlite/collection/__init__.py#L301-L302`, `neosqlite/collection/sql_translator_unified.py#L931`, `#L1070`, `#L1108`, `neosqlite/collection/temporary_table_aggregation/operators_match.py#L81`, `neosqlite/collection/query_helper/schema_compiler.py#L23`, `neosqlite/collection/index_manager.py#L867`.
- **Unused tuple unpacking (`PERF102`):** Use `.keys()` / `.values()` in `_sql_updates.py` (`L876`, `L1102`, `L1596`), `update_operations.py` (`L444`), and `expr_temp_table.py` (`L888`).
- **Stale UTF-8 headers (`UP009`):** Remove `# coding: utf-8` in `neosqlite/bulk_operations.py#L1` and `packages/nx_27017/tests/conftest.py#L1`.
- **Careful Audit Item (`UP028`):** `yield from self._load_documents(rows)` in `neosqlite/collection/cursor.py#L1014` and `#L1116` changes `.close()` / `.send()` delegation semantics when early-exiting a cursor inside `try/finally: db_cursor.close()`; verify early-close tests explicitly before adopting.

---

## Verification & Acceptance Criteria

After each phase (and before committing):

1. Formatting and static checks:
   - `poetry run black --check -t py312 -l 80 .`
   - `poetry run ruff check --select=I,F,UP,PERF,FURB --target-version=py312 neosqlite packages`
   - `poetry run mypy .` (and `poetry run mypy packages/nx_27017`)
2. Test suites:
   - `PYTHONPATH=. poetry run pytest tests/ -n $(sysctl -n hw.physicalcpu) --cov=neosqlite --cov-report=term-missing --cov-fail-under=80`
   - `PYTHONPATH=packages/nx_27017/src:. poetry run pytest packages/nx_27017/tests`
3. Mark implemented sections in this document and log changes in [../log.md](../log.md) when committing.
