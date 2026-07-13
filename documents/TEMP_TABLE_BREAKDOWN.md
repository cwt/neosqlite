# Temporary Table Aggregation — Refactor Plan

**Goal:** Split `neosqlite/collection/temporary_table_aggregation.py` (~4,285 lines) into a
**package** of the same name so that `from neosqlite.collection.temporary_table_aggregation import X`
works unchanged. **Zero behavior changes. Zero test edits.**

> ## Status
> - **Phase 1 — package split: ✅ COMPLETE & verified** (old `.py` deleted, `ruff` + `mypy` clean,
>   300 tests pass). See "Corrections applied during execution" below.
> - **Phase 2 — split `operators.py`: ✅ COMPLETE & verified** (7 mixin modules + `operators_base.py`,
>   `ruff` + `mypy` clean, 300 tests pass). See Section 10.

> **Source-of-truth rule:** The symbol inventory in Section 3 below was generated directly from the
> current source file (every `def` / `class` line). The original plan's table was stale and listed
> methods that do not exist. If the source changes again, **regenerate Section 3 from the file**
> (`grep -rnE '^(def |class |    def |HASH_JOIN_MEMORY_THRESHOLD)' neosqlite/collection/temporary_table_aggregation/`)
> before executing — do not trust a hand-count.

---

## 1. Target Package Layout

```
neosqlite/collection/temporary_table_aggregation/
├── __init__.py          # Public API re-exports + TemporaryTableAggregationProcessor (__init__ + process_pipeline)
├── operators.py         # OperatorsMixin (all 40 instance methods + HASH_JOIN_MEMORY_THRESHOLD)
├── utils.py             # Pure module-level functions only (no self.*)
├── manager.py           # DeterministicTempTableManager + aggregation_pipeline_context (+ nested create_temp_table)
└── core.py              # execute_2nd_tier_aggregation + can_process_with_temporary_tables
```

**Constraint:** Directory name must be exactly `temporary_table_aggregation` with `__init__.py` so the import path is preserved.

---

## 2. External Dependencies (MUST EXIST OUTSIDE THIS PACKAGE)

These modules are imported by the source file and **must stay as sibling modules** in
`neosqlite/collection/`. The source used **mixed** relative levels, so inside the new package the
rules differ by depth:
- **Collection-level siblings** (`expr_evaluator`, `json_path_utils`, `jsonb_support`,
  `sql_translator_unified`, `json_helpers`, `query_helper`): were `from .X` in source → `from ..X`.
- **Top-level `neosqlite` modules** (`_sqlite`, `objectid`, `sql_utils`): were `from ..X` in source
  → **`from ...X`** (triple-dot). Getting this wrong is a silent `ImportError` at runtime.

(The first mechanical cut wrote `_sqlite`/`objectid`/`sql_utils` as `..` and failed at import. The
table below shows the **correct** final form.)

| Module | Symbols actually imported by the source |
|--------|------------------------------------------|
| `..._sqlite` (triple-dot — top-level `neosqlite` module) | `sqlite3` |
| `...objectid` (triple-dot — top-level `neosqlite` module) | `ObjectId` |
| `...sql_utils` (triple-dot — top-level `neosqlite` module) | `quote_table_name` |
| `..expr_evaluator` | `AggregationContext`, `ExprEvaluator`, `_is_expression` |
| `..json_path_utils` | `parse_json_path` |
| `..jsonb_support` | `_contains_text_operator`, `_get_json_each_function`, `_get_json_function_prefix`, `_get_json_group_array_function`, `_get_json_tree_function`, `json_data_column`, `supports_jsonb`, `supports_jsonb_each` |
| `..sql_translator_unified` | `SQLTranslator` |
| `..query_helper` | `get_force_fallback` (**lazy / function-local import only** — see Rule 7) |

> **Correction vs. old plan:** `SQLTranslator`, `ObjectId`, and the whole `sql_utils` / `json_path_utils`
> set were missing or mislabeled. `expr_evaluator` actually exports `ExprEvaluator` + `AggregationContext`
> (not `ExpressionEvaluator` / `_evaluate_expression`). `_contains_text_search` is **defined locally** in
> the source (delegates to `_contains_text_operator`) — it is NOT imported from `jsonb_support`, so it is
> moved, not re-imported.

**Do not move these into the new package.** Import collection-level ones from `..` and top-level
`neosqlite` ones (`_sqlite`, `objectid`, `sql_utils`) from `...` (see table).

### 2.2 Corrections applied during Phase 1 execution
The first mechanical split failed at import time; three fixes were required before tests passed:
1. **Triple-dot imports for top-level modules.** `_sqlite`, `objectid`, `sql_utils` live at the
   `neosqlite` top level, so inside the package they need `from ...X` (not `..X`). The plan table in
   Section 2 was initially wrong here — the correct form is shown above.
2. **Preserve `@contextmanager` on `aggregation_pipeline_context`.** A naive slice dropped the
   decorator, turning the generator into a plain generator-returning function and breaking every
   call site. It must stay a `@contextmanager`.
3. **`OperatorsMixin` needs class-level attribute annotations** for mypy strict — declare
   `collection`, `db`, `query_engine`, `expr_evaluator`, `sql_translator`, `_jsonb_supported`,
   `_jsonb_each_supported`, `_json_each_function`, `_json_function_prefix`, `json_group_array_function`,
   `_has_sort_stage`, `_text_on_temp_table_warned`, `_has_unwind_in_pipeline`. This mirrors the existing
   `SqlConvertersMixin` pattern in `expr_evaluator/sql_converters.py`. Without them mypy errors on every
   `self.X` use.

---

## 3. Symbol → File Mapping (Complete — verified against current source)

### 3a. Module-level (no `self`)

| Symbol | Destination |
|--------|-------------|
| `_sanitize_params` | `utils.py` |
| `_json_extract_field_with_objectid_support` | `utils.py` |
| `_contains_text_search` (defined locally; delegates to `_contains_text_operator`) | `utils.py` |
| `DeterministicTempTableManager` | `manager.py` |
| `aggregation_pipeline_context` (and its nested `create_temp_table`) | `manager.py` |
| `can_process_with_temporary_tables` | `core.py` |
| `execute_2nd_tier_aggregation` | `core.py` |
| `HASH_JOIN_MEMORY_THRESHOLD = 100 * 1024 * 1024` | `operators.py` |

### 3b. `TemporaryTableAggregationProcessor` Instance Methods → `operators.py` (OperatorsMixin)

All **40** methods below become methods of `class OperatorsMixin` in `operators.py`. **Copy verbatim.**

```
_process_match_stage
_process_unwind_stages
_create_lookup_hash_table
_create_lookup_hash_table_fallback
_estimate_collection_size
_get_available_memory
_should_use_hash_join
_extract_field_value
_process_lookup_stage
_process_lookup_correlated_subquery
_process_lookup_hash_join
_process_sort_skip_limit_stage
_process_add_fields_stage
_process_add_fields_stage_python_hybrid
_is_expression                       # instance method — distinct from imported _is_expression_module
_process_project_stage
_process_project_exclusion
_process_project_inclusion
_generate_text_score_sql
_process_replace_root_stage
_process_group_stage
_id_to_json_object_args
_get_results_from_table
_matches_text_search
_batch_insert_documents
_process_text_search_stage
_detect_fts_tokenizer
_process_bucket_stage
_build_group_by_expr
_process_bucket_auto_stage
_process_densify_stage
_process_facet_stage
_process_union_with_stage
_process_merge_stage
_process_redact_stage
_process_set_window_fields_stage
_map_window_operator_to_sql
_build_window_frame_sql
_process_graph_lookup_stage
_process_fill_stage
```

> **Total: 40 instance methods** → all go to `OperatorsMixin` in `operators.py`.

> **Stages handled inline in `process_pipeline` (no separate `_process_*_stage` method exists):**
> `$count`, `$sample`, `$sort`, `$limit`, `$skip`, `$set`, `$out` (and `$unset`) are dispatched inside
> `process_pipeline`'s `match/case` block. The old plan listed phantom methods
> `_process_count_stage`, `_process_sample_stage`, `_process_sort_stage`, `_process_limit_stage`,
> `_process_skip_stage`, `_process_set_stage`, `_process_out_stage`, and a non-existent singular
> `_process_unwind_stage` — **none of these exist in the source; do not create them.** Copy
> `process_pipeline` verbatim (it stays in `__init__.py`).

> All 40 methods rely on attributes set in `TemporaryTableAggregationProcessor.__init__`
> (`self.db`, `self.expr_evaluator`, `self._json_function_prefix`, `self._jsonb_supported`,
> `self._jsonb_each_supported`, `self._json_each_function`, `self.json_group_array_function`, …).
> That is expected: `OperatorsMixin` is the base class and the processor's `__init__` supplies them.

### 3c. `TemporaryTableAggregationProcessor` Class Members Staying in `__init__.py`

| Member | Stays in `__init__.py` |
|--------|------------------------|
| `__init__` | Yes (full body verbatim — needs its own import block, see 4.5) |
| `process_pipeline` | Yes (full `match/case` dispatch verbatim) |

---

## 4. Module Specifications

### 4.1 `utils.py` — Pure Module-Level Functions Only

```python
from __future__ import annotations
from typing import Any
from ...objectid import ObjectId
from ..jsonb_support import _contains_text_operator

logger = logging.getLogger(__name__)

# --- Copy verbatim from source: _sanitize_params, _json_extract_field_with_objectid_support, _contains_text_search ---
```

**No class definitions. No `self` references.**

---

### 4.2 `manager.py` — Manager Class + Context Manager

```python
from __future__ import annotations
import hashlib
import logging
import uuid
from contextlib import contextmanager
from typing import Any

logger = logging.getLogger(__name__)

# --- Copy verbatim from source: DeterministicTempTableManager, aggregation_pipeline_context ---
```

**Imports allowed:** stdlib + `..` siblings used by this code only. Note `create_temp_table` is a
**nested function inside `aggregation_pipeline_context`** — move it together with the context manager.

---

### 4.3 `core.py` — Tier-2 Execution + Capability Check

```python
from __future__ import annotations
import logging
from typing import Any
from ..._sqlite import sqlite3

logger = logging.getLogger(__name__)

# --- Copy verbatim from source: can_process_with_temporary_tables, execute_2nd_tier_aggregation ---
```

**Lazy import pattern required inside `execute_2nd_tier_aggregation` (keep exactly as in source):**
```python
def execute_2nd_tier_aggregation(...):
    from ..query_helper import get_force_fallback   # function-local only
    from . import TemporaryTableAggregationProcessor  # function-local only
    ...
```

---

### 4.4 `operators.py` — OperatorsMixin (All 40 Instance Methods)

```python
from __future__ import annotations
import logging
import re
import hashlib
import uuid
import warnings
from typing import Any, Callable, List, Dict, Optional, Tuple, Union, Set, Iterator
from ..._sqlite import sqlite3
from ...sql_utils import quote_table_name
from ..expr_evaluator import (
    AggregationContext,
    ExprEvaluator,
    _is_expression as _is_expression_module,
)
from ..json_path_utils import parse_json_path
from ..jsonb_support import (
    _contains_text_operator,
    _get_json_each_function,
    _get_json_function_prefix,
    _get_json_group_array_function,
    _get_json_tree_function,
    json_data_column,
    supports_jsonb,
    supports_jsonb_each,
)
from .utils import (
    _sanitize_params,
    _json_extract_field_with_objectid_support,
    _contains_text_search,
)

logger = logging.getLogger(__name__)

HASH_JOIN_MEMORY_THRESHOLD = 100 * 1024 * 1024

class OperatorsMixin:
    # --- Paste ALL 40 instance methods verbatim from source ---
    # (full list in Section 3b)
    def _process_match_stage(self, ...): ...
    # ... all 40 methods ...

    # Instance method _is_expression (distinct from imported _is_expression_module)
    def _is_expression(self, ...): ...
```

**Correction vs. old plan:** `ObjectId` and `SQLTranslator` are **not** imported here — `ObjectId` is
only used by `_sanitize_params` (in `utils.py`) and `SQLTranslator` is only used by
`TemporaryTableAggregationProcessor.__init__` (in `__init__.py`). Adding them here would be unused
imports (ruff F401).

**Keep `get_force_fallback` lazy.** In the source it is imported function-local inside
`_process_project_stage` and `_process_group_stage` (and in `execute_2nd_tier_aggregation`). Preserve
that — do **not** add a top-level `from ..query_helper import get_force_fallback`.

**Critical:** Do **not** import `TemporaryTableAggregationProcessor` at module level. Use the
function-local `from . import TemporaryTableAggregationProcessor` pattern only where the source uses it.

---

### 4.5 `__init__.py` — Public API + Processor Class

`__init__.py` holds both the **re-exports** AND the verbatim bodies of `__init__` and `process_pipeline`,
which themselves reference imports. Give it its own import block:

```python
from __future__ import annotations
import logging
from typing import Any
from ..._sqlite import sqlite3
from ...objectid import ObjectId
from ...sql_utils import quote_table_name
from ..expr_evaluator import AggregationContext, ExprEvaluator
from ..json_path_utils import parse_json_path
from ..jsonb_support import (
    _contains_text_operator,
    _get_json_each_function,
    _get_json_function_prefix,
    _get_json_group_array_function,
    _get_json_tree_function,
    json_data_column,
    supports_jsonb,
    supports_jsonb_each,
)
from ..sql_translator_unified import SQLTranslator

from .operators import OperatorsMixin, HASH_JOIN_MEMORY_THRESHOLD
from .manager import DeterministicTempTableManager, aggregation_pipeline_context
from .core import can_process_with_temporary_tables, execute_2nd_tier_aggregation
from .utils import (
    _sanitize_params,
    _json_extract_field_with_objectid_support,
    _contains_text_search,
)

logger = logging.getLogger(__name__)

__all__ = [
    "TemporaryTableAggregationProcessor",
    "DeterministicTempTableManager",
    "aggregation_pipeline_context",
    "can_process_with_temporary_tables",
    "execute_2nd_tier_aggregation",
    "_sanitize_params",
    "_json_extract_field_with_objectid_support",
    "_contains_text_search",
    "HASH_JOIN_MEMORY_THRESHOLD",
]

class TemporaryTableAggregationProcessor(OperatorsMixin):
    # --- Copy __init__ verbatim from source (uses SQLTranslator, supports_jsonb,
    #     _get_json_function_prefix, ExprEvaluator, quote_table_name, ...) ---
    def __init__(self, ...): ...

    # --- Copy process_pipeline verbatim from source (full match/case dispatch) ---
    def process_pipeline(self, ...): ...
```

**Correction vs. old plan:** the old `__init__.py` spec had *no* import block for the actual code in
`__init__`/`process_pipeline`. Copying those two methods verbatim requires the imports above or the
module will fail with `NameError`.

---

## 4.6 Import-Trim Safety Rule (applies to every module)

Because partitioning imports by hand is error-prone, after writing each module run:

```bash
./scripts/lint-check-and-fix.sh   # ruff
```

- If ruff reports **F401 (unused import)**, delete that import line.
- If ruff reports **F821 (undefined name)**, add the missing import from the source's top block
  (lines 7–33), rewriting `from .X` → `from ..X`.

This guarantees a clean import graph without guessing.

---

## 5. Implementation Order (Execute Sequentially)

| Step | Action | Files Created |
|------|--------|---------------|
| 1 | Create package directory | `neosqlite/collection/temporary_table_aggregation/` |
| 2 | Write `utils.py` (pure functions) | `utils.py` |
| 3 | Write `manager.py` (manager + context) | `manager.py` |
| 4 | Write `core.py` (tier-2 + capability) | `core.py` |
| 5 | Write `operators.py` (OperatorsMixin + all 40 methods) | `operators.py` |
| 6 | Write `__init__.py` (re-exports + Processor class + its imports) | `__init__.py` |
| 7 | **Delete** `neosqlite/collection/temporary_table_aggregation.py` | — |
| 8 | Run verification checklist | — |

**Do not skip steps. Do not reorder.** Steps 1–6 build the package *while the old `.py` still exists*;
that coexistence is fine as long as you **do not import the package** until step 7 deletes the old file
(see Pitfalls). Import only after step 7.

---

## 6. Critical Rules (Non-Negotiable)

1. **No behavior changes.** Copy code verbatim. No reformatting, no docstring edits, no logic tweaks.
2. **`from __future__ import annotations`** at top of **every** new module.
3. **No circular imports:**
   - `operators.py` imports from `.utils` (OK, utils has no deps) and from `..` siblings.
   - `operators.py` does **NOT** import from `.` or `.__init__`.
   - `core.py` uses **function-local** `from . import TemporaryTableAggregationProcessor` only.
   - `__init__.py` imports from `.operators`, `.manager`, `.core`, `.utils` (leaf modules).
4. **`_is_expression` appears twice:**
   - Module-level import: `from ..expr_evaluator import _is_expression as _is_expression_module`
   - Instance method: `def _is_expression(self, ...): ...` in `OperatorsMixin`
   - Keep both. Do not merge.
5. **External deps stay external.** Collection-level ones use the `..` prefix; top-level `neosqlite`
   ones (`_sqlite`, `objectid`, `sql_utils`) use `...` (triple-dot). Do not move them in.
6. **`get_force_fallback` stays lazy** (function-local) wherever the source had it — never a top-level
   import in `operators.py` or `core.py`. This avoids a circular import through `query_helper`.
7. **Delete the old `.py` file** after verification. A leftover file alongside the package directory
   causes import ambiguity.

8. **Rewrite internal self-imports when moving code.** The source contains at least one self-reference
   that works only as a single file: `from .temporary_table_aggregation import (TemporaryTableAggregationProcessor,)`
   inside `_process_lookup_stage` (source line ~1369). Once that method lives in `operators.py`, the path
   `.temporary_table_aggregation` no longer exists as a submodule and raises `ModuleNotFoundError`. Rewrite
   every such `from .temporary_table_aggregation import X` (and `from ..temporary_table_aggregation import X`)
   found inside the moved code to `from . import X` (lazy, as the source already does it function-local).
   This is the one place "copy verbatim" must be adjusted.

---

## 7. Verification Checklist (All Must Pass)

```bash
# 0. Old module must be gone (no import ambiguity)
test ! -f neosqlite/collection/temporary_table_aggregation.py && echo "old module removed OK"

# 1. Import succeeds with all public symbols
python -c "
import neosqlite.collection.temporary_table_aggregation as m
expected = [
    'TemporaryTableAggregationProcessor',
    'DeterministicTempTableManager',
    'aggregation_pipeline_context',
    'can_process_with_temporary_tables',
    'execute_2nd_tier_aggregation',
    '_sanitize_params',
    '_json_extract_field_with_objectid_support',
    '_contains_text_search',
]
missing = [n for n in expected if not hasattr(m, n)]
print('Missing public symbols:', missing)
assert not missing, missing
print('Public API OK')
"

# 2. ALL 40 operator methods moved (catches any missed/forgotten method)
python -c "
from neosqlite.collection.temporary_table_aggregation import TemporaryTableAggregationProcessor as P
methods = [
    '_process_match_stage','_process_unwind_stages','_create_lookup_hash_table',
    '_create_lookup_hash_table_fallback','_estimate_collection_size','_get_available_memory',
    '_should_use_hash_join','_extract_field_value','_process_lookup_stage',
    '_process_lookup_correlated_subquery','_process_lookup_hash_join','_process_sort_skip_limit_stage',
    '_process_add_fields_stage','_process_add_fields_stage_python_hybrid','_is_expression',
    '_process_project_stage','_process_project_exclusion','_process_project_inclusion',
    '_generate_text_score_sql','_process_replace_root_stage','_process_group_stage',
    '_id_to_json_object_args','_get_results_from_table','_matches_text_search',
    '_batch_insert_documents','_process_text_search_stage','_detect_fts_tokenizer',
    '_process_bucket_stage','_build_group_by_expr','_process_bucket_auto_stage',
    '_process_densify_stage','_process_facet_stage','_process_union_with_stage',
    '_process_merge_stage','_process_redact_stage','_process_set_window_fields_stage',
    '_map_window_operator_to_sql','_build_window_frame_sql','_process_graph_lookup_stage',
    '_process_fill_stage',
]
missing = [m for m in methods if not hasattr(P, m)]
print('Missing operator methods:', missing)
assert not missing, missing
assert len(methods) == 40
print('All 40 operator methods present')
"

# 3. Internal consumers still import cleanly
python -c "
import neosqlite.collection.datetime_query_processor
import neosqlite.collection.query_engine
import neosqlite.collection.temporary_table_aggregation
print('OK')
"

# 4. Full test suite passes (no test edits allowed)
pytest tests/test_temp_table_aggregation_fixes.py \
       tests/test_temp_table_aggregation_errors.py \
       tests/test_query_engine_suite.py \
       tests/test_aggregation_pipeline.py \
       tests/test_jsonb_temporary_tables.py \
       tests/test_tier2/ -v

# 5. Smoke test: instantiate processor and run a tiny pipeline
python -c "
import neosqlite
db = neosqlite.Connection(':memory:')
coll = db['test']
coll.insert_many([{'x': i} for i in range(10)])
proc = neosqlite.collection.temporary_table_aggregation.TemporaryTableAggregationProcessor(coll)
result = list(proc.process_pipeline([{'\$match': {'x': {'\$gte': 5}}}, {'\$sort': {'x': 1}}, {'\$limit': 3}]))
print('Results:', result)
assert len(result) == 3
print('Smoke test passed')
"

# 6. CI quality gates — must pass with ZERO warnings, ZERO errors
./scripts/runtest.sh      # full test suite with coverage >= 80%
./scripts/type-check.sh   # mypy clean
./scripts/lint-check-and-fix.sh  # ruff clean (imports, pyflakes)
```

---

## 8. Common Pitfalls Checklist

- [ ] Forgot `from __future__ import annotations` in any module
- [ ] Left old `.py` file alongside package directory
- [ ] Added top-level import of `TemporaryTableAggregationProcessor` in `operators.py` or `core.py`
- [ ] Added a top-level `from ..query_helper import get_force_fallback` (keep it lazy)
- [ ] Merged the two `_is_expression` (module + instance) into one
- [ ] Moved `jsonb_support`, `expr_evaluator`, `sql_translator_unified`, etc. into the package (don't)
- [ ] Used `from .X` instead of `from ..X` for sibling `neosqlite/collection/` modules
- [ ] Used `..X` instead of `...X` for top-level `neosqlite` modules (`_sqlite`, `objectid`, `sql_utils`)
- [ ] **Missed any of the 40 instance methods** in `OperatorsMixin` (see checklist step 2)
- [ ] Created phantom methods (`_process_count_stage`, `_process_sort_stage`, …) that don't exist
- [ ] Forgot the import block for `__init__.py` / `process_pipeline` (NameError at runtime)
- [ ] Forgot to re-export a symbol in `__init__.py.__all__`
- [ ] Ran tests / imported the package before deleting the old `.py` file (import ambiguity)

---

## 9. Rollback Plan

If verification fails (this repo uses Mercurial):

```bash
hg revert --no-backup neosqlite/collection/temporary_table_aggregation.py
rm -rf neosqlite/collection/temporary_table_aggregation/
```

Then re-examine the source file and the mapping table in Section 3 (regenerate it from the file if
the source has since changed).

---

## 10. Phase 2 — Split `operators.py` (COMPLETE)

`operators.py` was ~148 KB / ~3,500 lines holding all 40 `OperatorsMixin` methods. Split into
**multiple mixin classes** composed into `OperatorsMixin` — purely structural, zero behavior
change. MRO resolves every `self._process_x` regardless of which mixin defines it.

### Resulting files (in `neosqlite/collection/temporary_table_aggregation/`)
| File | Contents |
|------|----------|
| `operators_base.py` | `OperatorsBaseMixin`: class-level attribute annotations + a single stub `_process_text_search_stage` (see 10.3). All mixins inherit it. |
| `operators_match.py` | `OperatorsMatchMixin` (`_process_match_stage`, `_process_unwind_stages`) |
| `operators_lookup.py` | `OperatorsLookupMixin` (9 lookup/hash-join methods) + `HASH_JOIN_MEMORY_THRESHOLD` (defined here, re-exported from `operators.py`) |
| `operators_sort_proj.py` | `OperatorsSortProjMixin` (9 sort/project/addFields/replaceRoot methods) |
| `operators_group.py` | `OperatorsGroupMixin` (6 group/bucket methods) |
| `operators_text.py` | `OperatorsTextMixin` (4 text-search methods; defines the real `_process_text_search_stage`) |
| `operators_advanced.py` | `OperatorsAdvancedMixin` (10 densify/facet/union/merge/redact/window/graphLookup/fill methods) |
| `operators.py` | `OperatorsMixin(OperatorsMatchMixin, OperatorsLookupMixin, OperatorsSortProjMixin, OperatorsGroupMixin, OperatorsTextMixin, OperatorsAdvancedMixin)` — no method bodies; re-exports `HASH_JOIN_MEMORY_THRESHOLD` via `__all__`. |

### 10.1 Mixin split (by stage family)

| Mixin file | Methods |
|------------|---------|
| `operators_match.py` | `_process_match_stage`, `_process_unwind_stages` |
| `operators_lookup.py` | `_create_lookup_hash_table`, `_create_lookup_hash_table_fallback`, `_estimate_collection_size`, `_get_available_memory`, `_should_use_hash_join`, `_extract_field_value`, `_process_lookup_stage`, `_process_lookup_correlated_subquery`, `_process_lookup_hash_join` |
| `operators_sort_proj.py` | `_process_sort_skip_limit_stage`, `_process_add_fields_stage`, `_process_add_fields_stage_python_hybrid`, `_is_expression`, `_process_project_stage`, `_process_project_exclusion`, `_process_project_inclusion`, `_generate_text_score_sql`, `_process_replace_root_stage` |
| `operators_group.py` | `_process_group_stage`, `_id_to_json_object_args`, `_get_results_from_table`, `_build_group_by_expr`, `_process_bucket_stage`, `_process_bucket_auto_stage` |
| `operators_text.py` | `_matches_text_search`, `_batch_insert_documents`, `_process_text_search_stage`, `_detect_fts_tokenizer` |
| `operators_advanced.py` | `_process_densify_stage`, `_process_facet_stage`, `_process_union_with_stage`, `_process_merge_stage`, `_process_redact_stage`, `_process_set_window_fields_stage`, `_map_window_operator_to_sql`, `_build_window_frame_sql`, `_process_graph_lookup_stage`, `_process_fill_stage` |
| `operators.py` | `OperatorsMixin(AllMixins...)` + `HASH_JOIN_MEMORY_THRESHOLD` + the lazy `from . import TemporaryTableAggregationProcessor` |

### 10.2 Constraints (same as Phase 1)

- Keep `from __future__ import annotations`; keep top-level `..._sqlite` / `...objectid` / `...sql_utils`
  and `..` collection siblings; keep `_is_expression` (module import as `_is_expression_module` + instance
  method); keep `get_force_fallback` and `TemporaryTableAggregationProcessor` lazy.
- Add class-level attribute annotations to `OperatorsMixin` (or to each mixin) — see 2.2 fix #3.
- `__init__.py` import line stays `from .operators import OperatorsMixin, HASH_JOIN_MEMORY_THRESHOLD`.
- **Verification = same as Phase 1 checklist (Section 7):** ruff clean, mypy clean, 300 tests pass,
  smoke test passes. No test edits.

### 10.3 Notes from execution
- **Single cross-mixin call.** Only one `self.<method>` call crosses a mixin boundary:
  `_process_match_stage` (match) → `_process_text_search_stage` (text). Resolved by declaring a
  permissive stub `def _process_text_search_stage(self, *args, **kwargs) -> Any: ...` on
  `OperatorsBaseMixin`; the real implementation lives in `OperatorsTextMixin`. MRO satisfies mypy.
- **`HASH_JOIN_MEMORY_THRESHOLD`** is defined in `operators_lookup.py` (the only file that uses it)
  and re-exported from `operators.py`. Without an `__all__` listing it, ruff strips the re-export
  import as F401 — so `operators.py` declares `__all__ = ["OperatorsMixin", "HASH_JOIN_MEMORY_THRESHOLD"]`.
- Methods were sliced verbatim by AST `lineno`/`end_lineno`; each mixin carries the full original
  import header and ruff trimmed unused imports per file.

---

**End of Plan.** Both Phase 1 (package split) and Phase 2 (operators split) are complete & verified.
No further phases planned.
