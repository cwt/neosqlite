"""Query Builder Mixin for NeoSQLite.

Provides Python-based query application methods (``_apply_query``).
SQL WHERE clause building lives in ``_sql_query_builder.py``
(``SqlQueryBuilderMixin``).
"""

import logging
import re
from typing import TYPE_CHECKING, Any

from ... import query_operators
from ...exceptions import MalformedQueryException
from ...sql_utils import validate_identifier
from ..expr_evaluator import ExprEvaluator
from ..index_manager import _load_index_keys
from ..text_search import unified_text_search
from ._sql_query_builder import SqlQueryBuilderMixin

if TYPE_CHECKING:
    from .. import Collection
    from ..jsonb_support import JSONBContext

logger = logging.getLogger(__name__)


def _iter_leaf_values(document: dict[str, Any], parts: list[str]) -> Any | None:
    """Resolve a dotted path, expanding arrays along the way (#99).

    Walks ``parts`` step by step starting from ``document``, descending into
    matching dictionary keys and dictionary elements inside lists while
    flattening list values by one level per step.

    Args:
        document: Root document dictionary to traverse.
        parts: Ordered field path segments split on ``"."``.

    Returns:
        Any | None: List of resolved leaf values found along the path, or
            ``None`` when any segment is absent everywhere.
    """
    current: list[Any] = [document]
    for part in parts:
        nxt: list[Any] = []
        found = False
        for node in current:
            if isinstance(node, dict) and part in node:
                found = True
                v = node[part]
                if isinstance(v, list):
                    nxt.extend(v)
                else:
                    # Preserve explicit nulls so equality/null matching works in
                    # the Python fallback (MongoDB treats present null as a value).
                    nxt.append(v)
            elif isinstance(node, list):
                # A numeric segment addresses an array index in MongoDB dot
                # notation; other segments look up keys in array elements.
                if part.isascii() and part.isdigit():
                    idx = int(part)
                    if 0 <= idx < len(node):
                        found = True
                        indexed = node[idx]
                        if isinstance(indexed, list):
                            nxt.extend(indexed)
                        else:
                            nxt.append(indexed)
                else:
                    for el in node:
                        if isinstance(el, dict) and part in el:
                            found = True
                            ev = el[part]
                            if isinstance(ev, list):
                                nxt.extend(ev)
                            else:
                                nxt.append(ev)
        if not found:
            return None
        current = nxt
    return current


class QueryBuilderMixin(SqlQueryBuilderMixin):
    """A mixin class that provides Python-side query evaluation helpers.

    Inherits SQL WHERE clause construction from ``SqlQueryBuilderMixin`` and
    implements the Python fallback query matcher (``_apply_query``).

    This mixin assumes the host class provides:
    - ``self.collection`` (with ``db`` and ``name`` attributes)
    - ``self.jsonb.jsonb_supported``
    - ``self.jsonb.json_function_prefix``
    - ``self._build_expr_where_clause`` (for handling ``$expr`` queries)
    """

    collection: "Collection"
    jsonb: "JSONBContext"
    _build_expr_where_clause: Any

    def _score_text_field(
        self, value: Any, search_term: str, search_lower: str
    ) -> float:
        """Compute relevance score contribution for an FTS-indexed field value.

        Scores strings via case-insensitive substring count or
        ``unified_text_search``, recursively sums scores across lists, and
        checks nested dicts using ``_search_in_value`` or
        ``unified_text_search``.

        Args:
            value: Field value extracted from a candidate document.
            search_term: Raw ``$search`` query string.
            search_lower: Lowercased ``search_term`` for substring counting.

        Returns:
            float: Relevance score contribution (``0.0`` when not matched).
        """
        if isinstance(value, str):
            val_lower = value.lower()
            if search_lower in val_lower:
                return 1.0 + 0.1 * val_lower.count(search_lower)
            if unified_text_search({"_": value}, search_term):
                return 1.0
            return 0.0
        if isinstance(value, list):
            return sum(
                self._score_text_field(elem, search_term, search_lower)
                for elem in value
            )
        if isinstance(value, dict):
            if self._search_in_value(value, search_term):
                return 1.0
            if unified_text_search(value, search_term):
                return 1.0
            return 0.0
        return 0.0

    def _search_in_value(self, value: Any, search_term: str) -> bool:
        """Recursively search for a term in a value (string, dict, or list).

        Args:
            value: The value to search in.
            search_term: The substring to match case-insensitively.

        Returns:
            bool: True if the search term is found, False otherwise.
        """
        match value:
            case str():
                return search_term.lower() in value.lower()
            case dict():
                return any(
                    self._search_in_value(v, search_term)
                    for v in value.values()
                )
            case list():
                return any(
                    self._search_in_value(elem, search_term) for elem in value
                )
            case _:
                return False

    def _get_fts_field_names(self) -> list[str] | None:
        """Return FTS-indexed field names for this collection, or ``None`` if none exist."""
        coll_name = validate_identifier(self.collection.name)
        escaped_coll = (
            coll_name.replace("\\", "\\\\")
            .replace("%", "\\%")
            .replace("_", "\\_")
        )
        cursor = self.collection.db.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' AND name LIKE ? ESCAPE '\\'",
            (f"{escaped_coll}\\_%\\_fts",),
        )
        fts_tables = cursor.fetchall()
        if not fts_tables:
            return None

        # Filter out FTS tables belonging to other collections
        # whose names start with `{coll_name}_`
        all_tables = {
            row[0]
            for row in self.collection.db.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        longer_prefixes = [
            tbl + "_"
            for tbl in all_tables
            if tbl != coll_name and tbl.startswith(coll_name + "_")
        ]

        field_names: list[str] = []
        prefix_len = len(f"{coll_name}_")
        for fts_table in fts_tables:
            fts_table_name = fts_table[0]
            base_name = fts_table_name[:-4]
            if any(base_name.startswith(lp) for lp in longer_prefixes):
                continue
            index_name = fts_table_name[prefix_len:-4]
            stored_keys = _load_index_keys(self.collection.db, fts_table_name)
            field_names.append(
                stored_keys[0] if stored_keys else index_name.replace("_", ".")
            )
        return field_names

    @staticmethod
    def _get_expr_evaluator(_context: dict[str, Any], db: Any) -> ExprEvaluator:
        """Return cached ``ExprEvaluator`` from query evaluation ``_context``."""
        evaluator = _context.get("evaluator")
        if evaluator is None:
            evaluator = ExprEvaluator(data_column="data", db_connection=db)
            _context["evaluator"] = evaluator
        return evaluator

    def _apply_query(
        self,
        query: dict[str, Any],
        document: dict[str, Any],
        _context: dict[str, Any] | None = None,
    ) -> bool:
        """Apply a query to a document to check if it matches the criteria.

        Handles ``$expr``, direct 2-argument expression comparisons, ``$text``
        (updating ``document["_textScore"]`` when FTS tables exist), logical
        operators (``$and``, ``$or``, ``$nor``, ``$not``), ``$jsonSchema``,
        field operator dictionaries, regex patterns, and dotted-path equality
        or array-element matching.

        Args:
            query: Dictionary representing the MongoDB-style query criteria.
            document: Document dictionary to evaluate against ``query``.
            _context: Optional per-query cache reused across documents and
                recursive subqueries to avoid repeated ``ExprEvaluator``
                creation and ``sqlite_master`` metadata lookups.

        Returns:
            bool: True if the document matches the query, False otherwise.
        """
        if document is None:
            return False
        if _context is None:
            _context = {}
        matches: list[bool] = []

        def reapply(q: dict[str, Any]) -> bool:
            """Recursively evaluate a sub-query against the outer document.

            Args:
                q: Sub-query dictionary to apply to ``document``.

            Returns:
                bool: True if ``document`` matches ``q``, False otherwise.
            """
            return self._apply_query(q, document, _context)

        for field, value in query.items():
            match field:
                case "$expr":
                    # Handle $expr operator in Python fallback
                    evaluator = self._get_expr_evaluator(
                        _context, self.collection.db
                    )
                    result = evaluator._evaluate_expr_python(value, document)
                    matches.append(bool(result))
                case "$where":
                    from ...js import (
                        JsWorkerError,
                        get_js_worker,
                        js_unavailable_reason,
                    )

                    worker = get_js_worker()
                    if worker is None:
                        raise NotImplementedError(
                            "The '$where' operator is not supported in NeoSQLite "
                            f"(no supported JS engine: {js_unavailable_reason()})."
                        )
                    if not isinstance(value, str):
                        raise ValueError("$where requires a JavaScript string")
                    stripped = value.strip()
                    code = (
                        stripped
                        if stripped.startswith("function")
                        else f"function() {{ return ({stripped}); }}"
                    )
                    try:
                        matches.append(
                            bool(worker.call(code, this_arg=document))
                        )
                    except JsWorkerError as exc:
                        raise ValueError(
                            f"JavaScript error in $where: {exc}"
                        ) from exc
                case "$gt" | "$lt" | "$gte" | "$lte" | "$eq" | "$ne" | "$cmp":
                    # Handle direct comparison expressions (without $expr wrapper)
                    # These are expressions like {"$gt": [{"$sin": "$angle"}, 0.5]}
                    # Check if value is an array (expression form) vs dict (field operator form)
                    if isinstance(value, list) and len(value) == 2:
                        # This is a direct expression, not a field operator
                        evaluator = self._get_expr_evaluator(
                            _context, self.collection.db
                        )
                        result = evaluator._evaluate_expr_python(
                            query, document
                        )
                        matches.append(bool(result))
                        break  # Direct expression is the entire query
                    # Otherwise, fall through to normal field operator handling
                case "$text":
                    # Handle $text operator in Python fallback
                    text_match = False
                    if isinstance(value, dict) and "$search" in value:
                        search_term = value["$search"]
                        if isinstance(search_term, str):
                            if "fts_fields" not in _context:
                                _context["fts_fields"] = (
                                    self._get_fts_field_names()
                                )
                            fts_fields: list[str] | None = _context[
                                "fts_fields"
                            ]

                            # Check each FTS-indexed field for matches and accumulate score
                            if fts_fields is not None:
                                search_lower = search_term.lower()
                                matched_score = 0.0
                                for field_name in fts_fields:
                                    try:
                                        field_value = self.collection._get_val(
                                            document, field_name
                                        )
                                    except (AttributeError, TypeError) as e:
                                        logger.debug(
                                            f"Failed to get field '{field_name}' for FTS matching: {e}"
                                        )
                                        continue

                                    if field_value is not None:
                                        field_score = self._score_text_field(
                                            field_value,
                                            search_term,
                                            search_lower,
                                        )
                                        if field_score > 0.0:
                                            text_match = True
                                            matched_score += field_score

                                if text_match:
                                    document["_textScore"] = (
                                        document.get("_textScore", 0.0)
                                        + matched_score
                                    )
                            else:
                                # No FTS indexes, search all fields
                                text_match = unified_text_search(
                                    document, search_term
                                )

                    matches.append(text_match)
                case "$and":
                    matches.append(all(map(reapply, value)))
                case "$or":
                    matches.append(any(map(reapply, value)))
                case "$nor":
                    matches.append(not any(map(reapply, value)))
                case "$not":
                    if not isinstance(value, dict):
                        raise MalformedQueryException(
                            "$not requires a dictionary query expression"
                        )
                    matches.append(
                        not self._apply_query(value, document, _context)
                    )
                case "$jsonSchema":
                    from .schema_validator import matches_json_schema

                    matches.append(matches_json_schema(document, value))
                case _:
                    if isinstance(value, dict):
                        # Extract $options for $regex if present
                        options = value.get("$options", "")
                        if options and "$regex" not in value:
                            raise MalformedQueryException(
                                "Can't use $options without $regex"
                            )

                        for operator, arg in value.items():
                            if operator == "$options":
                                # $options is handled together with $regex
                                continue

                            if operator == "$not":
                                if not isinstance(arg, (dict, re.Pattern)):
                                    raise MalformedQueryException(
                                        "$not requires a regex or operator expression"
                                    )
                                if self._apply_query(
                                    {field: arg}, document, _context
                                ):
                                    matches.append(False)
                                    break
                                continue

                            fn = self._get_operator_fn(operator)
                            # Call operator function, passing options if it's $regex
                            if operator == "$regex":
                                if not fn(
                                    field, arg, document, options=options
                                ):
                                    matches.append(False)
                                    break
                            else:
                                if not fn(field, arg, document):
                                    matches.append(False)
                                    break
                        else:
                            matches.append(True)
                    else:
                        from ...query_operators import _bson_values_equal

                        doc_value: dict[str, Any] | None = document
                        if doc_value and field in doc_value:
                            doc_value = doc_value.get(field, None)
                        else:
                            for path in field.split("."):
                                if isinstance(doc_value, dict):
                                    doc_value = doc_value.get(path, None)
                                elif (
                                    isinstance(doc_value, list)
                                    and path.isascii()
                                    and path.isdigit()
                                ):
                                    idx = int(path)
                                    doc_value = (
                                        doc_value[idx]
                                        if 0 <= idx < len(doc_value)
                                        else None
                                    )
                                else:
                                    doc_value = None
                                if doc_value is None:
                                    break

                        if isinstance(value, re.Pattern):
                            if doc_value is None or not value.search(
                                str(doc_value)
                            ):
                                matches.append(False)
                        else:
                            candidates = _iter_leaf_values(
                                document, field.split(".")
                            )
                            if candidates is None:
                                if not _bson_values_equal(value, doc_value):
                                    matches.append(False)
                            elif isinstance(value, (list, dict, re.Pattern)):
                                if not _bson_values_equal(value, doc_value):
                                    matches.append(False)
                            elif not any(
                                _bson_values_equal(cv, value)
                                for cv in candidates
                                for cv in (cv if isinstance(cv, list) else [cv])
                            ):
                                matches.append(False)
        return all(matches)

    def _get_operator_fn(self, op: str) -> Any:
        """Retrieve the Python operator callable from ``query_operators``.

        Maps a ``$``-prefixed operator name (such as ``"$gt"``) to the
        corresponding ``_``-prefixed function on ``query_operators``.

        Args:
            op: Operator string starting with ``"$"``.

        Returns:
            Any: Callable implementing the query operator.

        Raises:
            MalformedQueryException: If ``op`` does not start with ``"$"`` or
                is not implemented in ``query_operators``.
        """
        if not op.startswith("$"):
            raise MalformedQueryException(
                f"Operator '{op}' is not a valid query operation"
            )
        try:
            return getattr(query_operators, op.replace("$", "_"))
        except AttributeError:
            raise MalformedQueryException(
                f"Operator '{op}' is not currently implemented"
            )
