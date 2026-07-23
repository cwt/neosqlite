"""
Query Builder Mixin for NeoSQLite.

Provides Python-based query application methods (_apply_query).
SQL WHERE clause building lives in _sql_query_builder.py (SqlQueryBuilderMixin).
"""

import logging
import re
from typing import TYPE_CHECKING, Any

from ... import query_operators
from ...exceptions import MalformedQueryException
from ...sql_utils import quote_table_name
from ..expr_evaluator import ExprEvaluator
from ..text_search import unified_text_search
from ._sql_query_builder import SqlQueryBuilderMixin

if TYPE_CHECKING:
    from .. import Collection
    from ..jsonb_support import JSONBContext

logger = logging.getLogger(__name__)


class QueryBuilderMixin(SqlQueryBuilderMixin):
    """
    A mixin class that provides query building capabilities.

    This mixin assumes it will be used with a class that has:
    - self.collection (with db and name attributes)
    - self.jsonb.jsonb_supported
    - self.jsonb.json_function_prefix
    - self._build_expr_where_clause method (for handling $expr queries)
    """

    collection: "Collection"
    jsonb: "JSONBContext"
    _build_expr_where_clause: Any

    def _search_in_value(self, value: Any, search_term: str) -> bool:
        """
        Recursively search for a term in a value (string, dict, or list).

        Args:
            value: The value to search in
            search_term: The term to search for

        Returns:
            bool: True if the search term is found, False otherwise
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

    def _apply_query(
        self,
        query: dict[str, Any],
        document: dict[str, Any],
    ) -> bool:
        """
        Applies a query to a document to determine if it matches the query criteria.

        Handles logical operators ($and, $or, $nor, $not) and nested field paths.
        Processes both simple equality checks and complex query operators.

        Args:
            query (dict[str, Any]): A dictionary representing the query criteria.
            document (dict[str, Any]): The document to apply the query to.

        Returns:
            bool: True if the document matches the query, False otherwise.
        """
        if document is None:
            return False
        matches: list[bool] = []

        def reapply(q: dict[str, Any]) -> bool:
            """
            Recursively apply the query to the document to determine if it matches
            the query criteria.

            Args:
                q (dict[str, Any]): The query to apply.
                document (dict[str, Any]): The document to apply the query to.

            Returns:
                bool: True if the document matches the query, False otherwise.
            """
            return self._apply_query(q, document)

        for field, value in query.items():
            match field:
                case "$expr":
                    # Handle $expr operator in Python fallback
                    evaluator = ExprEvaluator(
                        data_column="data", db_connection=self.collection.db
                    )
                    result = evaluator._evaluate_expr_python(value, document)
                    matches.append(bool(result))
                case "$gt" | "$lt" | "$gte" | "$lte" | "$eq" | "$ne" | "$cmp":
                    # Handle direct comparison expressions (without $expr wrapper)
                    # These are expressions like {"$gt": [{"$sin": "$angle"}, 0.5]}
                    # Check if value is an array (expression form) vs dict (field operator form)
                    if isinstance(value, list) and len(value) == 2:
                        # This is a direct expression, not a field operator
                        evaluator = ExprEvaluator(
                            data_column="data", db_connection=self.collection.db
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
                            # Find FTS tables for this collection to determine which fields are indexed
                            cursor = self.collection.db.execute(
                                "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE ?",
                                (
                                    f"{quote_table_name(self.collection.name)}_%_fts",
                                ),
                            )
                            fts_tables = cursor.fetchall()

                            # Check each FTS-indexed field for matches
                            if fts_tables:
                                for fts_table in fts_tables:
                                    fts_table_name = fts_table[0]
                                    index_name = fts_table_name[
                                        len(
                                            f"{quote_table_name(self.collection.name)}_"
                                        ) : -4
                                    ]
                                    field_name = index_name.replace("_", ".")
                                    try:
                                        field_value = self.collection._get_val(
                                            document, field_name
                                        )
                                    except (AttributeError, TypeError) as e:
                                        logger.debug(
                                            f"Failed to get field '{field_name}' for FTS matching: {e}"
                                        )
                                        continue

                                    if field_value and isinstance(
                                        field_value, str
                                    ):
                                        if (
                                            search_term.lower()
                                            in field_value.lower()
                                        ):
                                            text_match = True
                                            break
                                    elif isinstance(field_value, list):
                                        for elem in field_value:
                                            if (
                                                isinstance(elem, str)
                                                and search_term.lower()
                                                in elem.lower()
                                            ):
                                                text_match = True
                                                break
                                            elif isinstance(
                                                elem, dict
                                            ) and self._search_in_value(
                                                elem, search_term
                                            ):
                                                text_match = True
                                                break
                                        if text_match:
                                            break
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
                    matches.append(not self._apply_query(value, document))
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
                        doc_value: dict[str, Any] | None = document
                        if doc_value and field in doc_value:
                            doc_value = doc_value.get(field, None)
                        else:
                            for path in field.split("."):
                                if not isinstance(doc_value, dict):
                                    break
                                doc_value = doc_value.get(path, None)

                        if isinstance(value, re.Pattern):
                            if doc_value is None or not value.search(
                                str(doc_value)
                            ):
                                matches.append(False)
                        elif value != doc_value:
                            matches.append(False)
        return all(matches)

    def _get_operator_fn(self, op: str) -> Any:
        """
        Retrieve the function associated with the given operator from the
        query_operators module.

        Args:
            op (str): The operator string, which should start with a '$' prefix.

        Returns:
            Any: The function corresponding to the operator.

        Raises:
            MalformedQueryException: If the operator does not start with '$'.
            MalformedQueryException: If the operator is not currently implemented.
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
