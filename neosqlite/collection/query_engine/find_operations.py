"""Find operations for the QueryEngine."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..client_session import ClientSession

from ...sql_utils import quote_table_name
from ..cursor import Cursor
from ..json_helpers import neosqlite_json_dumps

# Import feature detection
from ..query_helper import (
    _convert_bytes_to_binary,
    _supports_returning_clause,
    get_force_fallback,
)
from ..query_helper._sql_updates import _extract_preserved_json_expr
from ..query_helper.utils import build_upsert_base_document
from ..raw_batch_cursor import RawBatchCursor
from ..type_utils import validate_session
from .base import QueryEngineProtocol


class FindOperationsMixin(QueryEngineProtocol):
    """Mixin class providing find and find-and-modify operations.

    Implements ``find``, ``find_raw_batches``, ``find_one``,
    ``find_one_and_delete``, ``find_one_and_replace``, and
    ``find_one_and_update`` for ``QueryEngine`` with session validation, ID
    query normalization, SQLite ``RETURNING`` optimization when supported,
    and tiered SQL/Python fallback execution.
    """

    def find(
        self,
        filter: dict[str, Any] | None = None,
        projection: dict[str, Any] | None = None,
        hint: str | None = None,
        session: ClientSession | None = None,
    ) -> Cursor:
        """Query the database and retrieve documents matching the filter.

        Validates the optional session, normalizes ``id``/``ObjectId`` query
        predicates, and constructs a lazy ``Cursor`` over matching documents.

        Args:
            filter: Optional query criteria dictionary.
            projection: Optional field projection specification.
            hint: Optional index hint string.
            session: Optional ``ClientSession`` for transaction validation.

        Returns:
            Cursor: Cursor object iterating over matching documents.
        """
        validate_session(session, self.collection._database)
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        if filter is not None:
            filter = self.helpers._normalize_id_query(filter)
        return Cursor(
            self.collection, filter, projection, hint, session=session
        )

    def find_raw_batches(
        self,
        filter: dict[str, Any] | None = None,
        projection: dict[str, Any] | None = None,
        hint: str | None = None,
        batch_size: int = 100,
        session: ClientSession | None = None,
    ) -> RawBatchCursor:
        """Query the database and retrieve batches of raw JSON.

        Similar to :meth:`find` but returns a
        :class:`~neosqlite.raw_batch_cursor.RawBatchCursor` yielding raw JSON
        batches instead of decoded individual documents.

        Args:
            filter: Optional query criteria dictionary.
            projection: Optional field projection specification.
            hint: Optional index hint string.
            batch_size: Number of documents to include in each raw batch.
            session: Optional ``ClientSession`` for transaction validation.

        Returns:
            RawBatchCursor: Cursor iterating over raw JSON batches.
        """
        validate_session(session, self.collection._database)
        return RawBatchCursor(
            self.collection,
            filter,
            projection,
            hint,
            batch_size,
            session=session,
        )

    def find_one(
        self,
        filter: dict[str, Any] | None = None,
        projection: dict[str, Any] | None = None,
        hint: str | None = None,
        session: ClientSession | None = None,
    ) -> dict[str, Any] | None:
        """Find a single document matching the filter.

        Validates the optional session, normalizes ``id``/``ObjectId`` query
        predicates, and executes ``find(...).limit(1)``.

        Args:
            filter: Optional query criteria dictionary.
            projection: Optional field projection specification.
            hint: Optional index hint string.
            session: Optional ``ClientSession`` for transaction validation.

        Returns:
            dict[str, Any] | None: The first matching document, or ``None`` if
            no document matches.
        """
        validate_session(session, self.collection._database)
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        if filter is not None:
            filter = self.helpers._normalize_id_query(filter)
        try:
            return next(
                iter(
                    self.find(filter, projection, hint, session=session).limit(
                        1
                    )
                )
            )
        except StopIteration:
            return None

    def _prepare_find_and_modify(
        self, filter: dict[str, Any], sort: Any
    ) -> tuple[bool, bool, str, str | None, list[Any]]:
        """Prepare fallback flags, RETURNING toggle, sort, and WHERE SQL.

        Args:
            filter: Normalized MongoDB-style query filter dictionary.
            sort: Optional sequence of ``(field, direction)`` sort pairs.

        Returns:
            tuple[bool, bool, str, str | None, list[Any]]: Tuple containing
            ``force_fallback``, ``use_returning``, ``order_by``,
            ``where_clause``, and ``params``.
        """
        force_fallback = get_force_fallback()
        use_returning = _supports_returning_clause() and not force_fallback
        order_by = (
            self.helpers._build_sort_clause(dict(sort))
            if sort and not force_fallback
            else ""
        )
        where_clause, params = (
            (None, [])
            if force_fallback
            else self.sql_translator.translate_match(filter)
        )
        return force_fallback, use_returning, order_by, where_clause, params

    def find_one_and_delete(
        self,
        filter: dict[str, Any],
        projection: dict[str, Any] | None = None,
        sort: list[tuple[str, int]] | None = None,
        session: ClientSession | None = None,
        **kwargs: Any,
    ) -> dict[str, Any] | None:
        """Find a single document matching the filter and delete it.

        Uses a single atomic ``DELETE ... RETURNING`` statement when SQLite
        supports ``RETURNING`` and the filter translates to Tier-1 SQL;
        otherwise uses two-step ``SELECT`` + delete or Tier-3 Python fallback.

        Args:
            filter: Dictionary specifying the filter criteria.
            projection: Optional dictionary specifying fields to return.
            sort: Optional list of ``(key, direction)`` pairs for sorting.
            session: Optional ``ClientSession`` for transaction validation.
            **kwargs: Additional ignored compatibility keyword arguments.

        Returns:
            dict[str, Any] | None: The document before deletion (with
            projection applied if provided), or ``None`` if not found.
        """
        validate_session(session, self.collection._database)
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        filter = self.helpers._normalize_id_query(filter)

        # Prepare Tier-1 execution flags and SQL clauses
        (
            force_fallback,
            use_returning,
            order_by,
            where_clause,
            params,
        ) = self._prepare_find_and_modify(filter, sort)
        if force_fallback or where_clause is None:
            self._notify_tier_change("tier3", [filter])
            # Fallback: use Python-based approach for complex queries
            cursor = self.find(filter, session=session)
            if sort:
                cursor.sort(sort)
            try:
                doc = next(iter(cursor.limit(1)))
                if doc:
                    int_doc_id = self._get_integer_id_for_oid(doc["_id"])
                    self.helpers._internal_delete(int_doc_id)
                    if projection is not None:
                        doc = self.helpers._apply_projection(projection, doc)
                    return doc
            except StopIteration:
                pass
            return None

        if use_returning:
            # Tier-1: Use RETURNING clause for atomic find-and-delete
            # Note: SQLite doesn't support LIMIT with DELETE RETURNING directly
            # We need to use a subquery approach
            data_col = self.jsonb.data_column()
            cmd = (
                f"DELETE FROM {quote_table_name(self.collection.name)} "
                f"WHERE id = (SELECT id FROM {quote_table_name(self.collection.name)} "
                f"{where_clause} {order_by} LIMIT 1) "
                f"RETURNING id, _id, {data_col} as data"
            )
            cursor = self.collection.db.execute(cmd, params)
            if row := cursor.fetchone():
                int_id, stored_id, data = row
                doc = self.collection._load_with_stored_id(
                    int_id, data, stored_id
                )
                if projection is not None:
                    doc = self.helpers._apply_projection(projection, doc)
                return doc
            return None
        else:
            # Tier-1 (Fallback): Two-step process (SELECT then DELETE)
            # Used when RETURNING clause is not supported (SQLite < 3.35.0)
            cmd = (
                f"SELECT id, _id, {self.jsonb.data_column()} as data "
                f"FROM {quote_table_name(self.collection.name)} "
                f"{where_clause} {order_by} LIMIT 1"
            )
            cursor = self.collection.db.execute(cmd, params)
            if row := cursor.fetchone():
                int_id, stored_id, data = row
                doc = self.collection._load_with_stored_id(
                    int_id, data, stored_id
                )
                self.helpers._internal_delete(int_id)
                if projection is not None:
                    doc = self.helpers._apply_projection(projection, doc)
                return doc
            return None

    def find_one_and_replace(
        self,
        filter: dict[str, Any],
        replacement: dict[str, Any],
        projection: dict[str, Any] | None = None,
        sort: list[tuple[str, int]] | None = None,
        upsert: bool = False,
        return_document: bool = False,
        session: ClientSession | None = None,
        **kwargs: Any,
    ) -> dict[str, Any] | None:
        """Find a single document matching the filter and replace it.

        Uses atomic ``UPDATE ... RETURNING`` when ``use_returning`` and
        ``return_document`` are enabled, preserving any existing ``_id`` if
        omitted from ``replacement``. Otherwise performs two-step SQL
        replacement or Tier-3 Python fallback, with optional upsert insertion.

        Args:
            filter: Dictionary specifying the filter criteria.
            replacement: Replacement document dictionary.
            projection: Optional dictionary specifying fields to return.
            sort: Optional list of ``(key, direction)`` pairs for sorting.
            upsert: If True, insert ``replacement`` when no document matches.
            return_document: If True, return the replaced/upserted document
                instead of the original document (or ``None`` on upsert).
            session: Optional ``ClientSession`` for transaction validation.
            **kwargs: Additional ignored compatibility keyword arguments.

        Returns:
            dict[str, Any] | None: Document before or after replacement
            depending on ``return_document``, or ``None`` if no match (or on
            upsert when ``return_document`` is False).
        """
        validate_session(session, self.collection._database)
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        filter = self.helpers._normalize_id_query(filter)

        # Prepare Tier-1 execution flags and SQL clauses
        (
            force_fallback,
            use_returning,
            order_by,
            where_clause,
            params,
        ) = self._prepare_find_and_modify(filter, sort)
        if force_fallback or where_clause is None:
            self._notify_tier_change("tier3", [filter, replacement])
            # Fallback: use Python-based approach for complex queries
            cursor = self.find(filter, session=session)
            if sort:
                cursor.sort(sort)
            try:
                doc = next(iter(cursor.limit(1)))
                if doc:
                    int_doc_id = self._get_integer_id_for_oid(doc["_id"])
                    self.helpers._internal_replace(int_doc_id, replacement)
                    if return_document:
                        return self.find_one(
                            {"_id": doc["_id"]}, projection, session=session
                        )
                    if projection is not None:
                        doc = self.helpers._apply_projection(projection, doc)
                    return doc
            except StopIteration:
                pass

            if upsert:
                res = self.insert_one(replacement, session=session)
                if return_document:
                    return self.find_one(
                        {"_id": res.inserted_id}, projection, session=session
                    )
                return None
            return None

        if use_returning:
            # Tier-1: Use RETURNING clause for atomic find-and-replace
            data_col = self.jsonb.data_column()
            tbl = quote_table_name(self.collection.name)
            if return_document:
                replacement_copy = _convert_bytes_to_binary(dict(replacement))
                repl_json = neosqlite_json_dumps(replacement_copy)
                json_val_expr = self.jsonb.json_placeholder(wrap_text=True)
                if "_id" in replacement_copy:
                    set_data_sql = json_val_expr
                else:
                    set_func = self.jsonb.function("set")
                    extract_func = self.jsonb.function("extract")
                    preserved_id = _extract_preserved_json_expr(
                        extract_func, "data", "'$._id'"
                    )
                    set_data_sql = (
                        f"{set_func}({json_val_expr}, '$._id', "
                        f"CASE WHEN json_type(data, '$._id') IS NOT NULL "
                        f"THEN {preserved_id} ELSE _id END)"
                    )
                update_cmd = (
                    f"UPDATE {tbl} SET data = {set_data_sql} "
                    f"WHERE id = (SELECT id FROM {tbl} {where_clause} {order_by} LIMIT 1) "
                    f"RETURNING id, _id, {data_col} as data"
                )
                update_row = self.collection.db.execute(
                    update_cmd, [repl_json] + list(params)
                ).fetchone()
                if update_row:
                    self._notify_tier_change("tier1", [filter, replacement])
                    doc = self.collection._load_with_stored_id(
                        update_row[0], update_row[2], update_row[1]
                    )
                    if projection is not None:
                        doc = self.helpers._apply_projection(projection, doc)
                    return doc
            else:
                cmd = (
                    f"SELECT id, _id, {data_col} as data "
                    f"FROM {tbl} {where_clause} {order_by} LIMIT 1"
                )
                if row := self.collection.db.execute(cmd, params).fetchone():
                    int_id, stored_id, data = row
                    original_doc = self.collection._load_with_stored_id(
                        int_id, data, stored_id
                    )
                    self.helpers._internal_replace(int_id, replacement)
                    self._notify_tier_change("tier1", [filter, replacement])
                    if projection is not None:
                        original_doc = self.helpers._apply_projection(
                            projection, original_doc
                        )
                    return original_doc
            self._notify_tier_change("tier1", [filter, replacement])
            # No document found, handle upsert
            if upsert:
                res = self.insert_one(replacement, session=session)
                if return_document:
                    return self.find_one(
                        {"_id": res.inserted_id}, projection, session=session
                    )
                return None
            return None
        else:
            # Tier-1 (Fallback): Two-step process
            # Used when RETURNING clause is not supported (SQLite < 3.35.0)
            cmd = (
                f"SELECT id, _id, {self.jsonb.data_column()} as data "
                f"FROM {quote_table_name(self.collection.name)} "
                f"{where_clause} {order_by} LIMIT 1"
            )
            cursor = self.collection.db.execute(cmd, params)
            if row := cursor.fetchone():
                int_id, stored_id, data = row
                original_doc = self.collection._load_with_stored_id(
                    int_id, data, stored_id
                )
                self.helpers._internal_replace(int_id, replacement)
                if return_document:
                    return self.find_one(
                        {"_id": original_doc["_id"]},
                        projection,
                        session=session,
                    )
                if projection is not None:
                    original_doc = self.helpers._apply_projection(
                        projection, original_doc
                    )
                return original_doc

            if upsert:
                res = self.insert_one(replacement, session=session)
                if return_document:
                    return self.find_one(
                        {"_id": res.inserted_id}, projection, session=session
                    )
                return None

            return None

    def find_one_and_update(
        self,
        filter: dict[str, Any],
        update: dict[str, Any],
        projection: dict[str, Any] | None = None,
        sort: list[tuple[str, int]] | None = None,
        upsert: bool = False,
        return_document: bool = False,
        array_filters: list[dict[str, Any]] | None = None,
        session: ClientSession | None = None,
        **kwargs: Any,
    ) -> dict[str, Any] | None:
        """Find a single document matching the filter and update it.

        Executes an atomic ``UPDATE ... RETURNING`` statement when SQLite
        supports ``RETURNING``, ``return_document`` is True, and the update
        translates cleanly to SQL; otherwise runs a two-step update or Tier-3
        Python fallback, delegating unmatched upserts to
        ``_upsert_and_return``.

        Args:
            filter: Dictionary specifying the filter criteria.
            update: Dictionary specifying the update operators.
            projection: Optional dictionary specifying fields to return.
            sort: Optional list of ``(key, direction)`` pairs for sorting.
            upsert: If True, insert a new document when no document matches.
            return_document: If True, return the updated/inserted document
                instead of the original document.
            array_filters: Optional filter documents for filtered array
                updates.
            session: Optional ``ClientSession`` for transaction validation.
            **kwargs: Additional ignored compatibility keyword arguments.

        Returns:
            dict[str, Any] | None: Original document before update (or updated
            document when ``return_document`` is True), or ``None`` if no
            document matched (or on upsert with ``return_document`` False).
        """
        validate_session(session, self.collection._database)
        # Apply ID type normalization to handle cases where users query 'id' with ObjectId
        filter = self.helpers._normalize_id_query(filter)

        # Prepare Tier-1 execution flags and SQL clauses
        (
            force_fallback,
            use_returning,
            order_by,
            where_clause,
            params,
        ) = self._prepare_find_and_modify(filter, sort)
        if force_fallback or where_clause is None:
            self._notify_tier_change("tier3", [filter, update])
            # Fallback: use Python-based approach for complex queries
            cursor = self.find(filter, session=session)
            if sort:
                cursor.sort(sort)
            try:
                doc = next(iter(cursor.limit(1)))
                if doc:
                    int_doc_id = self._get_integer_id_for_oid(doc["_id"])
                    # Load the document for the update processing
                    doc_to_update = self.find_one(
                        {"_id": doc["_id"]}, session=session
                    )
                    if doc_to_update is not None:
                        self.helpers._internal_update(
                            int_doc_id,
                            update,
                            doc_to_update,
                            array_filters,
                            filter,
                        )
                    if return_document:
                        return self.find_one(
                            {"_id": doc["_id"]}, projection, session=session
                        )
                    if projection is not None:
                        doc = self.helpers._apply_projection(projection, doc)
                    return doc
            except StopIteration:
                pass

            if upsert:
                return self._upsert_and_return(
                    filter,
                    update,
                    projection,
                    return_document,
                    array_filters,
                    session,
                )
            return None

        if use_returning:
            # Atomic single-statement RETURNING when returning AFTER doc and SQL-capable
            tbl = quote_table_name(self.collection.name)
            data_col = self.jsonb.data_column()
            sql_expr_res = self.helpers._build_set_data_expression(
                update, array_filters
            )
            inc_mul_ok = (
                sql_expr_res is not None
                and self.helpers._validate_inc_mul_sql(
                    where_clause,
                    params,
                    update,
                    order_by=order_by,
                    limit_one=True,
                )
            )
            if return_document and sql_expr_res is not None and inc_mul_ok:
                data_expr, expr_params, op_tier = sql_expr_res
                if data_expr == "data":
                    row = self.collection.db.execute(
                        f"SELECT id, _id, {data_col} as data "
                        f"FROM {tbl} {where_clause} {order_by} LIMIT 1",
                        params,
                    ).fetchone()
                else:
                    row = self.collection.db.execute(
                        f"UPDATE {tbl} SET data = {data_expr} "
                        f"WHERE id = (SELECT id FROM {tbl} {where_clause} {order_by} LIMIT 1) "
                        f"RETURNING id, _id, {data_col} as data",
                        expr_params + list(params),
                    ).fetchone()
                self._notify_tier_change(op_tier, [filter, update])
                if row:
                    doc = self.collection._load_with_stored_id(
                        row[0], row[2], row[1]
                    )
                    if projection is not None:
                        doc = self.helpers._apply_projection(projection, doc)
                    return doc
                if upsert:
                    return self._upsert_and_return(
                        filter,
                        update,
                        projection,
                        return_document,
                        array_filters,
                        session,
                    )
                return None

            cmd = (
                f"SELECT id, _id, {data_col} as data "
                f"FROM {tbl} {where_clause} {order_by} LIMIT 1"
            )
            cursor = self.collection.db.execute(cmd, params)
            op_tier = (
                sql_expr_res[2]
                if (sql_expr_res is not None and inc_mul_ok)
                else "tier3"
            )
            self._notify_tier_change(op_tier, [filter, update])
            if row := cursor.fetchone():
                int_id, stored_id, data = row
                original_doc = self.collection._load_with_stored_id(
                    int_id, data, stored_id
                )
                self.helpers._internal_update(
                    int_id, update, original_doc, array_filters, filter
                )
                if return_document:
                    return self.find_one(
                        {"_id": original_doc["_id"]},
                        projection,
                        session=session,
                    )
                if projection is not None:
                    original_doc = self.helpers._apply_projection(
                        projection, original_doc
                    )
                return original_doc
            if upsert:
                return self._upsert_and_return(
                    filter,
                    update,
                    projection,
                    return_document,
                    array_filters,
                    session,
                )
            return None
        else:
            # Fallback: Two-step process when RETURNING is not supported
            cmd = (
                f"SELECT id, _id, {self.jsonb.data_column()} as data "
                f"FROM {quote_table_name(self.collection.name)} "
                f"{where_clause} {order_by} LIMIT 1"
            )
            cursor = self.collection.db.execute(cmd, params)
            if row := cursor.fetchone():
                int_id, stored_id, data = row
                original_doc = self.collection._load_with_stored_id(
                    int_id, data, stored_id
                )
                self.helpers._internal_update(
                    int_id, update, original_doc, array_filters, filter
                )
                if return_document:
                    return self.find_one(
                        {"_id": original_doc["_id"]},
                        projection,
                        session=session,
                    )
                if projection is not None:
                    original_doc = self.helpers._apply_projection(
                        projection, original_doc
                    )
                return original_doc

            if upsert:
                return self._upsert_and_return(
                    filter,
                    update,
                    projection,
                    return_document,
                    array_filters,
                    session,
                )

            return None

    def _upsert_and_return(
        self,
        filter: dict[str, Any],
        update: dict[str, Any],
        projection: dict[str, Any] | None,
        return_document: bool,
        array_filters: list[dict[str, Any]] | None,
        session: ClientSession | None,
    ) -> dict[str, Any] | None:
        """Build, update, insert, and optionally return an upserted document.

        Extracts equality fields from ``filter`` via
        ``build_upsert_base_document``, applies ``update`` (including
        ``$setOnInsert``), inserts the resulting document, and fetches it when
        ``return_document`` is True.

        Args:
            filter: Normalized filter used to seed equality fields.
            update: Update operations applied to the new document.
            projection: Optional projection applied when returning the doc.
            return_document: If True, query and return the inserted document.
            array_filters: Optional array filters for ``_internal_update``.
            session: Optional ``ClientSession`` for transaction validation.

        Returns:
            dict[str, Any] | None: Inserted document if ``return_document`` is
            True, otherwise ``None``.
        """
        new_doc = build_upsert_base_document(filter)
        updated_doc, _ = self.helpers._internal_update(
            0, update, new_doc, array_filters, filter
        )
        res = self.insert_one(updated_doc, session=session)
        if return_document:
            return self.find_one(
                {"_id": res.inserted_id}, projection, session=session
            )
        return None
