"""Find operations for the QueryEngine."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..client_session import ClientSession

from ...sql_utils import quote_table_name
from ..cursor import Cursor
from ..json_helpers import neosqlite_json_dumps
from ..jsonb_support import json_data_column

# Import feature detection
from ..query_helper import (
    _convert_bytes_to_binary,
    _get_json_function,
    _supports_returning_clause,
    get_force_fallback,
)
from ..query_helper._sql_updates import _extract_preserved_json_expr
from ..query_helper.utils import build_upsert_base_document
from ..raw_batch_cursor import RawBatchCursor
from ..type_utils import validate_session
from .base import QueryEngineProtocol


class FindOperationsMixin(QueryEngineProtocol):
    """Mixin class providing find operations for QueryEngine."""

    def find(
        self,
        filter: dict[str, Any] | None = None,
        projection: dict[str, Any] | None = None,
        hint: str | None = None,
        session: ClientSession | None = None,
    ) -> Cursor:
        """
        Query the database and retrieve documents matching the provided filter.

        Args:
            filter (dict[str, Any] | None): A dictionary specifying the query criteria.
            projection (dict[str, Any] | None): A dictionary specifying which fields to return.
            hint (str | None): A string specifying the index to use.
            session (ClientSession, optional): A ClientSession for transactions.

        Returns:
            Cursor: A cursor object to iterate over the results.
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
        """
        Query the database and retrieve batches of raw JSON.

        Similar to the :meth:`find` method but returns a
        :class:`~neosqlite.raw_batch_cursor.RawBatchCursor`.

        This method returns raw JSON batches which can be more efficient for
        certain use cases where you want to process data in batches rather than
        individual documents.

        Args:
            filter (dict[str, Any] | None): A dictionary specifying the query criteria.
            projection (dict[str, Any] | None): A dictionary specifying which fields to return.
            hint (str | None): A string specifying the index to use.
            batch_size (int): The number of documents to include in each batch.
            session (ClientSession, optional): A ClientSession for transactions.

        Returns:
            RawBatchCursor instance.
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
        """
        Find a single document matching the filter.

        Args:
            filter (dict[str, Any]): A dictionary specifying the filter conditions.
            projection (dict[str, Any]): A dictionary specifying which fields to return.
            hint (str): A string specifying the index to use (not used in SQLite).
            session (ClientSession, optional): A ClientSession for transactions.

        Returns:
            dict[str, Any]: A dictionary representing the found document,
                            or None if no document matches.
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
        """Prepare kill-switch flag, RETURNING toggle, SQL sort, and WHERE clause."""
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
        """
        Find a single document and delete it.

        Args:
            filter (dict[str, Any]): A dictionary specifying the filter criteria.
            projection (dict[str, Any]): A dictionary specifying which fields to return.
            sort (list[tuple[str, int]]): A list of (key, direction) pairs for sorting.
            session (ClientSession, optional): A ClientSession for transactions.
            **kwargs: Additional keyword arguments.

        Returns:
            dict[str, Any] | None: The document before it was deleted,
                                 or None if not found.
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
            jsonb = self.jsonb.jsonb_supported
            data_col = json_data_column(jsonb)
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
            jsonb = self.jsonb.jsonb_supported
            cmd = (
                f"SELECT id, _id, {json_data_column(jsonb)} as data "
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
        """
        Find a single document and replace it.

        Args:
            filter (dict[str, Any]): A dictionary specifying the filter criteria.
            replacement (dict[str, Any]): The replacement document.
            projection (dict[str, Any]): A dictionary specifying which fields to return.
            sort (list[tuple[str, int]]): A list of (key, direction) pairs for sorting.
            upsert (bool): If True, perform an upsert if no document matches.
            return_document (bool): If True, return the updated document.
            session (ClientSession, optional): A ClientSession for transactions.
            **kwargs: Additional keyword arguments.

        Returns:
            dict[str, Any] | None: The document before or after replacement,
                                   or None if not found.
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
            jsonb = self.jsonb.jsonb_supported
            tbl = quote_table_name(self.collection.name)
            data_col = json_data_column(jsonb)
            if return_document:
                replacement_copy = _convert_bytes_to_binary(dict(replacement))
                repl_json = neosqlite_json_dumps(replacement_copy)
                json_val_expr = "jsonb(?)" if jsonb else "json(?)"
                if "_id" in replacement_copy:
                    set_data_sql = json_val_expr
                else:
                    set_func = _get_json_function("set", jsonb)
                    extract_func = _get_json_function("extract", jsonb)
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
            jsonb = self.jsonb.jsonb_supported
            cmd = (
                f"SELECT id, _id, {json_data_column(jsonb)} as data "
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
        """
        Find and update a single document.

        Args:
            filter (dict[str, Any]): A dictionary specifying the filter criteria.
            update (dict[str, Any]): A dictionary specifying the update operations.
            projection (dict[str, Any]): A dictionary specifying which fields to return.
            sort (list[tuple[str, int]]): A list of (key, direction) pairs for sorting.
            upsert (bool): If True, perform an upsert if no document matches.
            return_document (bool): If True, return the updated document.
            array_filters (list[dict[str, Any]]): Filters for array updates.
            session (ClientSession, optional): A ClientSession for transactions.
            **kwargs: Additional keyword arguments.

        Returns:
            dict[str, Any] | None: The original document (before update),
                                   or None if no document was found and updated.
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
            jsonb = self.jsonb.jsonb_supported
            tbl = quote_table_name(self.collection.name)
            data_col = json_data_column(jsonb)
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
            jsonb = self.jsonb.jsonb_supported
            cmd = (
                f"SELECT id, _id, {json_data_column(jsonb)} as data "
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
        """Apply update (including $setOnInsert) to upsert doc and optionally return it."""
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
