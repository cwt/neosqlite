from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable
from typing import Any

from ..._sqlite import sqlite3
from ...sql_utils import quote_table_name
from ..json_path_utils import parse_json_path
from .operators_base import OperatorsBaseMixin
from .operators_hybrid import python_hybrid_fallback
from .utils import (
    _json_extract_field_with_objectid_support,
    _sanitize_params,
)

logger = logging.getLogger(__name__)

HASH_JOIN_MEMORY_THRESHOLD = 100 * 1024 * 1024  # 100 MB default threshold


def _serialize_lookup_key(key_val: Any) -> str | None:
    """Serialize a lookup join key value to a canonical text string.

    Preserves ``None`` as SQL ``NULL``, encodes ``dict`` and ``list`` values
    as compact JSON strings, and coerces other scalar values via ``str()``.

    Args:
        key_val: Extracted join key value from a document.

    Returns:
        str | None: Serialized text key for indexing, or ``None``.
    """
    if key_val is None:
        return None
    if isinstance(key_val, (dict, list)):
        return json.dumps(key_val, separators=(",", ":"))
    return str(key_val)


class OperatorsLookupMixin(OperatorsBaseMixin):
    """Mixin implementing ``$lookup`` aggregation stage evaluation.

    Supports both an indexed temporary hash-join strategy (for small foreign
    collections or pipeline-based lookups) and a correlated SQL subquery
    fallback (for large foreign collections), with ObjectId-aware key
    extraction and row-by-row fallback for malformed JSON rows.
    """

    def _create_lookup_hash_table(
        self,
        from_collection: str,
        foreign_field: str | None,
        pipeline: list[dict[str, Any]] | None = None,
    ) -> tuple[str, str]:
        """Create an indexed temporary hash table for a foreign collection.

        Builds an indexed temp table enabling ``O(n + m)`` hash joins instead
        of ``O(n * m)`` correlated subqueries. When ``pipeline`` is provided,
        runs it through ``TemporaryTableAggregationProcessor`` first; otherwise
        reads directly from ``from_collection``, falling back to row-by-row
        parsing on malformed JSON ``OperationalError``. Drops the temp table
        and re-raises if creation fails.

        Args:
            from_collection: Name of the foreign collection to materialize.
            foreign_field: Foreign field path used as join key (defaults to
                ``"_id"`` when ``None``).
            pipeline: Optional sub-pipeline to execute on ``from_collection``
                before indexing.

        Returns:
            tuple[str, str]: Pair of ``(hash_table_name, join_key_column)``.
        """
        if foreign_field is None:
            foreign_field = "_id"
        stage_key = f"{from_collection}:{foreign_field}:{str(pipeline) if pipeline else ''}"
        hash_suffix = hashlib.sha256(stage_key.encode()).hexdigest()[:8]
        hash_table_name = f"_lookup_hash_{hash_suffix}"
        join_key = "_join_key"

        try:
            if pipeline:
                target_coll = self.collection.database.get_collection(
                    from_collection
                )
                from . import TemporaryTableAggregationProcessor

                processor = TemporaryTableAggregationProcessor(
                    target_coll, None
                )
                pipeline_result = processor.process_pipeline(pipeline)

                if not pipeline_result:
                    self.db.execute(
                        f"CREATE TEMP TABLE {hash_table_name} (id INTEGER PRIMARY KEY, _id BLOB, data TEXT, {join_key} TEXT)"
                    )
                else:
                    from ..json_helpers import neosqlite_json_dumps

                    self.db.execute(
                        f"CREATE TEMP TABLE {hash_table_name} (id INTEGER PRIMARY KEY, _id BLOB, data TEXT, {join_key} TEXT)"
                    )

                    row_number = 0
                    if foreign_field == "_id":
                        for doc in pipeline_result:
                            raw_id = (
                                _sanitize_params([doc.get("_id")]) or [None]
                            )[0]
                            self.db.execute(
                                f"INSERT INTO {hash_table_name} (id, _id, data, {join_key}) VALUES (?, ?, ?, ?)",
                                (
                                    (row_number := row_number + 1),
                                    raw_id,
                                    neosqlite_json_dumps(doc),
                                    _serialize_lookup_key(doc.get("_id")),
                                ),
                            )
                    else:
                        for doc in pipeline_result:
                            raw_id = (
                                _sanitize_params([doc.get("_id")]) or [None]
                            )[0]
                            key_val = self._extract_field_value(
                                doc, foreign_field
                            )
                            self.db.execute(
                                f"INSERT INTO {hash_table_name} (id, _id, data, {join_key}) VALUES (?, ?, ?, ?)",
                                (
                                    (row_number := row_number + 1),
                                    raw_id,
                                    neosqlite_json_dumps(doc),
                                    _serialize_lookup_key(key_val),
                                ),
                            )
            else:
                if foreign_field == "_id":
                    self.db.execute(
                        f"CREATE TEMP TABLE {hash_table_name} AS "
                        f"SELECT id, _id, data, CAST(_id AS TEXT) as {join_key} "
                        f"FROM {quote_table_name(from_collection)}"
                    )
                else:
                    # Use ObjectId-aware extraction for the foreign field
                    foreign_extract_expr = (
                        _json_extract_field_with_objectid_support(
                            self.jsonb.json_function_prefix,
                            foreign_field,
                            is_local_field=False,
                        )
                    )
                    # Try efficient SQL approach first
                    try:
                        self.db.execute(
                            f"CREATE TEMP TABLE {hash_table_name} AS "
                            f"SELECT id, _id, data, {foreign_extract_expr} as {join_key} "
                            f"FROM {quote_table_name(from_collection)}"
                        )
                    except sqlite3.OperationalError as e:
                        if (
                            "malformed JSON" in str(e)
                            or "json" in str(e).lower()
                        ):
                            # Fall back to Python processing to skip corrupted documents
                            logger.warning(
                                f"Hash table creation for '{from_collection}' encountered "
                                f"malformed JSON, falling back to row-by-row processing"
                            )
                            self._create_lookup_hash_table_fallback(
                                hash_table_name,
                                from_collection,
                                foreign_field,
                                join_key,
                            )
                        else:
                            raise

            self.db.execute(
                f"CREATE INDEX {hash_table_name}_idx ON {hash_table_name}({join_key})"
            )

            return hash_table_name, join_key

        except Exception as e:
            logger.debug(
                f"Failed to create hash table '{hash_table_name}': {e}"
            )
            self.db.execute(f"DROP TABLE IF EXISTS {hash_table_name}")
            raise

    def _create_lookup_hash_table_fallback(
        self,
        hash_table_name: str,
        from_collection: str,
        foreign_field: str,
        join_key: str,
    ) -> None:
        """Create a lookup hash table row by row, skipping corrupted JSON.

        Used when bulk SQL creation fails due to malformed JSON in ``data``.
        Parses each document individually via ``neosqlite_json_loads`` and
        skips rows raising ``UnicodeDecodeError``, ``ValueError``,
        ``TypeError``, or ``KeyError`` with a warning.

        Args:
            hash_table_name: Name of the temporary hash table to create.
            from_collection: Source foreign collection name.
            foreign_field: Foreign field path used as the join key.
            join_key: Name of the join key column in ``hash_table_name``.
        """
        from ..json_helpers import neosqlite_json_dumps, neosqlite_json_loads

        # Create the hash table structure
        self.db.execute(
            f"CREATE TEMP TABLE {hash_table_name} "
            f"(id INTEGER PRIMARY KEY, _id BLOB, data TEXT, {join_key} TEXT)"
        )

        # Read documents one by one
        cursor = self.db.execute(
            f"SELECT id, _id, data FROM {quote_table_name(from_collection)}"
        )

        inserted_count = 0
        skipped_count = 0

        for row in cursor.fetchall():
            doc_id, doc_underscore_id, doc_data = row

            try:
                # Parse the JSON data
                doc = neosqlite_json_loads(doc_data)

                # Extract the foreign field value
                if foreign_field == "_id":
                    key_val = _serialize_lookup_key(doc_underscore_id)
                else:
                    # Navigate nested field path
                    key_val = self._extract_field_value(doc, foreign_field)
                    key_val = _serialize_lookup_key(key_val)

                # Insert into hash table
                self.db.execute(
                    f"INSERT INTO {hash_table_name} "
                    f"(id, _id, data, {join_key}) VALUES (?, ?, ?, ?)",
                    (
                        doc_id,
                        doc_underscore_id,
                        neosqlite_json_dumps(doc),
                        key_val,
                    ),
                )
                inserted_count += 1

            except (UnicodeDecodeError, ValueError, TypeError, KeyError) as e:
                skipped_count += 1
                logger.warning(
                    f"Skipping corrupted document in $lookup hash table "
                    f"(collection='{from_collection}', id={doc_id}): {e}"
                )

        if skipped_count > 0:
            logger.warning(
                f"$lookup hash table creation skipped {skipped_count} "
                f"corrupted document(s) out of {inserted_count + skipped_count} "
                f"total from '{from_collection}'"
            )

    def _estimate_collection_size(self, collection_name: str) -> int:
        """Estimate the in-memory size of a collection in bytes.

        Queries row count and average ``LENGTH(data)``, adds 50 bytes per row
        for ``id`` and ``_id`` overhead, and returns ``0`` on empty or error.

        Args:
            collection_name: Name of the collection to inspect.

        Returns:
            int: Estimated collection size in bytes.
        """
        try:
            result = self.db.execute(
                f"SELECT COUNT(*), AVG(LENGTH(data)) FROM {quote_table_name(collection_name)}"
            ).fetchone()
            if result and result[0]:
                count, avg_size = result
                avg_size = avg_size or 0
                row_size = (
                    int(avg_size) + 50
                )  # Add overhead for id, _id columns
                return count * row_size
        except Exception as e:
            logger.debug(
                f"Failed to estimate collection size for '{collection_name}': {e}"
            )
            pass
        return 0

    def _get_available_memory(self) -> int:
        """Estimate available memory in bytes for hash join operations.

        Computes half of SQLite's configured page cache (``PRAGMA page_size``
        times ``abs(PRAGMA cache_size)``), falling back to 30% of
        ``RLIMIT_AS`` soft limit or ``HASH_JOIN_MEMORY_THRESHOLD``.

        Returns:
            int: Estimated available memory budget in bytes.
        """
        try:
            page_size = self.db.execute("PRAGMA page_size").fetchone()[0]
            cache_pages = self.db.execute("PRAGMA cache_size").fetchone()[0]
            if cache_pages < 0:
                cache_pages = -cache_pages
            sqlite_memory = page_size * cache_pages
            return int(sqlite_memory * 0.5)
        except Exception as e:
            logger.debug(f"{e=}")
            pass
        try:
            import resource

            getrlimit = getattr(resource, "getrlimit", None)
            rlimit_as = getattr(resource, "RLIMIT_AS", None)
            rlim_infinity = getattr(resource, "RLIM_INFINITY", None)
            if getrlimit is not None and rlimit_as is not None:
                soft, _hard = getrlimit(rlimit_as)
                if soft != rlim_infinity:
                    return int(soft * 0.3)
        except Exception as e:
            logger.debug(f"{e=}")
            pass
        return HASH_JOIN_MEMORY_THRESHOLD

    def _should_use_hash_join(
        self,
        from_collection: str,
        pipeline: list[dict[str, Any]] | None = None,
    ) -> bool:
        """Decide between hash join and correlated subquery for ``$lookup``.

        Always uses hash join when ``pipeline`` is provided. Otherwise compares
        the estimated foreign collection size against 30% of available memory,
        defaulting to ``True`` on estimation failure.

        Args:
            from_collection: Name of the foreign collection to join with.
            pipeline: Optional sub-pipeline to run on the foreign collection.

        Returns:
            bool: ``True`` to use hash join, ``False`` for correlated subquery.
        """
        if pipeline:
            return True
        try:
            est_size = self._estimate_collection_size(from_collection)
            available = self._get_available_memory()
            return est_size < (available * 0.3)
        except Exception as e:
            logger.debug(f"Failed during _should_use_hash_join check: {e}")
            return True

    def _extract_field_value(self, doc: dict[str, Any], field: str) -> Any:
        """Extract a nested field value from a document using dot notation.

        Traverses dictionary keys along ``field.split(".")``, returning
        ``None`` if any segment is missing or traverses a non-dict value.

        Args:
            doc: Source document dictionary.
            field: Field name or dot-separated nested path.

        Returns:
            Any: Extracted field value, or ``None`` if not found.
        """
        parts = field.split(".")
        val: Any = doc
        for part in parts:
            if isinstance(val, dict):
                val = val.get(part)
            else:
                return None
        return val

    @python_hybrid_fallback("$lookup")
    def _process_lookup_stage(
        self,
        create_temp: Callable,
        current_table: str,
        lookup_spec: dict[str, Any],
    ) -> str:
        """Process a ``$lookup`` aggregation stage into a new temp table.

        Selects between ``_process_lookup_hash_join`` (``O(n + m)`` via an
        indexed temporary hash table) and
        ``_process_lookup_correlated_subquery`` (``O(n * m)`` with lower
        memory usage) using ``_should_use_hash_join``.

        Args:
            create_temp: Callable that creates and populates the next temporary
                table from a stage specification and SQL query.
            current_table: Name of the current input temporary table.
            lookup_spec: ``$lookup`` stage configuration containing ``"from"``,
                ``"as"``, ``"localField"``, ``"foreignField"``, and optional
                ``"pipeline"``.

        Returns:
            str: Name of the newly created temporary table with joined arrays.
        """
        if lookup_spec.get("let") is not None:
            raise NotImplementedError(
                "$lookup with 'let' requires Python fallback"
            )

        from_collection = lookup_spec["from"]
        pipeline = lookup_spec.get("pipeline", [])
        local_field = lookup_spec.get("localField")

        use_hash_join = self._should_use_hash_join(from_collection, pipeline)
        if use_hash_join and local_field and local_field != "_id":
            local_path = parse_json_path(local_field)
            try:
                has_array_local = (
                    self.db.execute(
                        f"SELECT 1 FROM {current_table} "
                        f"WHERE json_type(json(data), '{local_path}') = 'array' LIMIT 1"
                    ).fetchone()
                    is not None
                )
                if has_array_local:
                    use_hash_join = False
            except sqlite3.OperationalError:
                pass

        if use_hash_join:
            return self._process_lookup_hash_join(
                create_temp, current_table, lookup_spec
            )
        else:
            return self._process_lookup_correlated_subquery(
                create_temp, current_table, lookup_spec
            )

    def _process_lookup_correlated_subquery(
        self,
        create_temp: Callable,
        current_table: str,
        lookup_spec: dict[str, Any],
    ) -> str:
        """Process ``$lookup`` via correlated subquery for low memory usage.

        Used as a fallback when the foreign collection is too large for an
        in-memory hash join. When ``pipeline`` is present, materializes its
        results into a temporary table that is dropped in a ``finally`` block.

        Args:
            create_temp: Callable that creates the next temporary table.
            current_table: Name of the current input temporary table.
            lookup_spec: ``$lookup`` stage configuration dictionary.

        Returns:
            str: Name of the newly created temporary table.

        Raises:
            NotImplementedError: If ``pipeline`` is given without both
                ``localField`` and ``foreignField``.
            ValueError: If ``from``, ``localField``, ``foreignField``, or
                ``as`` is missing in non-pipeline mode.
        """
        from_collection = lookup_spec["from"]
        local_field = lookup_spec.get("localField")
        foreign_field = lookup_spec.get("foreignField")
        as_field = lookup_spec["as"]
        pipeline = lookup_spec.get("pipeline", [])

        json_set_func = f"{self.jsonb.json_function_prefix}_set"

        if pipeline:
            if not local_field or not foreign_field:
                raise NotImplementedError(
                    "$lookup with pipeline requires localField and foreignField"
                )

            from . import (
                TemporaryTableAggregationProcessor,
            )

            target_coll = self.collection.database.get_collection(
                from_collection
            )
            processor = TemporaryTableAggregationProcessor(target_coll, None)

            pipeline_key = f"{from_collection}:{str(pipeline)}"
            pipeline_hash = hashlib.sha256(pipeline_key.encode()).hexdigest()[
                :8
            ]
            pipeline_result_table = f"_lookup_pipeline_{pipeline_hash}"
            try:
                pipeline_result = processor.process_pipeline(pipeline)
                if not pipeline_result:
                    self.collection.db.execute(
                        f"CREATE TEMP TABLE {pipeline_result_table} (id INTEGER PRIMARY KEY, _id INTEGER, data TEXT)"
                    )
                else:
                    from ..json_helpers import neosqlite_json_dumps

                    self.collection.db.execute(
                        f"CREATE TEMP TABLE {pipeline_result_table} (id INTEGER PRIMARY KEY, _id INTEGER, data TEXT)"
                    )
                    row_number = 0
                    for doc in pipeline_result:
                        raw_id = (_sanitize_params([doc.get("_id")]) or [None])[
                            0
                        ]
                        self.collection.db.execute(
                            f"INSERT INTO {pipeline_result_table} (id, _id, data) VALUES (?, ?, ?)",
                            (
                                (row_number := row_number + 1),
                                raw_id,
                                neosqlite_json_dumps(doc),
                            ),
                        )

                select_clause, from_clause = self._build_correlated_lookup_sql(
                    current_table,
                    pipeline_result_table,
                    local_field,
                    foreign_field,
                    as_field,
                    "json(related.data)",
                )

                lookup_stage = {"$lookup": lookup_spec}
                new_table = create_temp(
                    lookup_stage, f"{select_clause} {from_clause}"
                )
                return new_table
            finally:
                try:
                    self.collection.db.execute(
                        f"DROP TABLE IF EXISTS {pipeline_result_table}"
                    )
                except Exception as e:
                    logger.debug(
                        f"Failed to drop pipeline result table '{pipeline_result_table}': {e}"
                    )
                    pass

        if not all([from_collection, local_field, foreign_field, as_field]):
            raise ValueError(
                "$lookup requires from, localField, foreignField, and as"
            )

        assert local_field is not None
        assert foreign_field is not None
        select_clause, from_clause = self._build_correlated_lookup_sql(
            current_table,
            quote_table_name(from_collection),
            local_field,
            foreign_field,
            as_field,
            f"json({json_set_func}(related.data, '$._id', related._id))",
        )

        lookup_stage = {"$lookup": lookup_spec}
        new_table = create_temp(lookup_stage, f"{select_clause} {from_clause}")
        return new_table

    def _local_id_extract_expr(self) -> str:
        """Return a SQL TEXT expression extracting ``main_table._id``.

        Checks ``main_table._id``, falls back to ``$._id`` (unwrapping
        ``__neosqlite_objectid__`` markers when present), and finally falls
        back to ``main_table.id`` cast as ``TEXT``.

        Returns:
            str: SQL ``CAST(COALESCE(...) AS TEXT)`` expression.
        """
        prefix = self.jsonb.json_function_prefix
        return (
            f"CAST(COALESCE(main_table._id, "
            f"CASE WHEN {prefix}_extract(main_table.data, '$._id.__neosqlite_objectid__') = 1 "
            f"THEN {prefix}_extract(main_table.data, '$._id.id') "
            f"ELSE {prefix}_extract(main_table.data, '$._id') END, "
            f"main_table.id) AS TEXT)"
        )

    def _build_correlated_lookup_sql(
        self,
        current_table: str,
        related_table: str,
        local_field: str,
        foreign_field: str,
        as_field: str,
        related_doc_expr: str,
    ) -> tuple[str, str]:
        """Build SELECT and FROM SQL clauses for a correlated ``$lookup``.

        Handles ``_id`` and nested ObjectId-aware field extraction on both
        ``main_table`` and ``related``, aggregating matched rows with
        ``json_group_array`` / ``jsonb_group_array`` and defaulting empty
        matches to an empty JSON array.

        Args:
            current_table: Name of the left-hand temporary table.
            related_table: Quoted table name or temp table for foreign rows.
            local_field: Field path in ``current_table`` to match on.
            foreign_field: Field path in ``related_table`` to match on.
            as_field: Destination array field name written into ``data``.
            related_doc_expr: SQL expression yielding each matched foreign doc.

        Returns:
            tuple[str, str]: Pair of ``(select_clause, from_clause)``.
        """
        prefix = self.jsonb.json_function_prefix
        json_set_func = f"{prefix}_set"

        if foreign_field == "_id":
            foreign_extract = "CAST(related._id AS TEXT)"
        else:
            foreign_extract = _json_extract_field_with_objectid_support(
                prefix,
                foreign_field,
                is_local_field=False,
                data_column="related.data",
            )

        if local_field == "_id":
            local_extract = self._local_id_extract_expr()
            join_cond = f"{foreign_extract} = {local_extract}"
        else:
            local_extract = _json_extract_field_with_objectid_support(
                prefix,
                local_field,
                is_local_field=True,
                data_column="main_table.data",
            )
            local_path = parse_json_path(local_field)
            elem_expr = (
                "CASE "
                "WHEN _je.type = 'object' "
                "AND json_extract(_je.value, '$.__neosqlite_objectid__') = 1 "
                "THEN json_extract(_je.value, '$.id') "
                "ELSE CAST(_je.value AS TEXT) END"
            )
            join_cond = (
                f"("
                f"(json_type(json(main_table.data), '{local_path}') = 'array' AND EXISTS ("
                f"SELECT 1 FROM json_each(json(main_table.data), '{local_path}') AS _je "
                f"WHERE _je.value IS NOT NULL AND ({elem_expr}) = {foreign_extract}"
                f")) OR ("
                f"json_type(json(main_table.data), '{local_path}') IS NOT 'array' "
                f"AND {foreign_extract} = {local_extract}"
                f"))"
            )

        select_clause = (
            f"SELECT main_table.id, main_table._id, "
            f"json({json_set_func}(main_table.data, '{parse_json_path(as_field)}', "
            f"{prefix}(COALESCE(("
            f"  SELECT {self.jsonb.json_group_array_function}({related_doc_expr}) "
            f"  FROM {related_table} as related "
            f"  WHERE {join_cond}"
            f"), {prefix}('[]'))))) as data"
        )
        from_clause = f"FROM {current_table} as main_table"
        return select_clause, from_clause

    def _process_lookup_hash_join(
        self,
        create_temp: Callable,
        current_table: str,
        lookup_spec: dict[str, Any],
    ) -> str:
        """Process ``$lookup`` using an indexed temporary hash table join.

        Creates the indexed foreign hash table via
        ``_create_lookup_hash_table``, performs a ``LEFT JOIN`` grouped by
        ``_join_key`` using ``json_group_array_function``, writes the result
        via ``create_temp``, and drops the hash table in ``finally``.

        Args:
            create_temp: Callable that creates the next temporary table.
            current_table: Name of the current input temporary table.
            lookup_spec: ``$lookup`` stage configuration dictionary.

        Returns:
            str: Name of the newly created temporary table.

        Raises:
            NotImplementedError: If ``pipeline`` is given without both
                ``localField`` and ``foreignField``.
            ValueError: If ``from``, ``localField``, ``foreignField``, or
                ``as`` is missing in non-pipeline mode.
        """
        from_collection = lookup_spec["from"]
        local_field = lookup_spec.get("localField")
        foreign_field = lookup_spec.get("foreignField")
        as_field = lookup_spec["as"]
        pipeline = lookup_spec.get("pipeline", [])

        json_set_func = f"{self.jsonb.json_function_prefix}_set"

        if pipeline:
            if not local_field or not foreign_field:
                raise NotImplementedError(
                    "$lookup with pipeline requires localField and foreignField"
                )

            hash_table_name, join_key = self._create_lookup_hash_table(
                from_collection, foreign_field, pipeline
            )
        else:
            if not all([from_collection, local_field, foreign_field, as_field]):
                raise ValueError(
                    "$lookup requires from, localField, foreignField, and as"
                )
            hash_table_name, join_key = self._create_lookup_hash_table(
                from_collection, foreign_field, None
            )

        try:
            # Use ObjectId-aware extraction for local and foreign fields
            # At this point, local_field is guaranteed to be str (validated above)
            assert local_field is not None, "local_field should not be None"

            if local_field == "_id":
                local_extract = self._local_id_extract_expr()
            else:
                local_extract = _json_extract_field_with_objectid_support(
                    self.jsonb.json_function_prefix,
                    local_field,
                    is_local_field=True,
                )

            select_clause = (
                f"SELECT main_table.id, main_table._id, "
                f"json({json_set_func}(main_table.data, '{parse_json_path(as_field)}', "
                f"json(COALESCE(aggregated.results, json('[]'))))) as data "
            )

            from_clause = (
                f"FROM {current_table} as main_table "
                f"LEFT JOIN ("
                f"  SELECT {join_key}, {self.jsonb.json_group_array_function}(json({json_set_func}(data, '$._id', _id))) as results "
                f"  FROM {hash_table_name} "
                f"  GROUP BY {join_key}"
                f") aggregated ON {local_extract} = aggregated.{join_key}"
            )

            lookup_stage = {"$lookup": lookup_spec}
            new_table = create_temp(
                lookup_stage, f"{select_clause} {from_clause}"
            )
            return new_table
        finally:
            try:
                self.collection.db.execute(
                    f"DROP TABLE IF EXISTS {hash_table_name}"
                )
            except Exception as e:
                logger.debug(
                    f"Failed to drop hash table '{hash_table_name}': {e}"
                )
                pass
