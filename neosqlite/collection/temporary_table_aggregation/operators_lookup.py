from __future__ import annotations

import hashlib
import logging
from typing import Any, Callable

from ..._sqlite import sqlite3
from ...sql_utils import quote_table_name
from ..json_path_utils import parse_json_path
from .operators_base import OperatorsBaseMixin
from .utils import (
    _json_extract_field_with_objectid_support,
)

logger = logging.getLogger(__name__)

HASH_JOIN_MEMORY_THRESHOLD = 100 * 1024 * 1024  # 100 MB default threshold


class OperatorsLookupMixin(OperatorsBaseMixin):
    def _create_lookup_hash_table(
        self,
        from_collection: str,
        foreign_field: str | None,
        pipeline: list[dict[str, Any]] | None = None,
    ) -> tuple[str, str]:
        """
        Create a hash table (temp table with index) from a foreign collection
        for efficient hash join lookup.

        This implements O(n+m) hash join instead of O(n*m) correlated subquery.

        Args:
            from_collection: The collection to build hash table from
            foreign_field: The field to use as join key (None for _id)
            pipeline: Optional pipeline to run on foreign collection first

        Returns:
            Tuple of (hash_table_name, join_key_column)
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
                        f"CREATE TEMP TABLE {hash_table_name} (id INTEGER PRIMARY KEY, _id INTEGER, data TEXT, {join_key} TEXT)"
                    )
                else:
                    from ..json_helpers import neosqlite_json_dumps

                    self.db.execute(
                        f"CREATE TEMP TABLE {hash_table_name} (id INTEGER PRIMARY KEY, _id INTEGER, data TEXT, {join_key} TEXT)"
                    )

                    if foreign_field == "_id":
                        for doc in pipeline_result:
                            self.db.execute(
                                f"INSERT INTO {hash_table_name} (id, _id, data, {join_key}) VALUES (?, ?, ?, ?)",
                                (
                                    doc.get("id", 0),
                                    doc.get("_id"),
                                    neosqlite_json_dumps(doc),
                                    str(doc.get("_id")),
                                ),
                            )
                    else:
                        for doc in pipeline_result:
                            key_val = self._extract_field_value(
                                doc, foreign_field
                            )
                            self.db.execute(
                                f"INSERT INTO {hash_table_name} (id, _id, data, {join_key}) VALUES (?, ?, ?, ?)",
                                (
                                    doc.get("id", 0),
                                    doc.get("_id"),
                                    neosqlite_json_dumps(doc),
                                    (
                                        str(key_val)
                                        if key_val is not None
                                        else None
                                    ),
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
                            self._json_function_prefix,
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
        """
        Fallback method to create hash table by reading documents one by one.

        Used when the SQL approach fails due to malformed JSON in the data column.
        This method skips corrupted documents gracefully.

        Args:
            hash_table_name: Name of the hash table to create
            from_collection: Source collection name
            foreign_field: Field to use as join key
            join_key: Name of the join key column
        """
        from ..json_helpers import neosqlite_json_dumps, neosqlite_json_loads

        # Create the hash table structure
        self.db.execute(
            f"CREATE TEMP TABLE {hash_table_name} "
            f"(id INTEGER PRIMARY KEY, _id TEXT, data TEXT, {join_key} TEXT)"
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
                    key_val = (
                        str(doc_underscore_id) if doc_underscore_id else None
                    )
                else:
                    # Navigate nested field path
                    key_val = self._extract_field_value(doc, foreign_field)
                    key_val = str(key_val) if key_val is not None else None

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
        """
        Estimate the size of a collection in bytes.

        Uses SQLite's table statistics to estimate size.

        Args:
            collection_name: Name of the collection to estimate

        Returns:
            Estimated size in bytes
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
        """
        Get available memory for hash join operations.

        Returns:
            Available memory in bytes (estimated from SQLite cache or system)
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

            soft, hard = resource.getrlimit(resource.RLIMIT_AS)
            if soft != resource.RLIM_INFINITY:
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
        """
        Decide whether to use hash join or correlated subquery for $lookup.

        Uses memory estimation to decide:
        - If foreign collection estimate < 30% of available memory: use hash join (faster)
        - Otherwise: use correlated subquery (lower memory, slower)

        Args:
            from_collection: The foreign collection name
            pipeline: Optional pipeline to run on foreign collection first

        Returns:
            True if should use hash join, False for correlated subquery
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
        """Extract field value from document, supporting dot notation."""
        parts = field.split(".")
        val: Any = doc
        for part in parts:
            if isinstance(val, dict):
                val = val.get(part)
            else:
                return None
        return val

    def _process_lookup_stage(
        self,
        create_temp: Callable,
        current_table: str,
        lookup_spec: dict[str, Any],
    ) -> str:
        """
        Process a $lookup stage using hash join for O(n+m) complexity.

        This method implements the $lookup aggregation stage which performs a left
        outer join to another collection in the same database. It uses an optimized
        hash join approach:
        1. Creates a temporary table with an index on the foreign field (hash table)
        2. Uses a single JOIN query instead of correlated subquery

        This replaces the previous correlated subquery approach which was O(n*m).

        Args:
            create_temp (Callable): Function to create temporary tables
            current_table (str): Name of the current temporary table containing input data
            lookup_spec (dict[str, Any]): The $lookup stage specification containing:
                - "from": The name of the collection to join with
                - "localField": The field from the input documents
                - "foreignField": The field from the documents of the "from" collection
                - "as": The name of the new array field to add to the matching documents
                - "pipeline": Optional pipeline to run on foreign collection

        Returns:
            str: Name of the newly created temporary table with lookup results added
        """
        from_collection = lookup_spec["from"]
        pipeline = lookup_spec.get("pipeline", [])

        use_hash_join = self._should_use_hash_join(from_collection, pipeline)

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
        """
        Process $lookup using correlated subquery (O(n*m) but low memory).

        This is the fallback when the foreign collection is too large for hash join.

        Args:
            create_temp: Function to create temporary tables
            current_table: Current temp table name
            lookup_spec: The $lookup specification

        Returns:
            Name of the new temporary table
        """
        from_collection = lookup_spec["from"]
        local_field = lookup_spec.get("localField")
        foreign_field = lookup_spec.get("foreignField")
        as_field = lookup_spec["as"]
        pipeline = lookup_spec.get("pipeline", [])

        json_set_func = f"{self._json_function_prefix}_set"

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
                    for doc in pipeline_result:
                        self.collection.db.execute(
                            f"INSERT INTO {pipeline_result_table} (id, _id, data) VALUES (?, ?, ?)",
                            (
                                doc.get("id", 0),
                                doc.get("_id"),
                                neosqlite_json_dumps(doc),
                            ),
                        )

                if foreign_field == "_id":
                    foreign_extract = "related._id"
                else:
                    # Use ObjectId-aware extraction
                    foreign_extract = _json_extract_field_with_objectid_support(
                        self._json_function_prefix,
                        foreign_field,
                        is_local_field=False,
                    )

                if local_field == "_id":
                    local_extract = f"COALESCE({self._json_function_prefix}_extract(main_table.data, '$._id'), main_table.id)"
                else:
                    # Use ObjectId-aware extraction
                    local_extract = _json_extract_field_with_objectid_support(
                        self._json_function_prefix,
                        local_field,
                        is_local_field=True,
                    )

                select_clause = (
                    f"SELECT main_table.id, "
                    f"json({json_set_func}({json_set_func}(main_table.data, '$._id', main_table.id), '{parse_json_path(as_field)}', "
                    f"coalesCE(( "
                    f"  SELECT {self.json_group_array_function}(json(related.data)) "
                    f"  FROM {pipeline_result_table} as related "
                    f"  WHERE {foreign_extract} = "
                    f"        {local_extract} "
                    f"), json('[]')))) as data"
                )

                from_clause = f"FROM {current_table} as main_table"

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

        local_field_str: str = local_field  # type: ignore[assignment]
        foreign_field_str: str = foreign_field  # type: ignore[assignment]

        if foreign_field_str == "_id":
            foreign_extract = "related._id"
        else:
            # Use ObjectId-aware extraction
            foreign_extract = _json_extract_field_with_objectid_support(
                self._json_function_prefix,
                foreign_field_str,
                is_local_field=False,
            )

        if local_field_str == "_id":
            local_extract = f"COALESCE({self._json_function_prefix}_extract(main_table.data, '$._id'), main_table.id)"
        else:
            # Use ObjectId-aware extraction
            local_extract = _json_extract_field_with_objectid_support(
                self._json_function_prefix, local_field_str, is_local_field=True
            )

        select_clause = (
            f"SELECT main_table.id, "
            f"json({json_set_func}({json_set_func}(main_table.data, '$._id', main_table.id), '{parse_json_path(as_field)}', "
            f"coalesCE(( "
            f"  SELECT {self.json_group_array_function}(json(related.data)) "
            f"  FROM {from_collection} as related "
            f"  WHERE {foreign_extract} = "
            f"        {local_extract} "
            f"), json('[]')))) as data"
        )

        from_clause = f"FROM {current_table} as main_table"

        lookup_stage = {"$lookup": lookup_spec}
        new_table = create_temp(lookup_stage, f"{select_clause} {from_clause}")
        return new_table

    def _process_lookup_hash_join(
        self,
        create_temp: Callable,
        current_table: str,
        lookup_spec: dict[str, Any],
    ) -> str:
        """
        Process $lookup using hash join (O(n+m) but uses more memory).

        Args:
            create_temp: Function to create temporary tables
            current_table: Current temp table name
            lookup_spec: The $lookup specification

        Returns:
            Name of the new temporary table
        """
        from_collection = lookup_spec["from"]
        local_field = lookup_spec.get("localField")
        foreign_field = lookup_spec.get("foreignField")
        as_field = lookup_spec["as"]
        pipeline = lookup_spec.get("pipeline", [])

        json_set_func = f"{self._json_function_prefix}_set"

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
                local_extract = f"CAST(COALESCE({self._json_function_prefix}_extract(main_table.data, '$._id'), main_table.id) AS TEXT)"
            else:
                local_extract = _json_extract_field_with_objectid_support(
                    self._json_function_prefix, local_field, is_local_field=True
                )

            select_clause = (
                f"SELECT main_table.id, "
                f"json({json_set_func}({json_set_func}(main_table.data, '$._id', main_table.id), '$.{as_field}', "
                f"COALESCE(aggregated.results, json('[]')))) as data "
            )

            from_clause = (
                f"FROM {current_table} as main_table "
                f"LEFT JOIN ("
                f"  SELECT {join_key}, {self.json_group_array_function}(json(data)) as results "
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
