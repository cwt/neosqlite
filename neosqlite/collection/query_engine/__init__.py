from __future__ import annotations

import importlib.util
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

logger = logging.getLogger(__name__)

from neosqlite.collection.jsonb_support import JSONBContext

from ...bulk_operations import BulkOperationExecutor
from ...requests import DeleteOne, InsertOne, UpdateOne
from ...results import BulkWriteResult
from ..expr_evaluator import ExprEvaluator
from ..query_helper import QueryHelper
from ..raw_batch_cursor import RawBatchCursor
from ..sql_tier_aggregator import SQLTierAggregator
from ..sql_translator_unified import SQLTranslator
from ..type_utils import validate_session
from .crud_operations import CRUDOperationsMixin
from .find_operations import FindOperationsMixin
from .python_aggregation_engine import execute_python_aggregation
from .query_methods import QueryMethodsMixin

if TYPE_CHECKING:
    from quez import CompressedQueue

    from ..client_session import ClientSession

TierChangeCallback = Callable[[str | None, str, list], None]

# Check if quez is available
_HAS_QUEZ = importlib.util.find_spec("quez") is not None


class QueryEngine(CRUDOperationsMixin, FindOperationsMixin, QueryMethodsMixin):
    """
    A class that provides methods for querying and manipulating documents in a collection.

    The QueryEngine handles all database operations including inserting, updating, deleting,
    and finding documents. It also supports aggregation pipelines, bulk operations, and
    various utility methods for counting and retrieving distinct values.
    """

    def __init__(self, collection):
        """
        Initialize the QueryEngine with a collection.

        Args:
            collection: The collection instance this QueryEngine will operate on.
        """
        self.collection = collection
        self.helpers = QueryHelper(collection)
        # Check if JSONB is supported for this connection
        self.jsonb = JSONBContext.from_db(collection.db)
        self.sql_translator = SQLTranslator(
            collection.name,
            "data",
            "id",
            self.jsonb.jsonb_supported,
            self.jsonb.json_each_function,
        )
        # Get translation cache size from connection (default: 100, 0 to disable)
        # collection._database is the NeoSQLite Connection, collection.db is sqlite3
        neosqlite_conn = collection._database
        cache_size = getattr(neosqlite_conn, "_translation_cache_size", 100)
        # Initialize SQL tier aggregator for optimized aggregation pipelines
        self.sql_tier_aggregator = SQLTierAggregator(
            collection,
            expr_evaluator=ExprEvaluator(
                data_column="data", db_connection=collection.db
            ),
            translation_cache_size=cache_size,
        )
        self._tier_callbacks: list = []  # type: ignore[annotation-unchecked]
        self._last_tier: str | None = None  # type: ignore[annotation-unchecked]

    def add_tier_change_callback(self, callback: "TierChangeCallback") -> None:
        """Add a callback to be notified when query tier changes.

        Callback receives: (previous_tier: str | None, new_tier: str, pipeline: list)
        where tier is one of:
        - "tier1" (SQL CTE - new aggregation optimizer)
        - "tier1_standard" (non-CTE SQL aggregation)
        - "tier2" (temp table for complex $expr)
        - "tier3" (Python fallback)
        - None (before any query)
        """
        self._tier_callbacks.append(callback)

    def remove_tier_change_callback(
        self, callback: "TierChangeCallback"
    ) -> bool:
        """Remove a tier change callback. Returns True if found."""
        try:
            self._tier_callbacks.remove(callback)
            return True
        except ValueError:
            return False

    def get_last_tier(self) -> str | None:
        """Get the last tier that was used for query execution."""
        return self._last_tier

    def clear_tier_callbacks(self) -> None:
        """Clear all tier change callbacks."""
        self._tier_callbacks.clear()

    def _notify_tier_change(self, new_tier: str, pipeline: list) -> None:
        """Notify all callbacks of a tier change."""
        if self._last_tier != new_tier:
            for callback in self._tier_callbacks:
                try:
                    callback(self._last_tier, new_tier, pipeline)
                except Exception as e:
                    logger.debug(f"Query tier callback error: {e}")
                    pass  # Don't let callback errors affect query execution
            self._last_tier = new_tier

    def cleanup(self) -> None:
        """Clean up resources used by the QueryEngine."""
        if hasattr(self, "helpers"):
            self.helpers.cleanup()

    def aggregate(
        self,
        pipeline: list[dict[str, Any]],
        batch_size: int = 101,
        session: ClientSession | None = None,
    ) -> list[dict[str, Any]]:
        """
        Applies a list of aggregation pipeline stages to the collection.

        This method handles both simple and complex queries. For simpler queries,
        it leverages the database's native indexing capabilities to optimize
        performance. For more complex queries, it falls back to a Python-based
        processing mechanism.

        Args:
            pipeline (list[dict[str, Any]]): A list of aggregation pipeline stages to apply.
            batch_size (int): The batch size for fetching results from database.
            session (ClientSession, optional): A ClientSession for transactions.

        Returns:
            list[dict[str, Any]]: The list of documents after applying the aggregation pipeline.
        """
        validate_session(session, self.collection._database)
        return self.aggregate_with_constraints(
            pipeline, batch_size=batch_size, session=session
        )

    def aggregate_with_constraints(
        self,
        pipeline: list[dict[str, Any]],
        batch_size: int = 101,
        memory_constrained: bool = False,
        session: ClientSession | None = None,
    ) -> list[dict[str, Any]] | "CompressedQueue":
        """
        Applies a list of aggregation pipeline stages with memory constraints.

        Args:
            pipeline (list[dict[str, Any]]): A list of aggregation pipeline stages to apply.
            batch_size (int): The batch size for processing large result sets.
            memory_constrained (bool): Whether to use memory-constrained processing.
            session (ClientSession, optional): A ClientSession for transactions.

        Returns:
            list[dict[str, Any]] | CompressedQueue: The results as either a list or compressed queue.
        """
        validate_session(session, self.collection._database)
        # If memory_constrained is True and quez is available, use quez for processing
        if memory_constrained and _HAS_QUEZ:
            # Use quez for memory-constrained processing
            return self._aggregate_with_quez(pipeline, batch_size)

        # Try SQL Tier 1 optimization first (new CTE-based approach)
        try:
            if self.sql_tier_aggregator.can_optimize_pipeline(pipeline):
                sql, params = self.sql_tier_aggregator.build_pipeline_sql(
                    pipeline
                )
                if sql is not None:
                    db_cursor = self.collection.db.execute(sql, params)
                    results = []
                    # Use fetchmany to avoid loading all results into memory at once
                    while True:
                        rows = db_cursor.fetchmany(batch_size)
                        if not rows:
                            break
                        for row in rows:
                            # Load document from data column
                            # Row structure:
                            # If root_data preserved: (id, _id, root_data, data) - len 4
                            # Normal: (id, _id, data) - len 3
                            # GROUP BY results might have id=NULL and data as a custom object

                            doc_data = row[-1]
                            doc_id = row[0]
                            stored_id = row[1]

                            if doc_data is None:
                                continue

                            if doc_data.startswith("{") and doc_data.endswith(
                                "}"
                            ):
                                # It's a JSON object (standard or GROUP BY result)
                                from neosqlite.collection.json_helpers import (
                                    neosqlite_json_loads,
                                )

                                document = neosqlite_json_loads(doc_data)
                                if (
                                    "_id" not in document
                                    and stored_id is not None
                                ):
                                    document["_id"] = (
                                        self.collection._parse_stored_id(
                                            stored_id
                                        )
                                    )
                                results.append(document)
                            else:
                                # Normal loading via _load
                                results.append(
                                    self.collection._load(
                                        doc_id, doc_data, stored_id=stored_id
                                    )
                                )
                    self._notify_tier_change("tier1", pipeline)
                    return results
        except NotImplementedError as e:
            # Operator not yet translated to SQL — log at WARNING for visibility
            # during development/comparison runs, then fall back to next tier
            logger.warning("SQL tier 1 aggregation fallback: %s", e)
        except Exception as e:
            # If SQL tier optimization fails, continue to next approach
            logger.debug("SQL tier 1 aggregation optimization failed: %s", e)

        # Try existing SQL optimization (legacy CTE-based approach)
        try:
            query_result = self.helpers._build_aggregation_query(pipeline)
            if query_result is not None:
                cmd, params, output_fields = query_result
                db_cursor = self.collection.db.execute(cmd, params)
                if output_fields:
                    # Handle results from a GROUP BY query
                    from neosqlite.collection.json_helpers import (
                        neosqlite_json_loads,
                    )

                    results = []
                    # Use fetchmany to avoid loading all results into memory at once
                    while True:
                        rows = db_cursor.fetchmany(batch_size)
                        if not rows:
                            break
                        for row in rows:
                            processed_row = []
                            for i, value in enumerate(row):
                                # If this field contains a JSON array string, parse it
                                # This handles $push and $addToSet results
                                if (
                                    output_fields[i] != "_id"
                                    and isinstance(value, str)
                                    and value.startswith("[")
                                    and value.endswith("]")
                                ):
                                    try:
                                        processed_row.append(
                                            neosqlite_json_loads(value)
                                        )
                                    except Exception as e:
                                        logger.debug(
                                            f"Failed to parse JSON in aggregation result: {e}"
                                        )
                                        processed_row.append(value)
                                else:
                                    processed_row.append(value)
                            results.append(
                                dict(zip(output_fields, processed_row))
                            )
                    self._notify_tier_change("tier1_standard", pipeline)
                    return results
                else:
                    # Handle results from a regular find query
                    # Use fetchmany to avoid loading all results into memory at once
                    results = []
                    while True:
                        rows = db_cursor.fetchmany(batch_size)
                        if not rows:
                            break
                        for row in rows:
                            # Row structure: (id, data) or (id, root_data, data)
                            if len(row) == 3:
                                # root_data is present, data is in row[2]
                                results.append(
                                    self.collection._load(row[0], row[2])
                                )
                            else:
                                # No root_data, data is in row[1]
                                results.append(
                                    self.collection._load(row[0], row[1])
                                )
                    self._notify_tier_change("tier1_standard", pipeline)
                    return results
        except Exception as e:
            # If SQL optimization fails, continue to next approach
            logger.debug(
                "SQL tier 1 standard aggregation optimization failed: %s", e
            )

        # Try the temporary table approach for complex pipelines that the
        # current SQL optimization can't handle efficiently
        try:
            from ..temporary_table_aggregation import (
                execute_2nd_tier_aggregation,
            )

            # Use the temporary table aggregation which provides enhanced
            # SQL processing for complex pipelines
            result = execute_2nd_tier_aggregation(
                self, pipeline, batch_size=batch_size
            )
            if result is not None:
                self._notify_tier_change("tier2", pipeline)
                return result
        except NotImplementedError as e:
            # Operator not yet translated to SQL — log at WARNING for visibility
            # during development/comparison runs, then fall back to Python tier
            logger.warning("SQL tier 2 aggregation fallback: %s", e)
        except Exception as e:
            # If temporary table approach fails for other reasons,
            # continue to fallback below
            logger.debug("SQL tier 2 aggregation optimization failed: %s", e)

        # Optimize $count in SQLite when possible
        if (
            pipeline
            and isinstance(pipeline[-1], dict)
            and "$count" in pipeline[-1]
        ):
            count_field = pipeline[-1]["$count"]
            if not pipeline[:-1]:
                # No previous stages, count all documents
                count = self.estimated_document_count()
                return [{count_field: count}]
            elif len(pipeline) == 2 and "$match" in pipeline[0]:
                # Only $match before $count, use count_documents
                filter = pipeline[0]["$match"]
                count = self.count_documents(filter)
                return [{count_field: count}]
            # For more complex pipelines, fall back to Python

        # Fallback to Python implementation
        return execute_python_aggregation(self, pipeline, session)

    def explain_aggregation(
        self,
        pipeline: list[dict[str, Any]],
        session: ClientSession | None = None,
    ) -> dict[str, Any]:
        """
        Explain the execution plan for an aggregation pipeline.

        Args:
            pipeline (list[dict[str, Any]]): The aggregation pipeline to explain.
            session (ClientSession, optional): A ClientSession for transactions.

        Returns:
            dict[str, Any]: The execution plan explanation.
        """
        # 1. Try SQL Tier 1 optimization
        if self.sql_tier_aggregator.can_optimize_pipeline(pipeline):
            sql, params = self.sql_tier_aggregator.build_pipeline_sql(pipeline)
            if sql is not None:
                # Use EXPLAIN QUERY PLAN to get SQLite's plan
                explain_sql = f"EXPLAIN QUERY PLAN {sql}"
                db_cursor = self.collection.db.execute(explain_sql, params)
                plan = db_cursor.fetchall()
                return {
                    "tier": 1,
                    "type": "SQL Tier 1 (CTE-based)",
                    "sql": sql,
                    "params": params,
                    "sqlite_plan": plan,
                }

        # 2. Try legacy SQL optimization
        query_result = self.helpers._build_aggregation_query(pipeline)
        if query_result is not None:
            cmd, params, _ = query_result
            explain_sql = f"EXPLAIN QUERY PLAN {cmd}"
            db_cursor = self.collection.db.execute(explain_sql, params)
            plan = db_cursor.fetchall()
            return {
                "tier": 1,
                "type": "SQL Tier 1.5 (Non-CTE-based)",
                "sql": cmd,
                "params": params,
                "sqlite_plan": plan,
            }

        # 3. Check if Tier 2 (Temp Table) can handle it
        from ..temporary_table_aggregation import (
            can_process_with_temporary_tables,
        )

        if can_process_with_temporary_tables(pipeline):
            return {
                "tier": 2,
                "type": "Temporary Table Aggregation",
                "pipeline": pipeline,
            }

        # 4. Fallback to Python
        return {
            "tier": 3,
            "type": "Python Fallback",
            "pipeline": pipeline,
        }

    def aggregate_raw_batches(
        self,
        pipeline: list[dict[str, Any]],
        batch_size: int = 100,
        session: ClientSession | None = None,
    ) -> RawBatchCursor:
        """
        Perform aggregation and retrieve batches of raw JSON.

        Similar to the :meth:`aggregate` method but returns a
        :class:`~neosqlite.raw_batch_cursor.RawBatchCursor`.

        This method returns raw JSON batches which can be more efficient for
        certain use cases where you want to process data in batches rather than
        individual documents.

        Args:
            pipeline (list[dict[str, Any]]): A list of aggregation pipeline stages to apply.
            batch_size (int): The number of documents to include in each batch.
            session (ClientSession, optional): A ClientSession for transactions.

        Returns:
            RawBatchCursor instance.
        """
        validate_session(session, self.collection._database)
        return RawBatchCursor(
            self.collection,
            None,
            None,
            None,
            batch_size,
            pipeline=pipeline,
            session=session,
        )

    # --- Bulk Write methods ---
    def bulk_write(
        self,
        requests: list[Any],
        ordered: bool = True,
        session: ClientSession | None = None,
    ) -> BulkWriteResult:
        """
        Execute bulk write operations on the collection.

        Args:
            requests: List of write operations to execute.
            ordered: If true, operations will be performed in order and will
                     raise an exception if a single operation fails.
            session (ClientSession, optional): A ClientSession for transactions.

        Returns:
            BulkWriteResult: A result object containing the number of matched,
                             modified, and inserted documents.
        """
        validate_session(session, self.collection._database)
        inserted_count = 0
        matched_count = 0
        modified_count = 0
        deleted_count = 0
        upserted_count = 0

        released = False
        self.collection.db.execute("SAVEPOINT bulk_write")
        try:
            for req in requests:
                match req:
                    case InsertOne(document=doc):
                        self.insert_one(doc, session=session)
                        inserted_count += 1
                    case UpdateOne(filter=f, update=u, upsert=up):
                        update_res = self.update_one(f, u, up, session=session)
                        matched_count += update_res.matched_count
                        modified_count += update_res.modified_count
                        if update_res.upserted_id:
                            upserted_count += 1
                    case DeleteOne(filter=f):
                        delete_res = self.delete_one(f, session=session)
                        deleted_count += delete_res.deleted_count
            self.collection.db.execute("RELEASE SAVEPOINT bulk_write")
            released = True
        except Exception as e:
            logger.debug(f"Error in bulk_write: {e}")
            self.collection.db.execute("ROLLBACK TO SAVEPOINT bulk_write")
            raise e
        finally:
            if not released:
                try:
                    self.collection.db.execute("RELEASE SAVEPOINT bulk_write")
                except Exception as e:
                    logger.debug(f"Failed to release bulk_write savepoint: {e}")
                    pass

        return BulkWriteResult(
            inserted_count=inserted_count,
            matched_count=matched_count,
            modified_count=modified_count,
            deleted_count=deleted_count,
            upserted_count=upserted_count,
        )

    def _aggregate_with_quez(
        self, pipeline: list[dict[str, Any]], batch_size: int = 101
    ) -> CompressedQueue:
        """
        Process aggregation pipeline with quez compressed queue for memory efficiency.

        Args:
            pipeline (list[dict[str, Any]]): A list of aggregation pipeline stages to apply.
            batch_size (int): The batch size for quez queue processing.

        Returns:
            CompressedQueue: A compressed queue containing the results.
        """
        try:
            if _HAS_QUEZ:
                from quez import CompressedQueue

                # Create a compressed queue for results with a reasonable size
                # Use unbounded queue to avoid blocking during population
                result_queue = CompressedQueue()

            # Get results from normal aggregation
            results = self.aggregate(pipeline)

            # Add all results to the compressed queue
            for result in results:
                result_queue.put(result)

            return result_queue

        except ImportError:
            # If quez is not available, fall back to normal processing
            # This should never happen since we check for quez availability before calling this method
            raise RuntimeError("Quez is not available but was expected to be")

    def initialize_ordered_bulk_op(self) -> BulkOperationExecutor:
        """Initialize an ordered bulk operation.

        Returns:
            BulkOperationExecutor: An executor for ordered bulk operations.
        """
        return BulkOperationExecutor(self.collection, ordered=True)

    def initialize_unordered_bulk_op(self) -> BulkOperationExecutor:
        """Initialize an unordered bulk operation.

        Returns:
            BulkOperationExecutor: An executor for unordered bulk operations.
        """
        return BulkOperationExecutor(self.collection, ordered=False)
