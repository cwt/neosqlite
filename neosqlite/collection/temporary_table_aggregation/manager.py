from __future__ import annotations

import hashlib
import logging
import uuid
from contextlib import contextmanager
from typing import Any

logger = logging.getLogger(__name__)


class DeterministicTempTableManager:
    """Manager for deterministic temporary table names.

    Generates unique but deterministic temporary table names based on pipeline
    stages and a pipeline ID, ensuring that the same stage produces the same
    base table name within a pipeline execution.
    """

    def __init__(self, pipeline_id: str):
        """Initialize the manager with a pipeline ID for temp table naming.

        Args:
            pipeline_id: Unique identifier for the pipeline, used to keep
                generated table names deterministic and collision-free across
                pipeline executions.
        """
        self.pipeline_id = pipeline_id
        self.stage_counter = 0
        self.name_counter: dict[str, int] = (
            {}
        )  # Track how many times each name has been used

    def make_temp_table_name(
        self, stage: dict[str, Any], name_suffix: str = ""
    ) -> str:
        """Generate a deterministic temporary table name for a pipeline stage.

        Canonicalizes ``stage`` items, hashes them with SHA-256 to a 6-char
        hex suffix, combines ``pipeline_id``, the leading stage name without
        ``"$"``, the hash, and ``name_suffix``, and disambiguates repeated
        base names via ``self.name_counter``.

        Args:
            stage: Pipeline stage dictionary used to derive the table name.
            name_suffix: Optional suffix appended to the base table name.
                Defaults to ``""``.

        Returns:
            str: Unique, deterministic temporary table name within this
                pipeline context.
        """
        # Create a canonical representation of the stage
        stage_key = str(sorted(stage.items()))
        # Hash the stage to create a short, unique suffix
        hash_suffix = hashlib.sha256(stage_key.encode()).hexdigest()[:6]
        # Get the stage type (e.g., "match", "unwind")
        stage_type = next(iter(stage.keys())).lstrip("$")

        # Create a base name
        base_name = (
            f"temp_{self.pipeline_id}_{stage_type}_{hash_suffix}{name_suffix}"
        )

        # Ensure uniqueness by tracking usage
        if base_name in self.name_counter:
            self.name_counter[base_name] += 1
            unique_name = f"{base_name}_{self.name_counter[base_name]}"
        else:
            self.name_counter[base_name] = 0
            unique_name = base_name

        return unique_name


@contextmanager
def aggregation_pipeline_context(db_connection, pipeline_id: str | None = None):
    """Provide a transactional temporary-table context for pipelines.

    Opens a SQLite ``SAVEPOINT`` for atomicity, instantiates a
    ``DeterministicTempTableManager``, and yields a helper function that
    creates and tracks temporary tables. On error, rolls back to the
    savepoint, logs a warning, and re-raises; on exit, releases the savepoint
    and drops all created temporary tables.

    Args:
        db_connection: SQLite database connection supporting ``execute``.
        pipeline_id: Optional pipeline identifier for deterministic table and
            savepoint naming. When ``None``, a random default ID is generated.

    Yields:
        Callable: Helper function ``create_temp_table`` accepting
            ``(stage_or_suffix, query, params=None, name_suffix="")`` and
            returning the created table name.

    Raises:
        NotImplementedError: Re-raised after rolling back to the savepoint if
            an unsupported SQL translation falls back.
        Exception: Re-raised after rolling back to the savepoint if pipeline
            execution fails.
    """
    temp_tables = []

    # Generate a default pipeline ID if none provided (for backward compatibility)
    if pipeline_id is None:
        pipeline_id = f"default_{uuid.uuid4().hex[:8]}"

    savepoint_name = f"agg_pipeline_{pipeline_id}"

    # Create savepoint for atomicity
    db_connection.execute(f"SAVEPOINT {savepoint_name}")

    # Create a deterministic temp table manager
    temp_manager = DeterministicTempTableManager(pipeline_id)

    def create_temp_table(
        stage_or_suffix: Any,  # Can be dict[str, Any] for new usage or str for backward compatibility
        query: str,
        params: list[Any] | None = None,
        name_suffix: str = "",  # Used only for backward compatibility
    ) -> str:
        """Create and register a temporary table populated by a SQL query.

        Uses deterministic naming via ``DeterministicTempTableManager`` when
        ``stage_or_suffix`` is a stage dictionary, or falls back to a
        UUID-suffixed name when given a string suffix.

        Args:
            stage_or_suffix: Stage dictionary for deterministic naming, or a
                string suffix (anything else defaults to ``"unknown"``) for
                legacy UUID-based naming.
            query: SQL ``SELECT`` statement used to populate the table.
            params: Optional sequence of bind parameters for ``query``.
                Defaults to ``None``.
            name_suffix: Extra suffix passed to ``make_temp_table_name`` when
                ``stage_or_suffix`` is a dict. Defaults to ``""``.

        Returns:
            str: Name of the newly created temporary table.

        Raises:
            Exception: Propagates any SQLite execution error after the outer
                context rolls back the savepoint.
        """
        # Check if we're using the new approach (stage is a dict) or old approach (stage is a string)
        if isinstance(stage_or_suffix, dict):
            # New approach - deterministic naming
            table_name = temp_manager.make_temp_table_name(
                stage_or_suffix, name_suffix
            )
        else:
            # Old approach - backward compatibility
            if isinstance(stage_or_suffix, str):
                suffix = stage_or_suffix
            else:
                suffix = "unknown"

            table_name = f"temp_{suffix}_{uuid.uuid4().hex}"

        if params is not None:
            db_connection.execute(
                f"CREATE TEMP TABLE {table_name} AS {query}", params
            )
        else:
            db_connection.execute(f"CREATE TEMP TABLE {table_name} AS {query}")
        temp_tables.append(table_name)
        return table_name

    try:
        yield create_temp_table
    except NotImplementedError as e:
        # Expected fallback for operators not yet translated to SQL —
        # log at WARNING so it's visible during development/comparison
        # runs, but without the noisy traceback
        db_connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
        logger.warning(f"Temporary table aggregation SQL fallback: {e}")
        raise
    except Exception as e:
        # Rollback on error
        db_connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
        # Expected tier degradation (unsupported SQL shapes) — log at
        # WARNING without a traceback; ERROR + traceback buried real
        # failures under routine fallback noise (#169)
        logger.warning(f"Temporary table aggregation error: {e}")
        raise
    finally:
        # Cleanup
        db_connection.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        # Explicitly drop temp tables
        for table_name in temp_tables:
            try:
                db_connection.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception as drop_error:
                logger.debug(
                    f"Failed to drop temp table '{table_name}': {drop_error}"
                )
                pass
