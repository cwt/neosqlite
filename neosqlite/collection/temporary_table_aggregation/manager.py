from __future__ import annotations

import hashlib
import logging
import uuid
from contextlib import contextmanager
from typing import Any

logger = logging.getLogger(__name__)

class DeterministicTempTableManager:
    """
    Manager for deterministic temporary table names.

    This class generates unique but deterministic temporary table names based on
    pipeline stages and a pipeline ID. It ensures that the same pipeline stage
    will always generate the same table name within the same pipeline execution,
    which is useful for caching and optimization purposes.
    """

    def __init__(self, pipeline_id: str):
        """
        Initialize the DeterministicTempTableManager with a pipeline ID for generating
        unique table names.

        Args:
            pipeline_id (str): A unique identifier for the pipeline, used to ensure
                               table names are deterministic and unique across
                               different pipeline executions.
        """
        self.pipeline_id = pipeline_id
        self.stage_counter = 0
        self.name_counter: dict[str, int] = (
            {}
        )  # Track how many times each name has been used

    def make_temp_table_name(
        self, stage: dict[str, Any], name_suffix: str = ""
    ) -> str:
        """
        Generate a deterministic temporary table name based on the pipeline stage
        and pipeline ID.

        This method creates a unique but deterministic name for a temporary table by:
        1. Creating a canonical representation of the stage
        2. Hashing the stage to create a short, unique suffix
        3. Combining the pipeline ID, stage type, and hash to form a base name
        4. Ensuring uniqueness by tracking name usage within the pipeline

        Args:
            stage (dict[str, Any]): The pipeline stage dictionary used to generate
                                    the table name
            name_suffix (str, optional): An additional suffix to append to the
                                         table name. Defaults to "".

        Returns:
            str: A deterministic temporary table name unique to this stage and
                 pipeline
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
    """
    Context manager for temporary aggregation tables with automatic cleanup.

    This context manager provides a clean and safe way to work with temporary
    tables during aggregation pipeline processing. It handles:

    1. Creating a savepoint for atomicity of the entire pipeline
    2. Generating deterministic temporary table names
    3. Providing a function to create temporary tables with proper naming
    4. Automatic cleanup of all temporary tables and savepoint on exit

    The context manager supports both new deterministic naming (using stage dictionaries)
    and backward compatibility (using string suffixes) for temporary tables.

    Args:
        db_connection: The database connection object
        pipeline_id (str | None): A unique identifier for the pipeline. If None,
                                  a default ID is generated for backward compatibility.

    Yields:
        Callable: A function to create temporary tables with the signature:
                  create_temp_table(stage_or_suffix, query, params=None, name_suffix="")

                  Where:
                  - stage_or_suffix: Either a stage dict (new approach) or string
                                     (backward compatibility)
                  - query: The SQL query to populate the temporary table
                  - params: Optional parameters for the SQL query
                  - name_suffix: Optional suffix for backward compatibility naming

    Raises:
        Exception: Any exception that occurs during pipeline processing is re-raised
                   after cleanup operations
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
        """
        Create a temporary table for pipeline processing with deterministic naming.

        This function supports both the new deterministic naming approach (using
        stage dictionaries) and the old backward-compatible approach (using string
        suffixes) for temporary table names.

        The function creates a temporary table by executing a CREATE TEMP TABLE
        AS SELECT statement with the provided query and optional parameters. The
        table name is generated deterministically based on the pipeline stage or
        provided suffix, ensuring uniqueness within the pipeline context.

        Args:
            stage_or_suffix (Any): Either a stage dictionary (new approach) for
                                   deterministic naming or a string suffix (backward
                                   compatibility). When using the new approach,
                                   this should be the pipeline stage dictionary
                                   that determines the table name. When using the
                                   old approach, this should be a string suffix
                                   for the table name.
            query (str): The SQL query used to populate the temporary table
            params (list[Any] | None, optional): Parameters for the SQL query.
                                                 Defaults to None.
            name_suffix (str, optional): Additional suffix for table name (used
                                         only in backward compatibility mode).
                                         Defaults to "".

        Returns:
            str: The name of the created temporary table

        Raises:
            Exception: Any database execution errors are propagated to the caller
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
        logger.error(f"Temporary table aggregation error: {e}", exc_info=True)
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

