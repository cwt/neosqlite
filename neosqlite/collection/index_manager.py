from ..exceptions import MalformedQueryException
from typing import Any, Dict, List, Tuple, overload
from typing_extensions import Literal

try:
    from pysqlite3 import dbapi2 as sqlite3
except ImportError:
    import sqlite3  # type: ignore


class IndexManager:
    def __init__(self, collection):
        self.collection = collection

    def create_index(
        self,
        key: str | List[str],
        reindex: bool = True,
        sparse: bool = False,
        unique: bool = False,
        fts: bool = False,
        tokenizer: str | None = None,
    ):
        """
        Create an index on the specified key(s) for this collection.

        Handles both single-key and compound indexes by using SQLite's json_extract
        function to create indexes on the JSON-stored data. For compound indexes,
        multiple json_extract calls are used for each key in the list.

        Args:
            key: A string or list of strings representing the field(s) to index.
            reindex: Boolean indicating whether to reindex (not used in this implementation).
            sparse: Boolean indicating whether the index should be sparse (only include documents with the field).
            unique: Boolean indicating whether the index should be unique.
            fts: Boolean indicating whether to create an FTS index for text search.
            tokenizer: Optional tokenizer to use for FTS index (e.g., 'icu', 'icu_th').
        """
        # For single key indexes, we can use SQLite's native JSON indexing
        if isinstance(key, str):
            if fts:
                # Create FTS index with optional tokenizer
                self._create_fts_index(key, tokenizer)
            else:
                # Create index name (replace dots with underscores for valid identifiers)
                index_name = key.replace(".", "_")

                # Create the index using json_extract
                self.collection.db.execute(
                    (
                        f"CREATE {'UNIQUE ' if unique else ''}INDEX "
                        f"IF NOT EXISTS [idx_{self.collection.name}_{index_name}] "
                        f"ON {self.collection.name}(json_extract(data, '$.{key}'))"
                    )
                )
        else:
            # For compound indexes, we still need to handle them differently
            # This is a simplified implementation - we could expand on this later
            index_name = "_".join(key).replace(".", "_")

            # Create the compound index using multiple json_extract calls
            index_columns = ", ".join(
                f"json_extract(data, '$.{k}')" for k in key
            )
            self.collection.db.execute(
                (
                    f"CREATE {'UNIQUE ' if unique else ''}INDEX "
                    f"IF NOT EXISTS [idx_{self.collection.name}_{index_name}] "
                    f"ON {self.collection.name}({index_columns})"
                )
            )

    def _create_fts_index(self, field: str, tokenizer: str | None = None):
        """
        Creates an FTS5 index on the specified field for text search.

        Args:
            field (str): The field to create the FTS index on.
            tokenizer (str, optional): Optional tokenizer to use for the FTS index.

        Raises:
            MalformedQueryException: If FTS5 is not available in the SQLite installation.
        """
        # Create FTS5 virtual table
        index_name = field.replace(".", "_")
        fts_table_name = f"{self.collection.name}_{index_name}_fts"

        # Check if FTS5 is available by trying to create a simple FTS table
        try:
            # Try to create a temporary FTS table to check if FTS5 is available
            self.collection.db.execute(
                "CREATE VIRTUAL TABLE IF NOT EXISTS temp.fts_test USING fts5(test)"
            )
            self.collection.db.execute("DROP TABLE IF EXISTS temp.fts_test")
        except sqlite3.OperationalError:
            raise MalformedQueryException(
                "FTS5 is not available in this SQLite installation"
            )

        # Create the FTS table with optional tokenizer
        if tokenizer:
            self.collection.db.execute(f"""
                CREATE VIRTUAL TABLE IF NOT EXISTS {fts_table_name}
                USING fts5(
                    content='{self.collection.name}',
                    content_rowid='id',
                    {index_name},
                    tokenize='{tokenizer}'
                )
                """)
        else:
            self.collection.db.execute(f"""
                CREATE VIRTUAL TABLE IF NOT EXISTS {fts_table_name}
                USING fts5(
                    content='{self.collection.name}',
                    content_rowid='id',
                    {index_name}
                )
                """)

        # Create triggers to keep the FTS index in sync with the main table
        # Delete existing triggers if they exist
        self.collection.db.execute(
            f"DROP TRIGGER IF EXISTS {self.collection.name}_{index_name}_fts_insert"
        )
        self.collection.db.execute(
            f"DROP TRIGGER IF EXISTS {self.collection.name}_{index_name}_fts_update"
        )
        self.collection.db.execute(
            f"DROP TRIGGER IF EXISTS {self.collection.name}_{index_name}_fts_delete"
        )

        # Insert trigger
        self.collection.db.execute(f"""
            CREATE TRIGGER IF NOT EXISTS {self.collection.name}_{index_name}_fts_insert
            AFTER INSERT ON {self.collection.name}
            BEGIN
                INSERT INTO {fts_table_name}(rowid, {index_name})
                VALUES (new.id, lower(json_extract(new.data, '$.{field}')));
            END
            """)

        # Update trigger
        self.collection.db.execute(f"""
            CREATE TRIGGER IF NOT EXISTS {self.collection.name}_{index_name}_fts_update
            AFTER UPDATE ON {self.collection.name}
            BEGIN
                INSERT INTO {fts_table_name}({fts_table_name}, rowid, {index_name})
                VALUES ('delete', old.id, lower(json_extract(old.data, '$.{field}')));
                INSERT INTO {fts_table_name}(rowid, {index_name})
                VALUES (new.id, lower(json_extract(new.data, '$.{field}')));
            END
            """)

        # Delete trigger
        self.collection.db.execute(f"""
            CREATE TRIGGER IF NOT EXISTS {self.collection.name}_{index_name}_fts_delete
            AFTER DELETE ON {self.collection.name}
            BEGIN
                INSERT INTO {fts_table_name}({fts_table_name}, rowid, {index_name})
                VALUES ('delete', old.id, lower(json_extract(old.data, '$.{field}')));
            END
            """)

        # Populate the FTS index with existing data
        self.collection.db.execute(f"""
            INSERT INTO {fts_table_name}(rowid, {index_name})
            SELECT id, lower(json_extract(data, '$.{field}'))
            FROM {self.collection.name}
            WHERE json_extract(data, '$.{field}') IS NOT NULL
            """)

    def create_indexes(
        self,
        indexes: List[str | List[str] | List[Tuple[str, int]] | Dict[str, Any]],
    ) -> List[str]:
        """
        Create multiple indexes at once.

        This method provides a convenient way to create several indexes in a single call.
        It supports various formats for specifying indexes, including simple strings for
        single-field indexes, lists for compound indexes, and dictionaries for indexes
        with additional options.

        Args:
            indexes: A list of index specifications in various formats:
                    - str: Simple single-field index
                    - List[str]: Compound index with multiple fields
                    - List[Tuple[str, int]]: Compound index with field names and sort directions
                    - Dict: Index with additional options like unique, sparse, fts

        Returns:
            List[str]: A list of the names of the indexes that were created.
        """
        created_indexes = []
        for index_spec in indexes:
            # Handle dict format with options
            if isinstance(index_spec, dict):
                key: str | List[str] | None = index_spec.get("key")
                unique: bool = bool(index_spec.get("unique", False))
                sparse: bool = bool(index_spec.get("sparse", False))
                fts: bool = bool(index_spec.get("fts", False))
                tokenizer: str | None = index_spec.get("tokenizer")

                if key is not None:
                    self.create_index(
                        key,
                        unique=unique,
                        sparse=sparse,
                        fts=fts,
                        tokenizer=tokenizer,
                    )
                    if isinstance(key, str):
                        index_name = key.replace(".", "_")
                    else:
                        index_name = "_".join(str(k) for k in key).replace(
                            ".", "_"
                        )
                    created_indexes.append(
                        f"idx_{self.collection.name}_{index_name}"
                    )

            # Handle string format
            elif isinstance(index_spec, str):
                # Simple string key
                self.create_index(index_spec)
                index_name = index_spec.replace(".", "_")
                created_indexes.append(
                    f"idx_{self.collection.name}_{index_name}"
                )
            elif isinstance(index_spec, list):
                # List of keys for compound index
                # Handle both ['name', 'age'] and [('name', 1), ('age', -1)] formats
                if index_spec and isinstance(index_spec[0], tuple):  # type: ignore
                    # Format [('name', 1), ('age', -1)] - extract just the field names
                    key_list: List[str] = []
                    # Type assertion: we know this is List[Tuple[str, int]] at this point
                    tuple_list: List[Tuple[str, int]] = index_spec  # type: ignore
                    for k, _ in tuple_list:
                        key_list.append(k)
                    self.create_index(key_list)
                    # Join the key list with underscores
                    str_keys: List[str] = []
                    for k in key_list:
                        str_keys.append(str(k))
                    index_name = "_".join(str_keys).replace(".", "_")
                else:
                    # Format ['name', 'age']
                    # Type check: we know this is List[str] at this point
                    str_list: List[str] = index_spec  # type: ignore
                    self.create_index(str_list)
                    # Join the string list with underscores
                    str_keys2: List[str] = []
                    for k in str_list:
                        str_keys2.append(str(k))
                    index_name = "_".join(str_keys2).replace(".", "_")
                created_indexes.append(
                    f"idx_{self.collection.name}_{index_name}"
                )

        return created_indexes

    def reindex(
        self,
        table: str,
        sparse: bool = False,
        documents: List[Dict[str, Any]] | None = None,
    ):
        """
        Reindex the collection.

        With native JSON indexing, reindexing is handled automatically by SQLite.
        This method is kept for API compatibility but does nothing.

        Args:
            table (str): The table name (not used in this implementation).
            sparse (bool): Whether the index should be sparse (not used in this implementation).
            documents (List[Dict[str, Any]]): List of documents to reindex (not used in this implementation).
        """
        # With native JSON indexing, reindexing is handled automatically by SQLite
        # This method is kept for API compatibility but does nothing
        pass

    @overload
    def list_indexes(self, as_keys: Literal[True]) -> List[List[str]]: ...
    @overload
    def list_indexes(self, as_keys: Literal[False] = False) -> List[str]: ...
    def list_indexes(
        self,
        as_keys: bool = False,
    ) -> List[str] | List[List[str]]:
        """
        Retrieve indexes for the collection. Indexes are identified by names following a specific pattern.

        Args:
            as_keys (bool): If True, return the key names (converted from
                            underscores to dots) instead of the full index names.

        Returns:
            List[str] or List[List[str]]: List of index names or keys, depending on the as_keys parameter.
                If as_keys is True, each entry is a list containing a single string (the key name).
        """
        # Get indexes that match our naming convention
        cmd = (
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE ?"
        )
        like_pattern = f"idx_{self.collection.name}_%"
        if as_keys:
            # Extract key names from index names
            indexes = self.collection.db.execute(
                cmd, (like_pattern,)
            ).fetchall()
            result = []
            for idx in indexes:
                # Extract key name from index name (idx_collection_key -> key)
                key_name = idx[0][len(f"idx_{self.collection.name}_") :]
                # Convert underscores back to dots for nested keys
                key_name = key_name.replace("_", ".")
                result.append([key_name])
            return result
        # Return index names
        return [
            idx[0]
            for idx in self.collection.db.execute(
                cmd, (like_pattern,)
            ).fetchall()
        ]

    def drop_index(self, index: str):
        """
        Drop an index from the collection.

        Handles both single-key and compound indexes. For compound indexes, the
        input should be a list of field names. The index name is generated by
        joining the field names with underscores and replacing dots with underscores.

        Args:
            index (str or list): The name of the index to drop. If a list is provided,
                                 it represents a compound index.
        """
        # With native JSON indexing, we just need to drop the index
        if isinstance(index, str):
            # For single indexes
            index_name = index.replace(".", "_")
            self.collection.db.execute(
                f"DROP INDEX IF EXISTS idx_{self.collection.name}_{index_name}"
            )
        else:
            # For compound indexes
            index_name = "_".join(index).replace(".", "_")
            self.collection.db.execute(
                f"DROP INDEX IF EXISTS idx_{self.collection.name}_{index_name}"
            )

    def drop_indexes(self):
        """
        Drop all indexes associated with this collection.

        This method retrieves the list of indexes using the list_indexes method
        and drops each one.
        """
        indexes = self.list_indexes()
        for index in indexes:
            # Extract the actual index name from the full name
            self.collection.db.execute(f"DROP INDEX IF EXISTS {index}")

    def index_information(self) -> Dict[str, Any]:
        """
        Retrieves information on this collection's indexes.

        The function fetches all indexes associated with the collection and extracts
        relevant details such as whether the index is unique and the keys used in
        the index. It constructs a dictionary where the keys are the index names
        and the values are dictionaries containing the index information.

        Returns:
            dict: A dictionary containing index information.
        """
        info: Dict[str, Any] = {}

        try:
            # Get all indexes for this collection
            indexes = self.collection.db.execute(
                "SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name=?",
                (self.collection.name,),
            ).fetchall()

            for idx_name, idx_sql in indexes:
                # Parse the index information
                index_info: Dict[str, Any] = {
                    "v": 2,  # Index version
                }

                # Check if it's a unique index
                if idx_sql and "UNIQUE" in idx_sql.upper():
                    index_info["unique"] = True
                else:
                    index_info["unique"] = False

                # Try to extract key information from the SQL
                if idx_sql:
                    # Extract key information from json_extract expressions
                    import re

                    json_extract_matches = re.findall(
                        r"json_extract\(data, '(\$..*?)'\)", idx_sql
                    )
                    if json_extract_matches:
                        # Convert SQLite JSON paths back to dot notation
                        keys = []
                        for path in json_extract_matches:
                            # Remove $ and leading dot
                            if path.startswith("$."):
                                path = path[2:]
                            keys.append(path)

                        if len(keys) == 1:
                            index_info["key"] = {keys[0]: 1}
                        else:
                            index_info["key"] = {key: 1 for key in keys}

                info[idx_name] = index_info

        except sqlite3.Error:
            # If we can't get index information, return empty dict
            pass

        return info
