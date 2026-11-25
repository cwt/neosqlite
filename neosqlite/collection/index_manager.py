import json as _json
import logging
import re
from typing import TYPE_CHECKING, Any, Literal, overload

from .._sqlite import sqlite3
from ..sql_utils import quote_identifier, quote_table_name, validate_identifier
from .json_path_utils import parse_json_path
from .jsonb_support import JSONBContext

if TYPE_CHECKING:
    from ..index_model import IndexModel

logger = logging.getLogger(__name__)


_INDEX_KEYS_TABLE = "neosqlite_index_keys"

_TTL_META_TABLE = "_neosqlite_ttl_indexes"
_TTL_MAX_SECONDS = 2147483647


def _ensure_ttl_table(db) -> None:
    """Create the TTL index metadata table if it does not already exist.

    Args:
        db: SQLite database connection.
    """
    db.execute(
        f"CREATE TABLE IF NOT EXISTS {_TTL_META_TABLE} ("
        "index_name TEXT PRIMARY KEY, "
        "collection_name TEXT NOT NULL, "
        "field TEXT NOT NULL, "
        "expire_after_seconds INTEGER NOT NULL)"
    )


def _store_ttl_option(
    db, index_name: str, collection_name: str, field: str, expire: int
) -> None:
    """Persist TTL index metadata for a collection field index.

    Ensures ``_neosqlite_ttl_indexes`` exists and inserts or replaces the
    expiration policy row for ``index_name``.

    Args:
        db: SQLite database connection.
        index_name: Generated SQLite index name.
        collection_name: Name of the collection owning the index.
        field: Document field path governed by the TTL index.
        expire: Expiration duration in seconds.
    """
    _ensure_ttl_table(db)
    db.execute(
        f"INSERT OR REPLACE INTO {_TTL_META_TABLE} "
        "(index_name, collection_name, field, expire_after_seconds) "
        "VALUES (?, ?, ?, ?)",
        (index_name, collection_name, field, expire),
    )


def _load_ttl_option(db, index_name: str) -> int | None:
    """Load the ``expireAfterSeconds`` value stored for an index.

    Returns ``None`` if the metadata table does not exist, no entry matches
    ``index_name``, or the stored value cannot be parsed as an integer.

    Args:
        db: SQLite database connection.
        index_name: SQLite index name to look up.

    Returns:
        int | None: Stored expiration duration in seconds, or ``None``.
    """
    try:
        row = db.execute(
            f"SELECT expire_after_seconds FROM {_TTL_META_TABLE} "
            "WHERE index_name = ?",
            (index_name,),
        ).fetchone()
    except Exception:
        return None
    if row is None:
        return None
    try:
        return int(row[0])
    except (TypeError, ValueError):
        return None


def _delete_ttl_option(db, index_name: str) -> None:
    """Remove any stored TTL metadata entry for ``index_name``.

    Suppresses database errors when the metadata table has not been created.

    Args:
        db: SQLite database connection.
        index_name: SQLite index name whose TTL entry should be deleted.
    """
    try:
        db.execute(
            f"DELETE FROM {_TTL_META_TABLE} WHERE index_name = ?",
            (index_name,),
        )
    except Exception:
        pass


def _validate_expire_after_seconds(value: Any) -> int:
    """Validate and return a MongoDB ``expireAfterSeconds`` value.

    Args:
        value: Candidate TTL expiration duration in seconds.

    Returns:
        int: Validated non-negative integer within ``_TTL_MAX_SECONDS``.

    Raises:
        TypeError: If ``value`` is a ``bool`` or not an ``int``.
        ValueError: If ``value`` is negative or exceeds ``_TTL_MAX_SECONDS``.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(
            "expireAfterSeconds must be an integer, "
            f"got {type(value).__name__}"
        )
    if value < 0 or value > _TTL_MAX_SECONDS:
        raise ValueError(
            "expireAfterSeconds must be within 0 and "
            f"{_TTL_MAX_SECONDS} inclusive, got {value}"
        )
    return value


def list_ttl_specs(db, collection_name: str | None = None) -> list[dict]:
    """List TTL index specifications stored in the metadata table.

    Queries ``_neosqlite_ttl_indexes`` for all collections or a single
    collection and skips rows whose expiration value is not an integer.

    Args:
        db: SQLite database connection.
        collection_name: Optional collection name to filter results. If
            ``None``, returns TTL specs across all collections.

    Returns:
        list[dict]: Dictionaries containing ``index_name``,
            ``collection_name``, ``field``, and ``expireAfterSeconds``.
    """
    try:
        if collection_name is None:
            rows = db.execute(
                f"SELECT index_name, collection_name, field, "
                f"expire_after_seconds FROM {_TTL_META_TABLE}"
            ).fetchall()
        else:
            rows = db.execute(
                f"SELECT index_name, collection_name, field, "
                f"expire_after_seconds FROM {_TTL_META_TABLE} "
                "WHERE collection_name = ?",
                (collection_name,),
            ).fetchall()
    except Exception:
        return []
    specs = []
    for index_name, coll, field, expire in rows:
        try:
            specs.append(
                {
                    "index_name": index_name,
                    "collection_name": coll,
                    "field": field,
                    "expireAfterSeconds": int(expire),
                }
            )
        except (TypeError, ValueError):
            continue
    return specs


def _ensure_index_keys_table(db) -> None:
    """Create the index key specification table if it does not exist.

    Ensures ``custom_name`` column exists on pre-existing tables as well.

    Args:
        db: SQLite database connection.
    """
    db.execute(
        f"CREATE TABLE IF NOT EXISTS {_INDEX_KEYS_TABLE} ("
        "index_name TEXT PRIMARY KEY, keys_json TEXT NOT NULL, custom_name TEXT)"
    )
    try:
        cols = {
            row[1]
            for row in db.execute(
                f"PRAGMA table_info({_INDEX_KEYS_TABLE})"
            ).fetchall()
        }
        if cols and "custom_name" not in cols:
            db.execute(
                f"ALTER TABLE {_INDEX_KEYS_TABLE} ADD COLUMN custom_name TEXT"
            )
    except Exception:
        pass


def _normalize_direction(direction: Any) -> int:
    """Normalize an index sort direction specification to ``1`` or ``-1``."""
    if direction in (-1, "desc", "DESC"):
        return -1
    return 1


def _store_index_keys(
    db,
    index_name: str,
    keys: list[str] | list[tuple[str, Any]],
    custom_name: str | None = None,
) -> None:
    """Record the real key specification (with directions) for an index (#158, #189).

    Stores JSON pairs ``[[field, direction], ...]`` so callers can recover both
    unmangled dotted/underscored field names and ``ASC``/``DESC`` directions.

    Args:
        db: SQLite database connection.
        index_name: Generated SQLite index or FTS table name.
        keys: Ordered list of field names or ``(field, direction)`` tuples.
        custom_name: Optional user-provided custom index name.
    """
    encoded: list[list[Any]] = []
    for item in keys:
        if isinstance(item, tuple):
            encoded.append([str(item[0]), _normalize_direction(item[1])])
        else:
            encoded.append([str(item), 1])
    _ensure_index_keys_table(db)
    db.execute(
        f"INSERT OR REPLACE INTO {_INDEX_KEYS_TABLE} "
        f"(index_name, keys_json, custom_name) VALUES (?, ?, ?)",
        (index_name, _json.dumps(encoded), custom_name),
    )


def _delete_index_keys(db, index_name: str) -> None:
    """Remove any stored key specification row for ``index_name``."""
    try:
        db.execute(
            f"DELETE FROM {_INDEX_KEYS_TABLE} WHERE index_name = ?",
            (index_name,),
        )
    except Exception:
        pass


def _load_index_key_specs(db, index_name: str) -> list[tuple[str, int]] | None:
    """Load stored ``(field, direction)`` key specifications for ``index_name``.

    Supports both current ``[[field, direction], ...]`` entries and legacy
    ``["field", ...]`` entries.

    Args:
        db: SQLite database connection.
        index_name: SQLite index name to look up.

    Returns:
        list[tuple[str, int]] | None: Ordered ``(field, direction)`` tuples, or
        ``None`` when missing/invalid.
    """
    try:
        row = db.execute(
            f"SELECT keys_json FROM {_INDEX_KEYS_TABLE} WHERE index_name = ?",
            (index_name,),
        ).fetchone()
    except Exception:
        return None  # table not created yet / mocked db
    if row is None:
        return None
    try:
        raw = _json.loads(row[0])
    except Exception:
        return None
    if not isinstance(raw, list):
        return None
    specs: list[tuple[str, int]] = []
    for item in raw:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            specs.append((str(item[0]), _normalize_direction(item[1])))
        elif isinstance(item, str):
            specs.append((item, 1))
    return specs or None


def _load_index_keys(db, index_name: str) -> list[str] | None:
    """Load the stored field key name list for ``index_name``.

    Returns ``None`` if the metadata table does not exist, the index has no
    entry, or the stored JSON is invalid or not a list.

    Args:
        db: SQLite database connection.
        index_name: SQLite index name to look up.

    Returns:
        list[str] | None: Stored list of indexed field names, or ``None``.
    """
    specs = _load_index_key_specs(db, index_name)
    if specs is None:
        return None
    return [field for field, _ in specs]


def _load_custom_index_name(db, index_name: str) -> str | None:
    """Load an optional custom index name recorded for ``index_name``."""
    try:
        row = db.execute(
            f"SELECT custom_name FROM {_INDEX_KEYS_TABLE} WHERE index_name = ?",
            (index_name,),
        ).fetchone()
    except Exception:
        return None
    if row is None or not isinstance(row[0], str) or not row[0]:
        return None
    return row[0]


def _escape_glob(segment: str) -> str:
    """Escape SQLite GLOB metacharacters in a JSON field segment."""
    return segment.replace("[", "[[]").replace("*", "[*]").replace("?", "[?]")


def _fts_fullkey_predicate(field: str) -> str:
    """Build a SQLite built-in predicate matching nested ``t.fullkey`` paths.

    Matches both nested object paths (``$.author.name``) and array-expanded
    intermediate or terminal elements (``$.author[0].name``,
    ``$.reviews[0].comments[1]``) without matching unrelated sibling paths.
    """
    parts = [_escape_glob(part).replace("'", "''") for part in field.split(".")]
    dot_count = len(parts)
    ordered_glob = "$." + "*.".join(parts) + "*"
    clauses = [
        f"(length(t.fullkey) - length(replace(t.fullkey, '.', '')) = {dot_count})",
        f"(t.fullkey GLOB '$.{parts[0]}.*' OR t.fullkey GLOB '$.{parts[0]}[[][0-9]*[]].*')",
        f"t.fullkey GLOB '{ordered_glob}'",
    ]
    clauses.extend(
        f"(t.fullkey GLOB '*.{seg}.*' OR t.fullkey GLOB '*.{seg}[[][0-9]*[]].*')"
        for seg in parts[1:-1]
    )
    last = parts[-1]
    clauses.append(
        f"(t.fullkey GLOB '*.{last}' OR t.fullkey GLOB '*.{last}[[][0-9]*[]]')"
    )
    return " AND ".join(clauses)


def _extract_index_columns_clause(idx_sql: str) -> str:
    """Extract the parenthesized column list from a ``CREATE INDEX`` DDL."""
    on_match = re.search(r"\bON\b", idx_sql, re.IGNORECASE)
    start_search = on_match.end() if on_match else 0
    open_idx = idx_sql.find("(", start_search)
    if open_idx < 0:
        return ""
    depth = 0
    in_single = False
    in_double = False
    for idx in range(open_idx, len(idx_sql)):
        ch = idx_sql[idx]
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif not in_single and not in_double:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    return idx_sql[open_idx + 1 : idx]
    return ""


def _split_top_level_commas(expr: str) -> list[str]:
    """Split a SQL expression list by commas outside parentheses and quotes."""
    parts: list[str] = []
    depth = 0
    in_single = False
    in_double = False
    start = 0
    for idx, ch in enumerate(expr):
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif not in_single and not in_double:
            if ch == "(":
                depth += 1
            elif ch == ")" and depth > 0:
                depth -= 1
            elif ch == "," and depth == 0:
                parts.append(expr[start:idx].strip())
                start = idx + 1
    tail = expr[start:].strip()
    if tail:
        parts.append(tail)
    return parts


def _parse_index_sql_keys(idx_sql: str) -> dict[str, int]:
    """Fallback parser extracting key patterns and directions from ``idx_sql``."""
    cols_clause = _extract_index_columns_clause(idx_sql)
    if not cols_clause:
        return {}
    result: dict[str, int] = {}
    for col_expr in _split_top_level_commas(cols_clause):
        direction = (
            -1 if re.search(r"\bDESC\s*$", col_expr, re.IGNORECASE) else 1
        )
        json_match = re.search(
            r"(?:json|jsonb)_extract\(data,\s*'(\$\..*?)'\)", col_expr
        )
        if json_match:
            field_path = json_match.group(1).removeprefix("$.")
            if field_path.endswith(".$date"):
                field_path = field_path.removesuffix(".$date")
            result[field_path] = direction
        elif re.search(r'(?:^|[(,\s])"?_id"?\b', col_expr):
            result["_id"] = direction
    return result


class IndexManager:
    """Manages indexes for a NeoSQLite collection.

    Provides functionality to create, list, drop, and inspect single-field,
    compound, unique, sparse, FTS5 full-text search, UTC datetime, and TTL
    metadata indexes on the underlying SQLite table.
    """

    def __init__(self, collection):
        """Initialize the IndexManager with a collection instance.

        Args:
            collection: The NeoSQLite collection instance whose indexes are
                managed.
        """
        self.collection = collection
        # Initialize JSONB capabilities
        self.jsonb = JSONBContext.from_db(collection.db)

    def create_index(
        self,
        key: str | list[str] | list[tuple[str, int]],
        reindex: bool = True,
        sparse: bool = False,
        unique: bool = False,
        fts: bool = False,
        tokenizer: str | None = None,
        datetime_field: bool = False,
        expireAfterSeconds: int | None = None,
        name: str | None = None,
        **kwargs: Any,
    ) -> str:
        """Create an index on the specified key(s) for this collection.

        Handles single-field, compound, unique, sparse, FTS5 text, datetime,
        and TTL indexes using SQLite ``json_extract`` or ``jsonb_extract``
        expressions and virtual FTS5 tables.

        Args:
            key: Field name, single-element list, or list of PyMongo key
                tuples (such as ``[("field", 1)]`` or ``[("field", "text")]``)
                specifying the field(s) to index.
            reindex: Kept for API compatibility; SQLite indexes update
                automatically.
            sparse: If True, create a partial index excluding rows where the
                field extracts to ``NULL`` (applies to string ``key``).
            unique: If True, enforce uniqueness on the indexed expression(s).
            fts: If True, create an FTS5 full-text search index instead of a
                B-tree index.
            tokenizer: Optional FTS5 tokenizer name (for example, ``"icu"`` or
                ``"icu_th"``).
            datetime_field: If True, create a ``datetime()``-normalized UTC
                index for a single string ``key``.
            expireAfterSeconds: Optional TTL in seconds (PyMongo-compatible
                declaration) stored in ``_neosqlite_ttl_indexes`` for single-
                field non-FTS, non-datetime indexes.
            name: Optional custom index name accepted for PyMongo
                compatibility; the SQLite index name is still auto-generated.
            **kwargs: Additional PyMongo index options (for example,
                ``background``), accepted and ignored.

        Returns:
            str: The generated SQLite index or FTS virtual table name.

        Raises:
            ValueError: If ``expireAfterSeconds`` is combined with FTS,
                datetime, or compound indexes, if ``datetime_field`` uses a
                non-string key, or if a compound key format is invalid.
        """
        try:
            self.collection.__dict__["_ttl_specs_cache"] = None
        except Exception:
            pass
        expire: int | None = None
        if expireAfterSeconds is not None:
            expire = _validate_expire_after_seconds(expireAfterSeconds)
        if name is not None:
            logger.debug(
                "Custom index name %r accepted but SQLite name "
                "is auto-generated",
                name,
            )
        if kwargs:
            logger.debug("Ignoring extra index options: %r", sorted(kwargs))
        if expire is not None and (fts or datetime_field):
            raise ValueError(
                "expireAfterSeconds requires a single-field "
                "non-FTS, non-datetime index"
            )
        # For datetime fields, use special indexing.
        if datetime_field:
            if isinstance(key, str):
                self._create_datetime_index(
                    key, unique=unique, sparse=sparse, custom_name=name
                )
                coll_name = validate_identifier(self.collection.name)
                index_name_full = f"idx_{coll_name}_{key.replace('.', '_')}_utc"
                return index_name_full
            else:
                raise ValueError("Compound datetime indexes are not supported")
        elif (
            isinstance(key, list)
            and key
            and all(
                isinstance(k, tuple) and len(k) == 2 and k[1] == "text"
                for k in key
            )
        ):
            # Handle MongoDB text index format: [("field", "text"), ...]
            coll_name = validate_identifier(self.collection.name)
            created_fts = []
            for item in key:
                if isinstance(item, tuple):
                    field = str(item[0])
                    self._create_fts_index(field, tokenizer)
                    created_fts.append(
                        f"{coll_name}_{field.replace('.', '_')}_fts"
                    )
            return (
                created_fts[0]
                if len(created_fts) == 1
                else "_".join(created_fts)
            )
        elif isinstance(key, str):
            coll_name = validate_identifier(self.collection.name)
            if fts:
                # Create FTS index with optional tokenizer
                self._create_fts_index(key, tokenizer)
                return f"{coll_name}_{key.replace('.', '_')}_fts"
            return self._create_btree_index(
                [(key, 1)],
                unique=unique,
                sparse=sparse,
                expire=expire,
                custom_name=name,
            )
        else:
            pairs: list[tuple[str, Any]]
            if isinstance(key, list) and key:
                if len(key) == 1 and isinstance(key[0], str):
                    pairs = [(key[0], 1)]
                elif all(isinstance(k, tuple) and len(k) == 2 for k in key):
                    pairs = [(str(k[0]), k[1]) for k in key]
                else:
                    raise ValueError(
                        "Compound indexes must use PyMongo tuple format: "
                        f'[("field1", 1), ("field2", -1)]. Got: {key}'
                    )
            else:
                raise ValueError(
                    "Compound indexes must use PyMongo tuple format: "
                    f'[("field1", 1), ("field2", -1)]. Got: {key}'
                )

            if len(pairs) > 1 and expire is not None:
                raise ValueError(
                    "expireAfterSeconds requires a single-field index; "
                    "compound indexes do not support TTL"
                )
            return self._create_btree_index(
                pairs,
                unique=unique,
                sparse=sparse,
                expire=expire,
                custom_name=name,
            )

    def _index_column_expr(self, field: str) -> str:
        """Return the SQLite column or JSON extract expression for ``field``."""
        if field == "_id":
            # _id lives in a dedicated column, not inside data (#113, #189)
            return quote_identifier("_id")
        func_prefix = self.jsonb.json_function_prefix
        return f"{func_prefix}_extract(data, '{parse_json_path(field)}')"

    def _index_column_sql(self, field: str, direction: Any) -> str:
        """Return the SQLite index column definition with ASC/DESC ordering."""
        order = "DESC" if _normalize_direction(direction) == -1 else "ASC"
        return f"{self._index_column_expr(field)} {order}"

    def _create_btree_index(
        self,
        pairs: list[tuple[str, Any]],
        *,
        unique: bool = False,
        sparse: bool = False,
        expire: int | None = None,
        custom_name: str | None = None,
    ) -> str:
        """Create a single-field or compound B-tree index in SQLite."""
        coll_name = validate_identifier(self.collection.name)
        fields = [field for field, _ in pairs]
        index_name = "_".join(fields).replace(".", "_")
        index_name_full = f"idx_{coll_name}_{index_name}"
        index_columns = ", ".join(
            self._index_column_sql(field, direction)
            for field, direction in pairs
        )
        partial = ""
        if sparse:
            non_null_terms = " OR ".join(
                f"{self._index_column_expr(field)} IS NOT NULL"
                for field, _ in pairs
            )
            partial = f" WHERE {non_null_terms}"
        self.collection.db.execute(
            f"CREATE {'UNIQUE ' if unique else ''}INDEX "
            f"IF NOT EXISTS {quote_identifier(index_name_full)} "
            f"ON {quote_table_name(coll_name)}({index_columns}){partial}"
        )
        _store_index_keys(
            self.collection.db,
            index_name_full,
            pairs,
            custom_name=custom_name,
        )
        if expire is not None:
            _store_ttl_option(
                self.collection.db,
                index_name_full,
                coll_name,
                fields[0],
                expire,
            )
        return index_name_full

    @staticmethod
    def _validate_fts_tokenizer(tokenizer: str) -> str:
        """Validate an FTS5 tokenizer specification against a safe grammar."""
        if not isinstance(tokenizer, str):
            raise ValueError(
                f"Invalid FTS5 tokenizer specification: {tokenizer!r}"
            )
        stripped = tokenizer.strip()
        if not stripped or any(
            bad in stripped
            for bad in (
                "\x00",
                "\r",
                "\n",
                ";",
                "(",
                ")",
                "'",
                "--",
                "/*",
                "*/",
            )
        ):
            raise ValueError(
                f"Invalid FTS5 tokenizer specification: {tokenizer!r}"
            )
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", stripped):
            return stripped
        if re.fullmatch(
            r'[A-Za-z_][A-Za-z0-9_]*(?:\s+(?:[A-Za-z0-9_.\-+]+|"[^"\\]*"))+',
            stripped,
        ):
            return f"'{stripped}'"
        raise ValueError(f"Invalid FTS5 tokenizer specification: {tokenizer!r}")

    def _create_fts_index(self, field: str, tokenizer: str | None = None):
        """Create an FTS5 virtual table and sync triggers for ``field``.

        Creates ``{collection}_{field}_fts``, populates it from existing rows
        (using ``json_extract`` for top-level fields or ``json_tree`` /
        ``jsonb_tree`` for nested or array paths), records the key metadata,
        and installs ``INSERT``, ``UPDATE``, and ``DELETE`` triggers.

        Args:
            field: Document field path to index with FTS5.
            tokenizer: Optional FTS5 tokenizer specification (for example,
                ``"icu"``).
        """
        # Create index name (replace dots with underscores for valid identifiers)
        coll_name = validate_identifier(self.collection.name)
        index_name = validate_identifier(field.replace(".", "_"))
        fts_raw_name = f"{coll_name}_{index_name}_fts"
        fts_table_name = quote_identifier(fts_raw_name)
        quoted_coll = quote_table_name(coll_name)
        quoted_col = quote_identifier(index_name)

        # Create FTS table with optional tokenizer
        # Note: We don't use the 'content' option because we manage the FTS data manually
        # to properly support array fields with json_tree()
        if tokenizer is not None and tokenizer != "":
            validated_tokenizer = self._validate_fts_tokenizer(tokenizer)
            tokenizer_clause = f", TOKENIZE={validated_tokenizer}"
        elif tokenizer == "":
            raise ValueError("Invalid FTS5 tokenizer specification: ''")
        else:
            tokenizer_clause = ""
        self.collection.db.execute(f"""
            CREATE VIRTUAL TABLE IF NOT EXISTS {fts_table_name}
            USING FTS5({quoted_col}{tokenizer_clause})
            """)
        # Record the real field so as_keys/optimizer need not guess from
        # the underscore-mangled name (#158)
        _store_index_keys(self.collection.db, fts_raw_name, [field])

        # For nested fields with arrays, use json_tree() to index all matching values
        # Convert field path to json_tree fullkey pattern (e.g., "comments.text" -> "$.comments[*].text")
        field_parts = field.split(".")
        if len(field_parts) == 1:
            # Simple field - use direct json_extract
            json_path = parse_json_path(field)
            self.collection.db.execute(f"""
                INSERT INTO {fts_table_name}(rowid, {quoted_col})
                SELECT id, lower(json_extract(data, '{json_path}'))
                FROM {quoted_coll}
                WHERE json_extract(data, '{json_path}') IS NOT NULL
                """)
        else:
            # Nested field - match exact path via t.fullkey (supporting array elements)
            json_tree_func = self.jsonb.json_tree_function
            fullkey_pred = _fts_fullkey_predicate(field)
            self.collection.db.execute(f"""
                INSERT INTO {fts_table_name}(rowid, {quoted_col})
                SELECT
                    r.id,
                    group_concat(lower(t.value), ' ')
                FROM {quoted_coll} r
                JOIN {json_tree_func}(r.data) t
                WHERE {fullkey_pred} AND t.type = 'text'
                GROUP BY r.id
                """)

        # Create triggers to keep FTS table in sync with main table
        # For array fields, we need to delete all entries for the rowid and re-insert all values
        trig_insert = quote_identifier(f"{coll_name}_{index_name}_fts_insert")
        trig_update = quote_identifier(f"{coll_name}_{index_name}_fts_update")
        trig_delete = quote_identifier(f"{coll_name}_{index_name}_fts_delete")

        # Insert trigger - delete any existing entries and insert all matching values
        if len(field_parts) == 1:
            # Simple field
            json_path = parse_json_path(field)
            self.collection.db.execute(f"""
                CREATE TRIGGER IF NOT EXISTS {trig_insert}
                AFTER INSERT ON {quoted_coll}
                BEGIN
                    DELETE FROM {fts_table_name} WHERE rowid = new.id;
                    INSERT INTO {fts_table_name}(rowid, {quoted_col})
                    VALUES (new.id, lower(json_extract(new.data, '{json_path}')));
                END
                """)
        else:
            # Nested field - use json_tree/jsonb_tree with exact fullkey match
            json_tree_func = self.jsonb.json_tree_function
            fullkey_pred = _fts_fullkey_predicate(field)
            self.collection.db.execute(f"""
                CREATE TRIGGER IF NOT EXISTS {trig_insert}
                AFTER INSERT ON {quoted_coll}
                BEGIN
                    DELETE FROM {fts_table_name} WHERE rowid = new.id;
                    INSERT INTO {fts_table_name}(rowid, {quoted_col})
                    SELECT
                        new.id,
                        group_concat(lower(t.value), ' ')
                    FROM {json_tree_func}(new.data) t
                    WHERE {fullkey_pred} AND t.type = 'text'
                    GROUP BY new.id;
                END
                """)

        # Update trigger - delete old entries and insert new ones
        if len(field_parts) == 1:
            # Simple field
            json_path = parse_json_path(field)
            self.collection.db.execute(f"""
                CREATE TRIGGER IF NOT EXISTS {trig_update}
                AFTER UPDATE ON {quoted_coll}
                BEGIN
                    DELETE FROM {fts_table_name} WHERE rowid = old.id;
                    INSERT INTO {fts_table_name}(rowid, {quoted_col})
                    VALUES (new.id, lower(json_extract(new.data, '{json_path}')));
                END
                """)
        else:
            # Nested field - use json_tree/jsonb_tree with exact fullkey match
            json_tree_func = self.jsonb.json_tree_function
            fullkey_pred = _fts_fullkey_predicate(field)
            self.collection.db.execute(f"""
                CREATE TRIGGER IF NOT EXISTS {trig_update}
                AFTER UPDATE ON {quoted_coll}
                BEGIN
                    DELETE FROM {fts_table_name} WHERE rowid = old.id;
                    INSERT INTO {fts_table_name}(rowid, {quoted_col})
                    SELECT
                        new.id,
                        group_concat(lower(t.value), ' ')
                    FROM {json_tree_func}(new.data) t
                    WHERE {fullkey_pred} AND t.type = 'text'
                    GROUP BY new.id;
                END
                """)

        # Delete trigger - remove all entries for the deleted row
        self.collection.db.execute(f"""
            CREATE TRIGGER IF NOT EXISTS {trig_delete}
            AFTER DELETE ON {quoted_coll}
            BEGIN
                DELETE FROM {fts_table_name} WHERE rowid = old.id;
            END
            """)

    def create_indexes(
        self,
        indexes: list["IndexModel"],
    ) -> list[str]:
        """Create multiple indexes at once from ``IndexModel`` definitions.

        Accepts a list of ``IndexModel`` objects matching the PyMongo API,
        translates each model's ``key`` and options, and delegates index
        creation to :meth:`create_index`.

        Args:
            indexes: List of ``IndexModel`` objects describing indexes to
                create.

        Returns:
            list[str]: Names of the created indexes.

        Raises:
            ValueError: If an ``IndexModel`` contains an empty or unsupported
                ``key`` specification.
        """
        created_indexes = []
        for index_model in indexes:
            doc = index_model.document
            key = doc.get("key")
            if key is None:
                continue

            unique: bool = bool(doc.get("unique", False))
            sparse: bool = bool(doc.get("sparse", False))
            fts: bool = bool(doc.get("fts", False))
            tokenizer: str | None = doc.get("tokenizer")
            datetime_field: bool = bool(doc.get("datetime_field", False))
            expire_after: Any = doc.get("expireAfterSeconds")
            custom_name: Any = doc.get("name")
            extra: dict[str, Any] = {
                k: v
                for k, v in doc.items()
                if k
                not in {
                    "key",
                    "unique",
                    "sparse",
                    "fts",
                    "tokenizer",
                    "datetime_field",
                    "expireAfterSeconds",
                    "name",
                }
            }

            def _make_opts() -> dict[str, Any]:
                """Build keyword arguments for :meth:`create_index`.

                Returns:
                    dict[str, Any]: Index creation options assembled from the
                        current ``IndexModel`` document.
                """
                opts: dict[str, Any] = {
                    "unique": unique,
                    "sparse": sparse,
                    "fts": fts,
                    "tokenizer": tokenizer,
                    "datetime_field": datetime_field,
                }
                if expire_after is not None:
                    opts["expireAfterSeconds"] = expire_after
                if custom_name is not None:
                    opts["name"] = custom_name
                opts.update(extra)
                return opts

            # Convert key specification to the format expected by create_index
            match key:
                case dict():
                    if not key:
                        raise ValueError("Index key cannot be empty")
                    tuple_key = [(f, d) for f, d in key.items()]
                    returned = self.create_index(tuple_key, **_make_opts())
                case str():
                    if not key:
                        raise ValueError("Index key cannot be empty")
                    returned = self.create_index(key, **_make_opts())
                case [str()]:
                    returned = self.create_index(key[0], **_make_opts())
                case list():
                    if not key:
                        raise ValueError("Index key cannot be empty")
                    if all(isinstance(k, tuple) and len(k) == 2 for k in key):
                        returned = self.create_index(key, **_make_opts())
                    else:
                        raise ValueError(f"Invalid key specification: {key}")
                case _:
                    raise ValueError(f"Invalid key specification: {key}")

            created_indexes.append(returned)

        return created_indexes

    def reindex(
        self,
        table: str,
        sparse: bool = False,
        documents: list[dict[str, Any]] | None = None,
    ):
        """Reindex the collection (no-op for native SQLite JSON indexes).

        With native SQLite JSON indexing, indexes are maintained automatically
        by SQLite. Kept for API compatibility.

        Args:
            table: Table name (unused in this implementation).
            sparse: Whether the index is sparse (unused).
            documents: Optional list of documents to reindex (unused).
        """
        # With native JSON indexing, reindexing is handled automatically by SQLite
        # This method is kept for API compatibility but does nothing
        pass

    @overload
    def list_indexes(self, as_keys: Literal[True]) -> list[list[str]]:
        """Retrieve stored key specifications for the collection's indexes.

        Args:
            as_keys: Must be ``True`` to return lists of field names.

        Returns:
            list[list[str]]: List of indexed field-name lists per index.
        """
        ...

    @overload
    def list_indexes(self, as_keys: Literal[False] = False) -> list[str]:
        """Retrieve SQLite index names for the collection.

        Args:
            as_keys: ``False`` to return SQLite index names.

        Returns:
            list[str]: List of index names belonging to this collection.
        """
        ...

    def list_indexes(
        self,
        as_keys: bool = False,
    ) -> list[str] | list[list[str]]:
        """Retrieve indexes or indexed field keys for the collection.

        Excludes the hidden automatic ``_id`` index. When ``as_keys`` is True,
        uses stored key metadata when available and otherwise falls back to
        converting underscores to dots in the index name suffix.

        Args:
            as_keys: If True, return lists of field key names instead of raw
                SQLite index names.

        Returns:
            list[str] | list[list[str]]: List of SQLite index names when
                ``as_keys`` is False, or list of field-name lists when
                ``as_keys`` is True.
        """
        # Get indexes that match our naming convention and belong to this table
        coll_name = validate_identifier(self.collection.name)
        cmd = "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name = ? AND name LIKE ? ESCAPE '\\'"
        escaped_name = (
            coll_name.replace("\\", "\\\\")
            .replace("%", "\\%")
            .replace("_", "\\_")
        )
        like_pattern = f"idx_{escaped_name}\\_%"
        id_index_name = f"idx_{coll_name}_id"
        if as_keys:
            # Extract key names from index names
            indexes = self.collection.db.execute(
                cmd, (coll_name, like_pattern)
            ).fetchall()
            result = []
            for idx in indexes:
                # Skip the automatically created _id index since it should be hidden
                # like MongoDB's automatic _id index
                if idx[0] == id_index_name:
                    continue
                stored = _load_index_keys(self.collection.db, idx[0])
                if stored:
                    result.append(list(stored))
                    continue
                # Legacy fallback: derive from the (lossy) index name
                key_name = idx[0][len(f"idx_{coll_name}_") :]
                # Convert underscores back to dots for nested keys
                key_name = key_name.replace("_", ".")
                result.append([key_name])
            return result
        # Return index names
        all_indexes = [
            idx[0]
            for idx in self.collection.db.execute(
                cmd, (coll_name, like_pattern)
            ).fetchall()
        ]
        # Filter out the automatically created _id index since it should be hidden
        # like MongoDB's automatic _id index
        filtered_indexes = [
            idx_name for idx_name in all_indexes if idx_name != id_index_name
        ]
        return filtered_indexes

    def drop_index(self, index: str):
        """Drop an index from the collection and clean up its metadata.

        Resolves candidate SQLite index names from a full index name, field
        name (with optional ``_1``/``_-1`` suffix), or compound field list,
        drops the index if it exists, and deletes stored key and TTL metadata.

        Args:
            index: Index name, field name, or sequence of compound field names
                identifying the index to drop.
        """
        try:
            self.collection.__dict__["_ttl_specs_cache"] = None
        except Exception:
            pass
        coll_name = validate_identifier(self.collection.name)
        cursor = self.collection.db.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=?",
            (coll_name,),
        )
        existing = {row[0] for row in cursor.fetchall()}

        candidates = []
        if isinstance(index, str):
            candidates.append(index)
            base = index.replace(".", "_")
            candidates.append(f"idx_{coll_name}_{base}")
            if base.endswith("_1"):
                candidates.append(f"idx_{coll_name}_{base[:-2]}")
            elif base.endswith("_-1"):
                candidates.append(f"idx_{coll_name}_{base[:-3]}")
        else:
            base = "_".join(index).replace(".", "_")
            candidates.append(f"idx_{coll_name}_{base}")

        full_name = None
        for cand in candidates:
            if cand in existing:
                full_name = cand
                break
        if full_name is None:
            full_name = candidates[1] if len(candidates) > 1 else candidates[0]

        self.collection.db.execute(
            f"DROP INDEX IF EXISTS {quote_identifier(full_name)}"
        )
        # Remove the stored key spec (#158) and TTL metadata
        try:
            self.collection.db.execute(
                f"DELETE FROM {_INDEX_KEYS_TABLE} WHERE index_name = ?",
                (full_name,),
            )
        except Exception:
            pass
        _delete_ttl_option(self.collection.db, full_name)

    def drop_indexes(self):
        """Drop all user-created indexes associated with this collection.

        Clears the collection's cached TTL specs, drops each index returned by
        :meth:`list_indexes`, and removes associated TTL metadata entries.
        """
        try:
            self.collection.__dict__["_ttl_specs_cache"] = None
        except Exception:
            pass
        indexes = self.list_indexes()
        for index in indexes:
            # Extract the actual index name from the full name
            self.collection.db.execute(
                f"DROP INDEX IF EXISTS {quote_identifier(index)}"
            )
            _delete_index_keys(self.collection.db, index)
            _delete_ttl_option(self.collection.db, index)

    def index_information(self) -> dict[str, Any]:
        """Retrieve metadata describing this collection's indexes.

        Inspects ``sqlite_master`` for collection indexes (excluding the
        automatic ``_id`` index) and extracts version, uniqueness, key mapping
        (preserving directions and explicit ``_id`` keys), and any stored
        ``expireAfterSeconds`` TTL option.

        Returns:
            dict[str, Any]: Mapping from index name to its metadata dictionary.
        """
        info: dict[str, Any] = {}

        try:
            coll_name = validate_identifier(self.collection.name)
            # Get all indexes for this collection
            indexes = self.collection.db.execute(
                "SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name=?",
                (coll_name,),
            ).fetchall()

            for idx_name, idx_sql in indexes:
                # Skip the automatically created _id index since it should be hidden
                # like MongoDB's automatic _id index
                if idx_name == f"idx_{coll_name}_id":
                    continue

                # Parse the index information
                index_info: dict[str, Any] = {
                    "v": 2,  # Index version
                }

                # Check if it's a unique index
                if idx_sql and "UNIQUE" in idx_sql.upper():
                    index_info["unique"] = True
                else:
                    index_info["unique"] = False

                key_specs = _load_index_key_specs(self.collection.db, idx_name)
                if key_specs:
                    index_info["key"] = {
                        field: direction for field, direction in key_specs
                    }
                elif idx_sql:
                    parsed_keys = _parse_index_sql_keys(idx_sql)
                    if parsed_keys:
                        index_info["key"] = parsed_keys

                expire_opt = _load_ttl_option(self.collection.db, idx_name)
                if expire_opt is not None:
                    index_info["expireAfterSeconds"] = expire_opt

                info[idx_name] = index_info

        except sqlite3.Error as e:
            logger.debug(
                f"Failed to get index information for collection '{self.collection.name}': {e}"
            )
            pass

        return info

    def get_ttl_specs(self) -> list[dict]:
        """List TTL index specifications declared on this collection.

        Returns:
            list[dict]: Dictionaries with ``index_name``, ``collection_name``,
                ``field``, and ``expireAfterSeconds`` keys.
        """
        return list_ttl_specs(self.collection.db, self.collection.name)

    def create_search_index(
        self,
        key: str,
        tokenizer: str | None = None,
    ):
        """Create an FTS5 search index on ``key`` for full-text search.

        Convenience wrapper equivalent to calling
        ``create_index(key, fts=True, tokenizer=tokenizer)``.

        Args:
            key: Document field path to index for text search.
            tokenizer: Optional FTS5 tokenizer name (for example, ``"icu"``).

        Returns:
            str: Name of the created FTS virtual table.
        """
        return self.create_index(key, fts=True, tokenizer=tokenizer)

    def create_search_indexes(
        self,
        indexes: list[str],
    ) -> list[str]:
        """Create multiple FTS5 search indexes for text search.

        Calls ``create_index(key, fts=True)`` for each field in ``indexes``.

        Args:
            indexes: List of field paths to index for full-text search.

        Returns:
            list[str]: Formatted names of the search indexes created.
        """
        coll_name = validate_identifier(self.collection.name)
        created_indexes = []
        for key in indexes:
            # Call create_index with fts=True for each key
            self.create_index(key, fts=True)
            # Generate the index name the same way as in IndexManager
            index_name = key.replace(".", "_")
            created_indexes.append(f"idx_{coll_name}_{index_name}")
        return created_indexes

    def list_search_indexes(self) -> list[str]:
        """List all FTS5 search index field names for the collection.

        Identifies FTS virtual tables belonging exclusively to this collection
        via their ``ON <collection>`` synchronization triggers in
        ``sqlite_master``, then resolves unmangled field paths via
        ``_load_index_keys``.

        Returns:
            list[str]: Indexed field paths for each search index on this
                collection.
        """
        coll_name = validate_identifier(self.collection.name)
        trig_rows = self.collection.db.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='trigger' AND tbl_name = ? "
            "AND name LIKE '%\\_fts\\_insert' ESCAPE '\\' ORDER BY rowid",
            (coll_name,),
        ).fetchall()
        if not trig_rows:
            return []

        existing_tables = {
            row[0]
            for row in self.collection.db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }

        prefix = f"{coll_name}_"
        suffix = "_fts"
        search_indexes: list[str] = []
        seen: set[str] = set()
        for (trig_name,) in trig_rows:
            fts_table_name = trig_name.removesuffix("_insert")
            if fts_table_name not in existing_tables or fts_table_name in seen:
                continue
            seen.add(fts_table_name)
            stored = _load_index_keys(self.collection.db, fts_table_name)
            if stored:
                search_indexes.append(stored[0])
            elif fts_table_name.startswith(prefix) and fts_table_name.endswith(
                suffix
            ):
                search_indexes.append(
                    fts_table_name[len(prefix) : -len(suffix)]
                )

        return search_indexes

    def update_search_index(self, key: str, tokenizer: str | None = None):
        """Update a search index by dropping and recreating it.

        Allows changing the FTS5 tokenizer configuration or rebuilding the
        virtual table and triggers for ``key``.

        Args:
            key: Field name whose FTS5 search index should be recreated.
            tokenizer: Optional new FTS5 tokenizer name to apply.
        """
        # Drop the existing FTS index
        self.drop_search_index(key)

        # Create a new FTS index with the updated options
        self.create_search_index(key, tokenizer=tokenizer)

    def drop_search_index(self, index: str):
        """Drop an FTS5 search index and its synchronization triggers.

        Args:
            index: Field name identifying the search index to drop.
        """
        # For FTS indexes, we need to drop the FTS virtual table and its triggers
        # FTS tables have a specific naming pattern: {collection}_{field}_fts
        coll_name = validate_identifier(self.collection.name)
        index_name = validate_identifier(index.replace(".", "_"))
        fts_raw_name = f"{coll_name}_{index_name}_fts"
        fts_table_name = quote_identifier(fts_raw_name)

        # Drop the FTS table
        self.collection.db.execute(f"DROP TABLE IF EXISTS {fts_table_name}")
        _delete_index_keys(self.collection.db, fts_raw_name)

        # Drop the triggers associated with the FTS table
        self.collection.db.execute(
            f"DROP TRIGGER IF EXISTS {quote_identifier(f'{coll_name}_{index_name}_fts_insert')}"
        )
        self.collection.db.execute(
            f"DROP TRIGGER IF EXISTS {quote_identifier(f'{coll_name}_{index_name}_fts_update')}"
        )
        self.collection.db.execute(
            f"DROP TRIGGER IF EXISTS {quote_identifier(f'{coll_name}_{index_name}_fts_delete')}"
        )

    def _create_datetime_index(
        self,
        key: str,
        unique: bool = False,
        sparse: bool = False,
        custom_name: str | None = None,
    ):
        """Create a timezone-normalized UTC datetime index on ``key``.

        Indexes ``datetime(COALESCE(extract(..., '$.<path>.$date'),
        extract(..., '$.<path>')))`` under ``idx_{collection}_{key}_utc`` so
        both NeoSQLite ``{"$date": ...}`` objects and ISO strings evaluate
        properly and preserve ``unique=True``.

        Args:
            key: Field path to index (for example, ``"timestamp"`` or
                ``"user.created_at"``).
            unique: If True, enforce uniqueness on the normalized datetime
                values.
            sparse: If True, omit rows where the extracted datetime is ``NULL``.
            custom_name: Optional custom index name recorded in key metadata.
        """
        coll_name = validate_identifier(self.collection.name)
        column_name = f"{key.replace('.', '_')}_utc"
        idx_raw_name = f"idx_{coll_name}_{column_name}"
        idx_name = quote_identifier(idx_raw_name)

        func_prefix = self.jsonb.json_function_prefix
        json_path = parse_json_path(key)
        date_path = f"{json_path}.$date"
        dt_expr = (
            f"datetime(COALESCE("
            f"{func_prefix}_extract(data, '{date_path}'), "
            f"{func_prefix}_extract(data, '{json_path}')"
            f"))"
        )
        partial = f" WHERE {dt_expr} IS NOT NULL" if sparse else ""

        index_sql = (
            f"CREATE {'UNIQUE ' if unique else ''}INDEX IF NOT EXISTS "
            f"{idx_name} ON {quote_table_name(coll_name)}({dt_expr}){partial}"
        )
        self.collection.db.execute(index_sql)
        _store_index_keys(
            self.collection.db,
            idx_raw_name,
            [(key, 1)],
            custom_name=custom_name,
        )
