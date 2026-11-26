"""Schema column cache invalidation for FTS tables on drop/rename."""

from neosqlite import Connection
from neosqlite.collection.schema_utils import (
    _table_columns_cache,
    get_table_columns_cached,
)


def test_drop_invalidates_fts_shadow_column_cache():
    with Connection(":memory:") as conn:
        coll = conn.articles
        coll.create_index([("body", "text")])
        fts_table = "articles_body_fts"
        for table in (fts_table, fts_table + "_data"):
            get_table_columns_cached(conn.db, table)
            assert (id(conn.db), table) in _table_columns_cache

        coll.drop()

        for table in (fts_table, fts_table + "_data"):
            assert (id(conn.db), table) not in _table_columns_cache


def test_rename_invalidates_fts_column_cache():
    with Connection(":memory:") as conn:
        coll = conn.articles
        coll.create_index([("body", "text")])
        fts_table = "articles_body_fts"
        get_table_columns_cached(conn.db, fts_table)
        assert (id(conn.db), fts_table) in _table_columns_cache

        coll.rename("posts")

        assert (id(conn.db), fts_table) not in _table_columns_cache
        assert (id(conn.db), "posts_body_fts") not in _table_columns_cache
