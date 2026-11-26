"""Tests for JSON containment SQL generation with JSONB value sources."""

import pytest

from neosqlite import Connection
from neosqlite.collection.type_utils import build_json_containment_parts


@pytest.mark.parametrize("each_function", ["json_each", "jsonb_each"])
def test_nested_object_containment_with_jsonb_each(each_function):
    """Object leaves extracted from JSONB arrays need jsonb(?) binding."""
    with Connection(":memory:") as conn:
        db = conn.db
        db.execute("CREATE TABLE t(data JSONB)")
        db.execute(
            "INSERT INTO t(data) VALUES (?)",
            ('{"items":[{"b":{"c":1}},{"b":{"c":2}}]}',),
        )
        _scalar_sql, _scalar_params, array_sql, array_params = (
            build_json_containment_parts(
                "data",
                "items.b",
                "jsonb_extract(data,'$.items.b')",
                {"c": 1},
                each_function,
                jsonb_supported=True,
            )
        )
        count = db.execute(
            f"SELECT COUNT(*) FROM t WHERE {array_sql}", array_params
        ).fetchone()[0]
        assert count == 1
