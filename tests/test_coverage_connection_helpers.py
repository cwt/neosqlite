"""Tests covering custom SQLite functions and helper routines on Connection."""

from __future__ import annotations

import json
import neosqlite


def test_custom_regex_functions_on_connection():
    """Verify regexp, regexp_find, regexp_find_all, and regexp_replace functions."""
    conn = neosqlite.Connection(":memory:")
    db = conn.db

    # 1. regexp
    assert db.execute("SELECT regexp('^ab', 'abc')").fetchone()[0] == 1
    assert db.execute("SELECT regexp('^ab', 'xyz')").fetchone()[0] == 0
    assert db.execute("SELECT regexp('^ab', NULL)").fetchone()[0] == 0

    # 2. regexp_find
    res = db.execute("SELECT regexp_find('([0-9]+)', 'val 123 end')").fetchone()[0]
    parsed = json.loads(res)
    assert parsed["match"] == "123"
    assert parsed["idx"] == 4
    assert parsed["captures"] == ["123"]

    assert db.execute("SELECT regexp_find('[0-9]+', 'no digits')").fetchone()[0] is None
    assert db.execute("SELECT regexp_find(NULL, 'test')").fetchone()[0] is None
    assert db.execute("SELECT regexp_find('test', NULL)").fetchone()[0] is None

    # 3. regexp_find_all
    res_all = db.execute("SELECT regexp_find_all('([a-z]+)', 'one two three')").fetchone()[0]
    parsed_all = json.loads(res_all)
    assert len(parsed_all) == 3
    assert [p["match"] for p in parsed_all] == ["one", "two", "three"]

    assert db.execute("SELECT regexp_find_all('[0-9]+', 'no digits')").fetchone()[0] == "[]"
    assert db.execute("SELECT regexp_find_all(NULL, 'test')").fetchone()[0] == "[]"
    assert db.execute("SELECT regexp_find_all('test', NULL)").fetchone()[0] == "[]"

    # 4. regexp_replace
    rep = db.execute("SELECT regexp_replace('hello world', 'world', 'neosqlite', 0)").fetchone()[0]
    assert rep == "hello neosqlite"

    # Replace with limited count
    rep_count = db.execute("SELECT regexp_replace('a a a', 'a', 'b', 1)").fetchone()[0]
    assert rep_count == "b a a"

    # None inputs
    assert db.execute("SELECT regexp_replace(NULL, 'a', 'b', 0)").fetchone()[0] is None
    assert db.execute("SELECT regexp_replace('hello', NULL, 'b', 0)").fetchone()[0] == "hello"
    assert db.execute("SELECT regexp_replace('hello', 'a', NULL, 0)").fetchone()[0] == "hello"
