"""Regression test for #93: a nested transaction() failure used to roll
back the OUTER transaction's pending work.

The inner context manager issued BEGIN (which fails inside an open
transaction) and the except handler then called rollback() unconditionally,
discarding everything the outer transaction had done. Nesting is now
refused before any connection state is touched.
"""

import sqlite3


def test_nested_transaction_does_not_destroy_outer_work(connection):
    col = connection.c1
    with connection.transaction():
        col.insert_one({"outer": 1})
        try:
            with connection.transaction():
                pass  # BEGIN fails here
        except sqlite3.OperationalError:
            pass  # caller may swallow; outer work must survive
    assert col.find_one({"outer": 1}) is not None


def test_single_level_transaction_still_commits(connection):
    col = connection.c1
    with connection.transaction():
        col.insert_one({"doc": 1})
    assert col.find_one({"doc": 1}) is not None


def test_exception_inside_transaction_rolls_back(connection):
    col = connection.c1
    try:
        with connection.transaction():
            col.insert_one({"doomed": 1})
            raise ValueError("boom")
    except ValueError:
        pass
    assert col.find_one({"doomed": 1}) is None


def test_nested_refusal_raises_operational_error(connection):
    import pytest

    with connection.transaction():
        with pytest.raises(sqlite3.OperationalError):
            with connection.transaction():
                pass
