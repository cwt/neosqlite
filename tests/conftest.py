# coding: utf-8
import pytest
import sqlite3
import neosqlite


@pytest.fixture
def db():
    """Fixture to set up and tear down a database connection."""
    conn = sqlite3.connect(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def collection(db):
    """Fixture to provide a clean collection for each test."""
    collection = neosqlite.Collection(db, "foo")
    return collection
