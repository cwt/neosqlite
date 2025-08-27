# coding: utf-8
import pytest
import neosqlite


@pytest.fixture
def connection():
    """Fixture to set up and tear down a neosqlite connection."""
    conn = neosqlite.Connection(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def collection(connection):
    """Fixture to provide a clean collection for each test."""
    return connection["foo"]
