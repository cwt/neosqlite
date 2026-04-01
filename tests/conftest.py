# coding: utf-8
import logging

import pytest

import neosqlite

# Suppress neosqlite logger warnings during test runs (e.g., pending transaction
# warnings from fixture teardown). These warnings are still useful in production.
logging.getLogger("neosqlite").setLevel(logging.CRITICAL)


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
