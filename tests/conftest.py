# coding: utf-8
import sqlite3
from pytest import fixture

import sys
import os

# Add the parent directory to the sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import nosqlite


@fixture(scope="module")
def db(request) -> sqlite3.Connection:
    _db = sqlite3.connect(":memory:")
    request.addfinalizer(_db.close)
    return _db


@fixture(scope="module")
def collection(db: sqlite3.Connection, request) -> nosqlite.Collection:
    return nosqlite.Collection(db, "foo", create=False)

