import pytest
import os
from neosqlite import Connection
from neosqlite._sqlite import sqlite3


def test_json_schema_write_validation():
    db_path = "test_validation.db"
    if os.path.exists(db_path):
        os.remove(db_path)

    with Connection(db_path) as conn:
        # Create collection with validator
        schema = {
            "$jsonSchema": {
                "required": ["name"],
                "properties": {"age": {"minimum": 18}},
            }
        }
        coll = conn.create_collection("users", validator=schema)

        # 1. Valid insert
        coll.insert_one({"name": "Alice", "age": 20})
        assert coll.count_documents({"name": "Alice"}) == 1

        # 2. Invalid insert (missing required 'name')
        with pytest.raises(sqlite3.IntegrityError):
            coll.insert_one({"age": 25})

        # 3. Invalid insert (age too low)
        with pytest.raises(sqlite3.IntegrityError):
            coll.insert_one({"name": "Bob", "age": 15})

        # 4. Valid update
        coll.update_one({"name": "Alice"}, {"$set": {"age": 21}})

        # 5. Invalid update
        with pytest.raises(sqlite3.IntegrityError):
            coll.update_one({"name": "Alice"}, {"$set": {"age": 10}})

    if os.path.exists(db_path):
        os.remove(db_path)
