"""Module for comparing binary data support between NeoSQLite and PyMongo"""

import uuid
import warnings

import neosqlite
from neosqlite.binary import Binary

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_binary_support():
    """Compare Binary data support"""
    print("\n=== Binary Data Support Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        binary_data = Binary(b"test binary data")
        neo_collection.insert_one({"data": binary_data, "name": "test"})
        doc = neo_collection.find_one({"name": "test"})
        neo_has_binary = isinstance(doc.get("data"), (Binary, bytes))
        print(f"Neo Binary support: {neo_has_binary}")

        # Test from_uuid()
        neo_from_uuid = False
        try:
            if hasattr(Binary, "from_uuid"):
                test_uuid = uuid.uuid4()
                binary_from_uuid = Binary.from_uuid(test_uuid)
                neo_from_uuid = binary_from_uuid is not None
                print(
                    f"Neo Binary.from_uuid(): {'OK' if neo_from_uuid else 'FAIL'}"
                )
            else:
                print("Neo Binary.from_uuid(): NOT IMPLEMENTED")
        except Exception as e:
            print(f"Neo Binary.from_uuid(): Error - {e}")

        # Test as_uuid() - requires UUID subtype
        neo_as_uuid = False
        try:
            if hasattr(Binary, "as_uuid"):
                # Create a binary from UUID first (sets UUID_SUBTYPE)
                test_uuid = uuid.uuid4()
                binary_from_uuid = Binary.from_uuid(test_uuid)
                result_uuid = binary_from_uuid.as_uuid()
                neo_as_uuid = result_uuid == test_uuid
                print(
                    f"Neo Binary.as_uuid(): {'OK' if neo_as_uuid else 'FAIL'}"
                )
            else:
                print("Neo Binary.as_uuid(): NOT IMPLEMENTED")
        except Exception as e:
            print(f"Neo Binary.as_uuid(): Error - {e}")

        # Test Binary subtypes
        neo_subtypes = False
        try:
            # Check if Binary supports subtype parameter
            binary_with_subtype = Binary(
                b"test data", subtype=0x80
            )  # User-defined subtype
            if hasattr(binary_with_subtype, "subtype"):
                neo_subtypes = binary_with_subtype.subtype == 0x80
                print(
                    f"Neo Binary subtypes: {'OK' if neo_subtypes else 'PARTIAL'}"
                )
            else:
                print("Neo Binary subtypes: NOT IMPLEMENTED")
        except Exception as e:
            print(f"Neo Binary subtypes: Error - {e}")

    client = test_pymongo_connection()
    mongo_collection = None
    mongo_db = None
    mongo_has_binary = None
    mongo_from_uuid = None
    mongo_as_uuid = None
    mongo_subtypes = None

    if client:
        mongo_db = client.test_database
        mongo_collection = mongo_db.test_collection
        mongo_collection.delete_many({})
        from bson import Binary as BsonBinary

        binary_data = BsonBinary(b"test binary data")
        mongo_collection.insert_one({"data": binary_data, "name": "test"})
        doc = mongo_collection.find_one({"name": "test"})
        mongo_has_binary = isinstance(doc.get("data"), (BsonBinary, bytes))
        print(f"Mongo Binary support: {mongo_has_binary}")

        # Test from_uuid()
        try:
            test_uuid = uuid.uuid4()
            binary_from_uuid = BsonBinary.from_uuid(test_uuid)
            mongo_from_uuid = binary_from_uuid is not None
            print(
                f"Mongo Binary.from_uuid(): {'OK' if mongo_from_uuid else 'FAIL'}"
            )
        except Exception as e:
            mongo_from_uuid = False
            print(f"Mongo Binary.from_uuid(): Error - {e}")

        # Test as_uuid()
        try:
            test_uuid = uuid.uuid4()
            binary_from_uuid = BsonBinary.from_uuid(test_uuid)
            result_uuid = binary_from_uuid.as_uuid()
            mongo_as_uuid = result_uuid == test_uuid
            print(
                f"Mongo Binary.as_uuid(): {'OK' if mongo_as_uuid else 'FAIL'}"
            )
        except Exception as e:
            mongo_as_uuid = False
            print(f"Mongo Binary.as_uuid(): Error - {e}")

        # Test Binary subtypes
        try:
            binary_with_subtype = BsonBinary(
                b"test data", subtype=0x80
            )  # User-defined subtype
            mongo_subtypes = binary_with_subtype.subtype == 0x80
            print(
                f"Mongo Binary subtypes: {'OK' if mongo_subtypes else 'FAIL'}"
            )
        except Exception as e:
            mongo_subtypes = False
            print(f"Mongo Binary subtypes: Error - {e}")

        client.close()

    reporter.record_comparison(
        "Binary Support",
        "Binary",
        neo_has_binary if neo_has_binary else "FAIL",
        mongo_has_binary if mongo_has_binary else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Binary Support",
        "from_uuid",
        neo_from_uuid if neo_from_uuid else "NOT IMPLEMENTED",
        mongo_from_uuid if mongo_from_uuid else None,
        skip_reason=(
            "Not yet implemented in NeoSQLite" if not neo_from_uuid else None
        ),
    )
    reporter.record_comparison(
        "Binary Support",
        "as_uuid",
        neo_as_uuid if neo_as_uuid else "NOT IMPLEMENTED",
        mongo_as_uuid if mongo_as_uuid else None,
        skip_reason=(
            "Not yet implemented in NeoSQLite" if not neo_as_uuid else None
        ),
    )
    reporter.record_comparison(
        "Binary Support",
        "subtypes",
        neo_subtypes if neo_subtypes else "NOT IMPLEMENTED",
        mongo_subtypes if mongo_subtypes else None,
        skip_reason=(
            "Not yet implemented in NeoSQLite" if not neo_subtypes else None
        ),
    )
