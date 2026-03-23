"""Module for comparing binary data support between NeoSQLite and PyMongo"""

import uuid
import warnings

import neosqlite
from neosqlite.binary import Binary

from .reporter import reporter
from .timing import (
    end_mongo_timing,
    end_neo_timing,
    set_accumulation_mode,
    start_mongo_timing,
    start_neo_timing,
)
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_binary_support():
    """Compare Binary data support"""
    print("\n=== Binary Data Support Comparison ===")

    # Initialize results
    neo_has_binary = False
    neo_from_uuid = False
    neo_as_uuid = False
    neo_subtypes = False

    mongo_has_binary = None
    mongo_from_uuid = None
    mongo_as_uuid = None
    mongo_subtypes = None

    # NeoSQLite tests
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        set_accumulation_mode(True)

        # 1. Basic Binary support
        start_neo_timing()
        try:
            binary_data = Binary(b"test binary data")
            neo_collection.insert_one({"data": binary_data, "name": "test"})
            doc = neo_collection.find_one({"name": "test"})
            if doc and isinstance(doc.get("data"), Binary):
                neo_has_binary = True
        except Exception as e:
            print(f"Neo Basic Binary: Error - {e}")
        finally:
            end_neo_timing()
        print(f"Neo Binary support: {'OK' if neo_has_binary else 'FAIL'}")

        # 2. Test from_uuid()
        start_neo_timing()
        try:
            test_uuid = uuid.uuid4()
            binary_from_uuid = Binary.from_uuid(test_uuid)
            if binary_from_uuid is not None and isinstance(
                binary_from_uuid, Binary
            ):
                if binary_from_uuid.subtype == Binary.UUID_SUBTYPE:
                    neo_from_uuid = True
        except Exception as e:
            print(f"Neo Binary.from_uuid(): Error - {e}")
        finally:
            end_neo_timing()
        print(f"Neo Binary.from_uuid(): {'OK' if neo_from_uuid else 'FAIL'}")

        # 3. Test as_uuid()
        start_neo_timing()
        try:
            test_uuid = uuid.uuid4()
            binary_from_uuid = Binary.from_uuid(test_uuid)
            result_uuid = binary_from_uuid.as_uuid()
            if result_uuid == test_uuid:
                neo_as_uuid = True
        except Exception as e:
            print(f"Neo Binary.as_uuid(): Error - {e}")
        finally:
            end_neo_timing()
        print(f"Neo Binary.as_uuid(): {'OK' if neo_as_uuid else 'FAIL'}")

        # 4. Test Binary subtypes
        start_neo_timing()
        try:
            binary_with_subtype = Binary(b"test data", subtype=0x80)
            if binary_with_subtype.subtype == 0x80:
                neo_subtypes = True
        except Exception as e:
            print(f"Neo Binary subtypes: Error - {e}")
        finally:
            end_neo_timing()
        print(f"Neo Binary subtypes: {'OK' if neo_subtypes else 'FAIL'}")

    # MongoDB tests
    client = test_pymongo_connection()
    if client:
        try:
            from bson import Binary as BsonBinary
            from bson.binary import UuidRepresentation

            mongo_db = client.test_database
            mongo_collection = mongo_db.test_collection
            mongo_collection.delete_many({})
            set_accumulation_mode(True)

            # 1. Basic Binary support
            start_mongo_timing()
            try:
                binary_data = BsonBinary(b"test binary data")
                mongo_collection.insert_one(
                    {"data": binary_data, "name": "test"}
                )
                doc = mongo_collection.find_one({"name": "test"})
                if doc and isinstance(doc.get("data"), (BsonBinary, bytes)):
                    mongo_has_binary = True
            except Exception as e:
                print(f"Mongo Basic Binary: Error - {e}")
            finally:
                end_mongo_timing()
            print(
                f"Mongo Binary support: {'OK' if mongo_has_binary else 'FAIL'}"
            )

            # 2. Test from_uuid()
            start_mongo_timing()
            try:
                test_uuid = uuid.uuid4()
                # PyMongo requires a representation if not using CodecOptions
                binary_from_uuid = BsonBinary.from_uuid(
                    test_uuid, UuidRepresentation.STANDARD
                )
                if binary_from_uuid is not None:
                    mongo_from_uuid = True
            except Exception as e:
                print(f"Mongo Binary.from_uuid(): Error - {e}")
            finally:
                end_mongo_timing()
            print(
                f"Mongo Binary.from_uuid(): {'OK' if mongo_from_uuid else 'FAIL'}"
            )

            # 3. Test as_uuid()
            start_mongo_timing()
            try:
                test_uuid = uuid.uuid4()
                binary_from_uuid = BsonBinary.from_uuid(
                    test_uuid, UuidRepresentation.STANDARD
                )
                result_uuid = binary_from_uuid.as_uuid(
                    UuidRepresentation.STANDARD
                )
                if result_uuid == test_uuid:
                    mongo_as_uuid = True
            except Exception as e:
                print(f"Mongo Binary.as_uuid(): Error - {e}")
            finally:
                end_mongo_timing()
            print(
                f"Mongo Binary.as_uuid(): {'OK' if mongo_as_uuid else 'FAIL'}"
            )

            # 4. Test Binary subtypes
            start_mongo_timing()
            try:
                binary_with_subtype = BsonBinary(b"test data", subtype=0x80)
                if binary_with_subtype.subtype == 0x80:
                    mongo_subtypes = True
            except Exception as e:
                print(f"Mongo Binary subtypes: Error - {e}")
            finally:
                end_mongo_timing()
            print(
                f"Mongo Binary subtypes: {'OK' if mongo_subtypes else 'FAIL'}"
            )
        except ImportError:
            print("PyMongo or bson not installed correctly for binary tests")
        finally:
            client.close()

    reporter.record_comparison(
        "Binary Support",
        "Binary",
        neo_has_binary if neo_has_binary else "FAIL",
        mongo_has_binary if mongo_has_binary is not None else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Binary Support",
        "from_uuid",
        neo_from_uuid if neo_from_uuid else "FAIL",
        mongo_from_uuid if mongo_from_uuid is not None else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Binary Support",
        "as_uuid",
        neo_as_uuid if neo_as_uuid else "FAIL",
        mongo_as_uuid if mongo_as_uuid is not None else None,
        skip_reason="MongoDB not available" if not client else None,
    )
    reporter.record_comparison(
        "Binary Support",
        "subtypes",
        neo_subtypes if neo_subtypes else "FAIL",
        mongo_subtypes if mongo_subtypes is not None else None,
        skip_reason="MongoDB not available" if not client else None,
    )
