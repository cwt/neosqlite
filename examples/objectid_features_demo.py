#!/usr/bin/env python3
"""
Comprehensive demonstration of ObjectId features in NeoSQLite.

This example shows the new MongoDB-compatible ObjectId implementation,
including automatic generation, hex interchangeability, and query capabilities.
"""

from neosqlite import Connection
from neosqlite.objectid import ObjectId


def main():
    print("=== NeoSQLite ObjectId Features Demo ===\n")

    with Connection(":memory:") as conn:
        collection = conn.test_collection

        print("1. Automatic ObjectId generation for new documents:")
        # When inserting without specifying _id, an ObjectId is automatically generated
        result = collection.insert_one(
            {"name": "Auto-generated ID doc", "value": 123}
        )
        doc = collection.find_one({"_id": result.inserted_id})
        print(f"   Inserted document with integer ID: {result.inserted_id}")
        print(f"   Document _id field: {doc['_id']} (type: {type(doc['_id'])})")
        print()

        print("2. Manual ObjectId assignment:")
        manual_oid = ObjectId()
        collection.insert_one(
            {"_id": manual_oid, "name": "Manual ID doc", "value": 456}
        )
        found_doc = collection.find_one({"_id": manual_oid})
        print(f"   Document with manual ObjectId: {found_doc['_id']}")
        print()

        print("3. Querying with ObjectId hex strings:")
        hex_result = collection.find_one({"_id": str(manual_oid)})
        print(f"   Found using hex string: {hex_result is not None}")
        print()

        print("4. Querying with integer IDs (backward compatibility):")
        first_doc = collection.find_one({"_id": result.inserted_id})
        print(f"   Found using integer ID: {first_doc is not None}")
        print()

        print("5. Automatic ID type correction:")
        # These queries work automatically with the new ID type correction
        try:
            # Querying 'id' field with ObjectId (user mistake) - automatically corrected
            found_mistake1 = collection.find_one({"id": manual_oid})
            print(
                f"   Query 'id' field with ObjectId (auto-corrected): {found_mistake1 is not None}"
            )

            # Querying 'id' field with hex string - automatically corrected
            found_mistake2 = collection.find_one({"id": str(manual_oid)})
            print(
                f"   Query 'id' field with hex string (auto-corrected): {found_mistake2 is not None}"
            )

            # Querying '_id' field with integer string - automatically corrected
            found_mistake3 = collection.find_one(
                {"_id": str(result.inserted_id)}
            )
            print(
                f"   Query '_id' field with integer string (auto-corrected): {found_mistake3 is not None}"
            )
        except Exception as e:
            print(f"   Error in type correction demo: {e}")
        print()

        print("6. ObjectId features:")
        new_oid = ObjectId()
        print(f"   Generated ObjectId: {new_oid}")
        print(f"   Hex representation: {new_oid.hex}")
        print(f"   Binary representation: {new_oid.binary}")
        print(f"   Generation time: {new_oid.generation_time()}")
        print(f"   Is valid: {ObjectId.is_valid(new_oid)}")
        print()

        print("7. Mixed ID types in same collection:")
        # Collection can have documents with both auto-generated ObjectIds and manual IDs
        collection.insert_one({"name": "Integer ID doc"}).inserted_id
        collection.insert_one(
            {"_id": ObjectId(), "name": "Object ID doc"}
        ).inserted_id

        all_docs = list(collection.find())
        print(f"   Total documents in collection: {len(all_docs)}")
        for i, doc in enumerate(all_docs):
            print(
                f"     Doc {i+1}: _id={doc['_id']} (type: {type(doc['_id'])})"
            )
        print()

        print("8. PyMongo compatibility:")
        print("   ObjectIds are fully compatible with PyMongo format")
        print("   Hex strings can be interchanged with PyMongo ObjectIds")
        print("   Timestamp format is compatible")
        print()

    print("=== ObjectId Features Demo Complete ===")


if __name__ == "__main__":
    main()
