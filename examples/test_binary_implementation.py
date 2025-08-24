from neosqlite import Connection, Binary
import uuid


def test_binary_implementation():
    """Test the Binary implementation for neosqlite."""
    print("=== Testing Binary Implementation ===")
    print()

    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Test basic Binary usage
        print("1. Testing basic Binary usage...")

        # Create binary data
        binary_data = Binary(b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09")

        # Store in document
        result = collection.insert_one(
            {
                "name": "binary_test",
                "binary_field": binary_data,
                "nested": {"inner_binary": binary_data},
            }
        )

        print(f"   Inserted document with ID: {result.inserted_id}")

        # Retrieve and verify
        doc = collection.find_one({"name": "binary_test"})
        print(f"   Retrieved binary data type: {type(doc['binary_field'])}")
        print(f"   Retrieved binary data: {doc['binary_field']}")
        print(f"   Retrieved nested binary: {doc['nested']['inner_binary']}")
        print(f"   Binary data matches: {doc['binary_field'] == binary_data}")
        print(f"   Subtype: {doc['binary_field'].subtype}")

        # Test with different subtypes
        print("\n2. Testing different subtypes...")

        # Test with function subtype
        func_binary = Binary(b"function code", Binary.FUNCTION_SUBTYPE)
        result = collection.insert_one(
            {"name": "function_test", "function_data": func_binary}
        )

        doc = collection.find_one({"name": "function_test"})
        print(f"   Function binary subtype: {doc['function_data'].subtype}")
        print(
            f"   Function binary data matches: {doc['function_data'] == func_binary}"
        )

        # Test UUID binary
        print("\n3. Testing UUID binary...")

        # Create a UUID
        test_uuid = uuid.uuid4()
        uuid_binary = Binary.from_uuid(test_uuid)

        result = collection.insert_one(
            {"name": "uuid_test", "uuid_field": uuid_binary}
        )

        doc = collection.find_one({"name": "uuid_test"})
        print(f"   UUID binary subtype: {doc['uuid_field'].subtype}")

        # Convert back to UUID
        converted_uuid = doc["uuid_field"].as_uuid()
        print(f"   Converted UUID matches: {converted_uuid == test_uuid}")

        # Test large binary data
        print("\n4. Testing large binary data...")
        large_binary = Binary(b"x" * 10000)  # 10KB of data

        result = collection.insert_one(
            {"name": "large_binary_test", "large_data": large_binary}
        )

        doc = collection.find_one({"name": "large_binary_test"})
        print(f"   Large binary size: {len(doc['large_data'])} bytes")
        print(f"   Large binary matches: {doc['large_data'] == large_binary}")

        # Test binary data with update operations
        print("\n5. Testing binary data with update operations...")

        # Update with binary data
        new_binary = Binary(b"updated binary data")
        collection.update_one(
            {"name": "binary_test"}, {"$set": {"updated_binary": new_binary}}
        )

        doc = collection.find_one({"name": "binary_test"})
        print(f"   Updated binary data: {doc['updated_binary']}")
        print(f"   Updated binary type: {type(doc['updated_binary'])}")

        # Test queries with binary data
        print("\n6. Testing queries with binary data...")
        docs = list(collection.find({"binary_field": binary_data}))
        print(f"   Found {len(docs)} documents with specific binary data")

        print(
            "\n=== All binary implementation tests completed successfully! ==="
        )


if __name__ == "__main__":
    test_binary_implementation()
