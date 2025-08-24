from neosqlite import Connection
import io


def test_binary_data_support():
    """Test binary data support outside of GridFS."""
    print("=== Testing Binary Data Support Outside of GridFS ===\n")

    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Test storing binary data directly in documents
        print("1. Testing binary data in documents...")

        # Create binary data
        binary_data = b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09"

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
        print(f"   Retrieved binary data: {doc['binary_field']}")
        print(f"   Retrieved nested binary: {doc['nested']['inner_binary']}")
        print(f"   Binary data matches: {doc['binary_field'] == binary_data}")

        # Test with larger binary data
        print("\n2. Testing larger binary data...")
        large_binary = b"x" * 10000  # 10KB of data

        result = collection.insert_one(
            {"name": "large_binary_test", "large_data": large_binary}
        )

        doc = collection.find_one({"name": "large_binary_test"})
        print(f"   Large binary size: {len(doc['large_data'])} bytes")
        print(f"   Large binary matches: {doc['large_data'] == large_binary}")

        # Test binary data with different operations
        print("\n3. Testing binary data with update operations...")

        # Update with binary data
        collection.update_one(
            {"name": "binary_test"},
            {"$set": {"updated_binary": b"updated binary data"}},
        )

        doc = collection.find_one({"name": "binary_test"})
        print(f"   Updated binary data: {doc['updated_binary']}")

        # Test queries with binary data
        print("\n4. Testing queries with binary data...")
        docs = list(collection.find({"binary_field": binary_data}))
        print(f"   Found {len(docs)} documents with specific binary data")

        print("\n=== All binary data tests completed successfully! ===")


if __name__ == "__main__":
    test_binary_data_support()
