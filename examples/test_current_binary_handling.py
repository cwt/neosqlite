from neosqlite import Connection
import base64


def test_current_binary_handling():
    """Test how neosqlite currently handles binary-like data."""
    print("=== Testing Current Binary Data Handling ===\n")

    with Connection(":memory:") as conn:
        collection = conn.test_collection

        # Test storing binary data encoded as base64 string
        print("1. Testing base64-encoded binary data...")

        # Create binary data
        binary_data = b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09"
        encoded_data = base64.b64encode(binary_data).decode("utf-8")

        # Store encoded binary as string
        result = collection.insert_one(
            {
                "name": "encoded_binary_test",
                "binary_field": encoded_data,
                "nested": {"inner_binary": encoded_data},
            }
        )

        print(f"   Inserted document with ID: {result.inserted_id}")

        # Retrieve and verify
        doc = collection.find_one({"name": "encoded_binary_test"})
        print(f"   Retrieved encoded data: {doc['binary_field']}")
        decoded_data = base64.b64decode(doc["binary_field"])
        print(f"   Decoded binary data: {decoded_data}")
        print(
            f"   Decoded data matches original: {decoded_data == binary_data}"
        )

        # Test automatic encoding/decoding approach
        print("\n2. Testing automatic handling approach...")

        # We could implement a wrapper that automatically encodes/decodes
        class Binary:
            def __init__(self, data):
                if isinstance(data, str):
                    self.data = data.encode(
                        "latin1"
                    )  # Assume latin1 encoded bytes string
                else:
                    self.data = bytes(data)

            def encode_for_storage(self):
                """Encode binary data for JSON storage."""
                return base64.b64encode(self.data).decode("utf-8")

            @classmethod
            def decode_from_storage(cls, encoded_data):
                """Decode binary data from JSON storage."""
                return cls(base64.b64decode(encoded_data))

            def __eq__(self, other):
                if isinstance(other, Binary):
                    return self.data == other.data
                return False

            def __repr__(self):
                return f"Binary({self.data!r})"

        # Test our Binary wrapper
        binary_obj = Binary(b"test binary data")
        encoded_for_storage = binary_obj.encode_for_storage()

        result = collection.insert_one(
            {
                "name": "binary_wrapper_test",
                "binary_field": encoded_for_storage,
                "is_binary": True,
            }
        )

        doc = collection.find_one({"name": "binary_wrapper_test"})
        decoded_binary = Binary.decode_from_storage(doc["binary_field"])
        print(f"   Stored: {binary_obj}")
        print(f"   Retrieved: {decoded_binary}")
        print(f"   Data matches: {binary_obj == decoded_binary}")

        print("\n=== Current approach requires manual encoding/decoding ===")


if __name__ == "__main__":
    test_current_binary_handling()
