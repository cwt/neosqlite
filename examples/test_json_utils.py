from neosqlite.binary import Binary
from neosqlite.collection.json_helpers import (
    neosqlite_json_dumps,
    neosqlite_json_loads,
)


def test_json_utils():
    """Test the custom JSON utilities."""
    print("=== Testing JSON Utilities ===")

    # Test Binary serialization/deserialization
    binary_data = Binary(b"test binary data")
    print(f"Original binary: {binary_data}")
    print(f"Original type: {type(binary_data)}")

    # Serialize
    json_str = neosqlite_json_dumps({"data": binary_data})
    print(f"Serialized JSON: {json_str}")

    # Deserialize
    loaded_data = neosqlite_json_loads(json_str)
    print(f"Deserialized data: {loaded_data}")
    print(f"Deserialized type: {type(loaded_data['data'])}")
    print(f"Data matches: {loaded_data['data'] == binary_data}")

    # Test nested Binary
    nested_data = {
        "binary_field": binary_data,
        "nested": {
            "inner_binary": Binary(b"inner data", Binary.FUNCTION_SUBTYPE)
        },
    }

    json_str = neosqlite_json_dumps(nested_data)
    print(f"\nNested JSON: {json_str}")

    loaded_nested = neosqlite_json_loads(json_str)
    print(f"Loaded nested: {loaded_nested}")
    print(f"Nested binary type: {type(loaded_nested['binary_field'])}")
    print(f"Inner binary type: {type(loaded_nested['nested']['inner_binary'])}")
    print(f"Inner subtype: {loaded_nested['nested']['inner_binary'].subtype}")


if __name__ == "__main__":
    test_json_utils()
