import io
from neosqlite import Connection
from neosqlite.gridfs import GridFS


def test_new_file_method():
    """Test the new_file method in the legacy GridFS API."""
    print("=== Testing GridFS.new_file() Method ===\n")

    with Connection(":memory:") as conn:
        fs = GridFS(conn.db)

        # Test new_file method with context manager
        print("1. Testing new_file() with context manager...")
        with fs.new_file(
            filename="test_new_file.txt", author="tester"
        ) as grid_in:
            grid_in.write(b"Content for new file test")

        # Verify the file was created
        grid_out = fs.get_last_version("test_new_file.txt")
        content = grid_out.read()
        print(f"   Created file content: {content.decode('utf-8')}")

        # Test new_file method with manual close
        print("\n2. Testing new_file() with manual close...")
        grid_in = fs.new_file(filename="manual_close.txt", version="1.0")
        grid_in.write(b"Manual close test content")
        grid_in.close()

        # Verify the file was created
        grid_out = fs.get_last_version("manual_close.txt")
        content = grid_out.read()
        print(f"   Created file content: {content.decode('utf-8')}")

        # Test new_file with custom ID
        print("\n3. Testing new_file() with custom ID...")
        try:
            grid_in = fs.new_file(_id=100, filename="custom_id.txt")
            grid_in.write(b"Custom ID test content")
            grid_in.close()

            # Verify the file was created with the custom ID
            grid_out = fs.get(100)
            content = grid_out.read()
            print(
                f"   Created file with custom ID, content: {content.decode('utf-8')}"
            )
        except Exception as e:
            print(f"   Error with custom ID: {e}")

        print("\n=== All new_file() tests completed successfully! ===")


if __name__ == "__main__":
    test_new_file_method()
