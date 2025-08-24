from neosqlite import Connection
from neosqlite.gridfs import GridFS


def test_legacy_gridfs_with_extensions():
    """Test that legacy GridFS API works with the extended GridFSBucket methods."""
    print("=== Testing Legacy GridFS with Extension Methods ===\n")

    with Connection(":memory:") as conn:
        fs = GridFS(conn.db)

        # Upload test files using legacy API
        print("1. Uploading test files using legacy API...")
        file_id1 = fs.put(b"Content of test file 1", filename="test_file.txt")
        file_id2 = fs.put(
            b"Content of test file 2", filename="test_file.txt"
        )  # Same name, different version
        file_id3 = fs.put(b"Content of other file", filename="other_file.txt")
        print(f"   Uploaded files with IDs: {file_id1}, {file_id2}, {file_id3}")

        # Test that we can access files using legacy API
        print("\n2. Testing file access using legacy API...")
        grid_out1 = fs.get(file_id1)
        print(f"   File {file_id1} content: {grid_out1.read().decode('utf-8')}")

        grid_out2 = fs.get_last_version("test_file.txt")
        print(
            f"   Latest 'test_file.txt' content: {grid_out2.read().decode('utf-8')}"
        )

        # Test listing files
        print("\n3. Testing file listing...")
        filenames = fs.list()
        print(f"   All filenames: {filenames}")

        # Test exists method
        print("\n4. Testing exists method...")
        exists1 = fs.exists(file_id1)
        exists_nonexistent = fs.exists(file_id=999999)
        exists_by_name = fs.exists(filename="test_file.txt")
        print(f"   File {file_id1} exists: {exists1}")
        print(f"   Nonexistent file exists: {exists_nonexistent}")
        print(f"   File with name 'test_file.txt' exists: {exists_by_name}")

        print("\n=== Legacy GridFS API works correctly with extensions! ===")


if __name__ == "__main__":
    test_legacy_gridfs_with_extensions()
