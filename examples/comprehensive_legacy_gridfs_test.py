from neosqlite import Connection
from neosqlite.gridfs import GridFS, NoFile


def comprehensive_legacy_gridfs_test():
    """Comprehensive test of all legacy GridFS methods."""
    print("=== Comprehensive Legacy GridFS Test ===")
    print()

    with Connection(":memory:") as conn:
        fs = GridFS(conn.db)

        # Test put method with various parameters
        print("1. Testing put() method...")
        data1 = b"First file content"
        file_id1 = fs.put(
            data1, filename="first.txt", author="tester", version="1.0"
        )
        print(f"   Put first file with ID: {file_id1}")

        data2 = b"Second file content"
        file_id2 = fs.put(data2, filename="second.txt")
        print(f"   Put second file with ID: {file_id2}")

        # Test putting file with same name (creates new version)
        data3 = b"First file updated content"
        file_id3 = fs.put(
            data3, filename="first.txt", author="tester", version="2.0"
        )
        print(f"   Put updated first file with ID: {file_id3}")

        # Test get method
        print("\n2. Testing get() method...")
        grid_out = fs.get(file_id1)
        content = grid_out.read()
        print(
            f"   Retrieved file {grid_out.filename}: {content.decode('utf-8')}"
        )

        # Test get_version method
        print("\n3. Testing get_version() method...")
        grid_out_v0 = fs.get_version("first.txt", 0)  # First version
        print(
            f"   First version of 'first.txt': {grid_out_v0.read().decode('utf-8')}"
        )

        grid_out_v1 = fs.get_version("first.txt", 1)  # Second version
        print(
            f"   Second version of 'first.txt': {grid_out_v1.read().decode('utf-8')}"
        )

        # Test get_last_version method
        print("\n4. Testing get_last_version() method...")
        grid_out_latest = fs.get_last_version("first.txt")
        print(
            f"   Latest version of 'first.txt': {grid_out_latest.read().decode('utf-8')}"
        )

        # Test list method
        print("\n5. Testing list() method...")
        filenames = fs.list()
        print(f"   All filenames: {filenames}")

        # Test find method
        print("\n6. Testing find() method...")
        cursor = fs.find({"filename": "first.txt"})
        files = list(cursor)
        print(f"   Found {len(files)} versions of 'first.txt'")

        # Test find_one method
        print("\n7. Testing find_one() method...")
        file = fs.find_one({"filename": "second.txt"})
        if file:
            print(f"   Found file: {file.filename}")

        # Test exists method
        print("\n8. Testing exists() method...")
        exists_by_id = fs.exists(file_id1)
        print(f"   File with ID {file_id1} exists: {exists_by_id}")

        exists_by_name = fs.exists(filename="first.txt")
        print(f"   File with name 'first.txt' exists: {exists_by_name}")

        exists_nonexistent = fs.exists(file_id=999999)
        print(f"   Non-existent file exists: {exists_nonexistent}")

        # Test delete method
        print("\n9. Testing delete() method...")
        fs.delete(file_id2)
        print(f"   Deleted file with ID: {file_id2}")

        # Verify deletion
        exists_after_delete = fs.exists(file_id2)
        print(
            f"   File with ID {file_id2} exists after deletion: {exists_after_delete}"
        )

        # Test error handling
        print("\n10. Testing error handling...")
        try:
            fs.get(file_id2)  # Should raise NoFile
        except NoFile:
            print("   Correctly raised NoFile exception for deleted file")

        try:
            fs.get_version("nonexistent.txt", 0)  # Should raise NoFile
        except NoFile:
            print("   Correctly raised NoFile exception for nonexistent file")

        print("\n=== All tests passed! ===")


if __name__ == "__main__":
    comprehensive_legacy_gridfs_test()
