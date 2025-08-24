from neosqlite import Connection
from neosqlite.gridfs import GridFSBucket


def test_gridfsbucket_extensions():
    """Test the new GridFSBucket methods: delete_by_name, rename, rename_by_name."""
    print("=== Testing GridFSBucket Extension Methods ===\n")

    with Connection(":memory:") as conn:
        bucket = GridFSBucket(conn.db)

        # Upload test files
        print("1. Uploading test files...")
        file_id1 = bucket.upload_from_stream(
            "test_file.txt", b"Content of test file 1"
        )
        file_id2 = bucket.upload_from_stream(
            "test_file.txt", b"Content of test file 2"
        )  # Same name, different version
        file_id3 = bucket.upload_from_stream(
            "other_file.txt", b"Content of other file"
        )
        print(f"   Uploaded files with IDs: {file_id1}, {file_id2}, {file_id3}")

        # Test rename method
        print("\n2. Testing rename() method...")
        bucket.rename(file_id1, "renamed_file.txt")
        print(f"   Renamed file {file_id1} to 'renamed_file.txt'")

        # Verify rename worked
        grid_out = bucket.open_download_stream(file_id1)
        print(f"   File {file_id1} is now named: {grid_out.filename}")

        # Test rename_by_name method
        print("\n3. Testing rename_by_name() method...")
        bucket.rename_by_name("other_file.txt", "new_other_file.txt")
        print("   Renamed 'other_file.txt' to 'new_other_file.txt'")

        # Verify rename_by_name worked
        grid_out = bucket.open_download_stream(file_id3)
        print(f"   File {file_id3} is now named: {grid_out.filename}")

        # Test delete_by_name method
        print("\n4. Testing delete_by_name() method...")
        bucket.delete_by_name("test_file.txt")
        print("   Deleted all files named 'test_file.txt'")

        # Verify delete_by_name worked (should only have renamed_file.txt and new_other_file.txt left)
        cursor = bucket.find({})
        remaining_files = list(cursor)
        print(f"   Remaining files: {[f.filename for f in remaining_files]}")

        # Test error handling
        print("\n5. Testing error handling...")
        try:
            bucket.rename(999999, "nonexistent.txt")
        except Exception as e:
            print(
                f"   Correctly raised exception for rename of nonexistent file: {type(e).__name__}"
            )

        try:
            bucket.rename_by_name("nonexistent.txt", "new_name.txt")
        except Exception as e:
            print(
                f"   Correctly raised exception for rename_by_name of nonexistent file: {type(e).__name__}"
            )

        try:
            bucket.delete_by_name("nonexistent.txt")
        except Exception as e:
            print(
                f"   Correctly raised exception for delete_by_name of nonexistent file: {type(e).__name__}"
            )

        print(
            "\n=== All GridFSBucket extension methods tested successfully! ==="
        )


if __name__ == "__main__":
    test_gridfsbucket_extensions()
