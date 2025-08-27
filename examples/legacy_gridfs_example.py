from neosqlite import Connection
from neosqlite.gridfs import GridFS, NoFile


# Example usage of legacy GridFS API with NeoSQLite
def example_legacy_gridfs_usage():
    print("=== NeoSQLite Legacy GridFS Example ===\n")

    # Create an in-memory database
    with Connection(":memory:") as conn:
        # Create a legacy GridFS instance
        fs = GridFS(conn.db, collection_name="fs")

        # Put data into GridFS using the legacy API
        file_data = (
            b"This is a sample file content for legacy GridFS demonstration."
        )
        file_id = fs.put(
            file_data, filename="example.txt", author="NeoSQLite Team"
        )
        print(f"Stored file with ID: {file_id}")

        # Get the file by ID
        grid_out = fs.get(file_id)
        print(f"Retrieved file: {grid_out.filename}")
        print(f"File content: {grid_out.read().decode('utf-8')}")

        # Put another version of the same file
        file_data_v2 = b"This is version 2 of the sample file."
        file_id_v2 = fs.put(file_data_v2, filename="example.txt", version=2)
        print(f"\nStored version 2 with ID: {file_id_v2}")

        # Get the latest version by filename
        grid_out_latest = fs.get_last_version("example.txt")
        print(
            f"Latest version content: {grid_out_latest.read().decode('utf-8')}"
        )

        # Get a specific version by filename
        grid_out_v1 = fs.get_version(
            "example.txt", 0
        )  # 0-indexed, so first version
        print(f"First version content: {grid_out_v1.read().decode('utf-8')}")

        # List all filenames
        filenames = fs.list()
        print(f"\nAll filenames: {filenames}")

        # Find files with a filter
        cursor = fs.find({"filename": "example.txt"})
        files = list(cursor)
        print(f"Found {len(files)} file(s) with name 'example.txt'")

        # Find one file
        file = fs.find_one({"filename": "example.txt"})
        if file:
            print(f"Found one file: {file.filename}")

        # Check if a file exists
        exists = fs.exists(file_id)
        print(f"File with ID {file_id} exists: {exists}")

        # Check if a file exists by filename
        exists_by_name = fs.exists(filename="example.txt")
        print(f"File with name 'example.txt' exists: {exists_by_name}")

        # Delete a file
        fs.delete(file_id)
        print(f"\nDeleted file with ID: {file_id}")

        # Check if deleted file exists
        exists_after_delete = fs.exists(file_id)
        print(
            f"File with ID {file_id} exists after deletion: {exists_after_delete}"
        )

        # Try to get the deleted file (will raise NoFile exception)
        try:
            grid_out = fs.get(file_id)
        except NoFile as e:
            print(
                f"Expected error when getting deleted file: {type(e).__name__}"
            )


if __name__ == "__main__":
    example_legacy_gridfs_usage()
