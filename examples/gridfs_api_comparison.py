import io
from neosqlite import Connection
from neosqlite.gridfs import GridFS, GridFSBucket


def api_comparison_example():
    """Compare legacy GridFS API with modern GridFSBucket API."""
    print("=== GridFS API Comparison ===")
    print()

    with Connection(":memory:") as conn:
        # Legacy GridFS API (simpler but less flexible)
        print("1. Legacy GridFS API:")
        fs = GridFS(conn.db)

        # Put data (simple)
        file_id = fs.put(
            b"Hello from legacy API!", filename="legacy.txt", author="user"
        )
        print(f"   Put file with fs.put(): ID {file_id}")

        # Get data (simple)
        grid_out = fs.get(file_id)
        content = grid_out.read()
        print(f"   Got file with fs.get(): {content.decode('utf-8')}")

        print()

        # Modern GridFSBucket API (more flexible)
        print("2. Modern GridFSBucket API:")
        bucket = GridFSBucket(conn.db)

        # Upload from stream (explicit)
        file_id2 = bucket.upload_from_stream(
            "modern.txt", b"Hello from modern API!"
        )
        print(f"   Uploaded with bucket.upload_from_stream(): ID {file_id2}")

        # Download to stream (explicit)
        output = io.BytesIO()
        bucket.download_to_stream(file_id2, output)
        content2 = output.getvalue()
        print(
            f"   Downloaded with bucket.download_to_stream(): {content2.decode('utf-8')}"
        )

        print()
        print("3. Key Differences:")
        print(
            "   - Legacy API (GridFS): Simpler method names, automatic versioning"
        )
        print(
            "   - Modern API (GridFSBucket): More explicit method names, PyMongo compatible"
        )
        print("   - Both APIs work with the same underlying data storage")


if __name__ == "__main__":
    api_comparison_example()
