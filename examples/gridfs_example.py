import io
from neosqlite import Connection
from neosqlite.gridfs import GridFSBucket


# Example usage of GridFS with NeoSQLite
def example_gridfs_usage():
    # Create an in-memory database
    with Connection(":memory:") as conn:
        # Create a GridFS bucket
        bucket = GridFSBucket(conn.db)

        # Upload a file from bytes
        file_data = b"This is a sample file content for GridFS demonstration."
        file_id = bucket.upload_from_stream("example.txt", file_data)
        print(f"Uploaded file with ID: {file_id}")

        # Download the file to a BytesIO stream
        output = io.BytesIO()
        bucket.download_to_stream(file_id, output)
        downloaded_data = output.getvalue()
        print(f"Downloaded data: {downloaded_data.decode('utf-8')}")

        # Upload a file using streaming
        with bucket.open_upload_stream("streamed.txt") as grid_in:
            grid_in.write(b"First part of the file.")
            grid_in.write(b" Second part of the file.")
            grid_in.write(b" Final part of the file.")

        # Download the streamed file
        with bucket.open_download_stream_by_name("streamed.txt") as grid_out:
            streamed_data = grid_out.read()
            print(f"Streamed data: {streamed_data.decode('utf-8')}")

        # Find files
        cursor = bucket.find({"filename": "example.txt"})
        files = list(cursor)
        print(f"Found {len(files)} file(s) with name 'example.txt'")

        # Delete a file
        bucket.delete(file_id)
        print("Deleted file with ID:", file_id)

        # Try to download the deleted file (will raise NoFile exception)
        try:
            output = io.BytesIO()
            bucket.download_to_stream(file_id, output)
        except Exception as e:
            print(
                f"Expected error when downloading deleted file: {type(e).__name__}"
            )


if __name__ == "__main__":
    example_gridfs_usage()
