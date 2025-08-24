import io
from neosqlite import Connection
from neosqlite.gridfs import GridFSBucket, NoFile


# Simple GridFS example
def simple_gridfs_example():
    print("=== NeoSQLite GridFS Simple Example ===\n")

    # Create an in-memory database
    with Connection(":memory:") as conn:
        # Create a GridFS bucket
        bucket = GridFSBucket(conn.db)

        # Upload a file
        file_data = b"This is a sample file for GridFS demonstration."
        file_id = bucket.upload_from_stream("sample.txt", file_data)
        print(f"Uploaded file with ID: {file_id}")

        # Download the file
        output = io.BytesIO()
        bucket.download_to_stream(file_id, output)
        downloaded_data = output.getvalue()
        print(f"Downloaded data: {downloaded_data.decode('utf-8')}")

        # Stream upload
        with bucket.open_upload_stream("streamed.txt") as grid_in:
            grid_in.write(b"First part. ")
            grid_in.write(b"Second part.")

        # Stream download
        with bucket.open_download_stream_by_name("streamed.txt") as grid_out:
            streamed_data = grid_out.read()
            print(f"Streamed data: {streamed_data.decode('utf-8')}")

        # Find files
        cursor = bucket.find({"filename": "sample.txt"})
        files = list(cursor)
        print(f"Found {len(files)} file(s) with name 'sample.txt'")

        # Delete a file
        bucket.delete(file_id)
        print(f"Deleted file with ID: {file_id}")

        # Try to download deleted file
        try:
            output = io.BytesIO()
            bucket.download_to_stream(file_id, output)
        except NoFile as e:
            print(f"Expected error: {type(e).__name__}")


if __name__ == "__main__":
    simple_gridfs_example()
