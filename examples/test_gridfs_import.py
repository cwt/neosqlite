import io
from neosqlite import Connection

# Test that we can import GridFS directly from neosqlite
from neosqlite import GridFS


def test_gridfs_import():
    """Test that GridFS can be imported from the main package."""
    print("Testing GridFS import from main package...")

    # Create an in-memory database
    with Connection(":memory:") as conn:
        # Create a legacy GridFS instance
        fs = GridFS(conn.db)

        # Put and get a file
        file_id = fs.put(b"Test content", filename="test.txt")
        grid_out = fs.get(file_id)
        content = grid_out.read()

        assert content == b"Test content"
        print("GridFS import and basic functionality test passed!")


if __name__ == "__main__":
    test_gridfs_import()
