#!/usr/bin/env python3
"""
Demo script showing PyMongo exception compatibility.
This demonstrates that NeoSQLite GridFS exceptions can be used
the same way as PyMongo GridFS exceptions.
"""

from neosqlite import Connection
from neosqlite.gridfs import GridFS, GridFSBucket
from neosqlite.gridfs.errors import (
    GridFSError,
    NoFile,
    FileExists,
    NeoSQLiteError,
)


def demo_exception_handling():
    """Demonstrate PyMongo-compatible exception handling."""
    print("=== NeoSQLite Exception Compatibility Demo ===\n")

    # Create connection and GridFS instances
    with Connection(":memory:") as conn:
        fs = GridFS(conn.db)
        bucket = GridFSBucket(conn.db)

        # Demo 1: Specific exception handling
        print("1. Specific exception handling:")
        try:
            # Try to get a non-existent file
            fs.get(999999)
        except NoFile as e:
            print(f"   Caught NoFile exception: {e}")
        except Exception as e:
            print(f"   Caught other exception: {e}")

        # Demo 2: Base class exception handling
        print("\n2. Base class exception handling:")
        try:
            # Try to get a non-existent file
            bucket.open_download_stream(888888)
        except GridFSError as e:
            print(f"   Caught GridFSError (base class): {e}")
        except Exception as e:
            print(f"   Caught other exception: {e}")

        # Demo 3: NeoSQLite base exception handling
        print("\n3. NeoSQLite base exception handling:")
        try:
            # Try to get a non-existent file
            bucket.open_download_stream(777777)
        except NeoSQLiteError as e:
            print(f"   Caught NeoSQLiteError (base class): {e}")
        except Exception as e:
            print(f"   Caught other exception: {e}")

        # Demo 4: Exception hierarchy
        print("\n4. Exception hierarchy demonstration:")
        try:
            # Upload a file
            file_id = fs.put(b"test data", filename="test.txt")
            print(f"   Uploaded file with ID: {file_id}")

            # Try to upload with same ID (will raise FileExists)
            fs.put(b"more data", _id=file_id)
        except FileExists as e:
            print(f"   Caught FileExists: {e}")
        except GridFSError as e:
            print(f"   Caught GridFSError: {e}")
        except NeoSQLiteError as e:
            print(f"   Caught NeoSQLiteError: {e}")

        # Demo 5: Error labels (PyMongo compatibility feature)
        print("\n5. Error labels functionality:")
        try:
            exc = NoFile(
                "Test error with labels", ["TransientError", "NetworkIssue"]
            )
            print(f"   Exception message: {exc}")
            print(
                f"   Has 'TransientError' label: {exc.has_error_label('TransientError')}"
            )
            print(
                f"   Has 'DatabaseError' label: {exc.has_error_label('DatabaseError')}"
            )

            # Add a label
            exc._add_error_label("Retryable")
            print(
                f"   After adding 'Retryable' label: {exc.has_error_label('Retryable')}"
            )

            # Remove a label
            exc._remove_error_label("TransientError")
            print(
                f"   After removing 'TransientError' label: {exc.has_error_label('TransientError')}"
            )

        except Exception as e:
            print(f"   Error with error labels: {e}")

    print("\n=== Demo Complete ===")
    print(
        "\nNote: PyMongoError is also available as an alias for NeoSQLiteError"
    )
    print("for compatibility with existing PyMongo code.")


if __name__ == "__main__":
    demo_exception_handling()
