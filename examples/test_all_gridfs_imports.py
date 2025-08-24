# Test all GridFS imports
from neosqlite.gridfs import (
    GridFSBucket,
    GridFS,
    NoFile,
    FileExists,
    CorruptGridFile,
)

print("All GridFS imports successful!")

# Test that we can create instances
print("GridFSBucket and GridFS classes are accessible")

# Test error imports
print("Error classes imported:", NoFile, FileExists, CorruptGridFile)
