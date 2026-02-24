# GridFS Missing Features Analysis

This document outlines the missing GridFS features identified from a comparison with the PyMongo GridFS API, grouped by their perceived usefulness/popularity and analyzed for their implementability within a SQLite-based backend.

### Group 1: Essential & Highly Useful Helpers

These methods are very common in applications using GridFS and provide significant convenience by simplifying common query patterns.

*   **`find_one()`**
    *   **Usefulness:** Very high. It's one of the most frequently used methods in the PyMongo API for retrieving a single document or file.
    *   **Implementability:** **High.** This is straightforward to implement. It would be a thin wrapper around the existing `find()` method, simply appending `LIMIT 1` to the underlying SQL query.

*   **`get_last_version(filename)` / `get_version(filename, revision=-1)`**
    *   **Usefulness:** High. The ability to retrieve files by name and revision is a core concept in GridFS, which is designed to handle multiple versions of the same file. `get_last_version` is the most common use case.
    *   **Implementability:** **High.** The `uploadDate` column is already available.
        *   `get_last_version`: Can be implemented with an SQL query like `... WHERE filename = ? ORDER BY uploadDate DESC LIMIT 1`.
        *   `get_version`: Can be implemented with `... WHERE filename = ? ORDER BY uploadDate ASC LIMIT 1 OFFSET ?`, where the offset is the revision number. The existing `open_download_stream_by_name` already utilizes similar logic, so this involves creating new top-level helper methods.

*   **`list()`**
    *   **Usefulness:** High. This provides a simple way to get a list of all unique filenames in the bucket, which is useful for browsing or indexing.
    *   **Implementability:** **High.** This can be implemented with a simple and efficient SQL query: `SELECT DISTINCT filename FROM fs_files`.

### Group 2: Useful Metadata Fields

These features involve adding more structured metadata to the file document, which is useful for web applications and comprehensive file management.

*   **`content_type` property**
    *   **Usefulness:** Medium to High. Storing a file's MIME type (e.g., `'image/png'`, `'application/pdf'`) is crucial for web applications that need to serve files with the correct `Content-Type` header.
    *   **Implementability:** **High.** This requires a schema modification. A `content_type TEXT` column would be added to the `fs_files` table. The `open_upload_stream` methods would be updated to accept a `content_type` parameter, and the `GridOut` object would expose it as a property.

*   **`aliases` property**
    *   **Usefulness:** Medium. This allows a single file to be found under multiple names, enhancing discoverability and organization.
    *   **Implementability:** **High.** This would also be a schema modification. An `aliases TEXT` column would be added to `fs_files`. Since aliases are a list of strings, they would be stored as a JSON array string (e.g., `'["alias1", "alias2"]'`). The `find` logic would then need to be updated to also search within this JSON array.

### Group 3: Convenience Aliases

This group contains methods that are purely for convenience and do not introduce new core functionality.

*   **`get(file_id)`**
    *   **Usefulness:** Low. It's a direct alias for `open_download_stream(file_id)`. While it makes the code slightly more concise, it adds no new capability.
    *   **Implementability:** **Trivial.** It would be a one-line method: `def get(self, file_id): return self.open_download_stream(file_id)`.

---

**Overall Conclusion:**

All identified missing features are **highly implementable** within the existing `neosqlite` framework. Their implementation primarily involves standard SQL querying techniques (`LIMIT`, `ORDER BY`, `DISTINCT`) and straightforward schema adjustments (`ALTER TABLE ... ADD COLUMN ...`). No fundamental changes to the architectural approach would be required to integrate these features.