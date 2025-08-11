from typing import List


class InsertOneResult:
    def __init__(self, inserted_id: int):
        self._inserted_id = inserted_id

    @property
    def inserted_id(self) -> int:
        return self._inserted_id


class InsertManyResult:
    def __init__(self, inserted_ids: List[int]):
        self._inserted_ids = inserted_ids

    @property
    def inserted_ids(self) -> List[int]:
        return self._inserted_ids


class UpdateResult:
    def __init__(
        self,
        matched_count: int,
        modified_count: int,
        upserted_id: int | None,
    ):
        self._matched_count = matched_count
        self._modified_count = modified_count
        self._upserted_id = upserted_id

    @property
    def matched_count(self) -> int:
        return self._matched_count

    @property
    def modified_count(self) -> int:
        return self._modified_count

    @property
    def upserted_id(self) -> int | None:
        return self._upserted_id


class DeleteResult:
    def __init__(self, deleted_count: int):
        self._deleted_count = deleted_count

    @property
    def deleted_count(self) -> int:
        return self._deleted_count


class BulkWriteResult:
    def __init__(
        self,
        inserted_count: int,
        matched_count: int,
        modified_count: int,
        deleted_count: int,
        upserted_count: int,
    ):
        self.inserted_count = inserted_count
        self.matched_count = matched_count
        self.modified_count = modified_count
        self.deleted_count = deleted_count
        self.upserted_count = upserted_count
