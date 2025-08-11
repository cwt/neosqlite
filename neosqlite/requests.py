from typing import Any, Dict


class InsertOne:
    def __init__(self, document: Dict[str, Any]):
        self.document = document


class UpdateOne:
    def __init__(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = False,
    ):
        self.filter = filter
        self.update = update
        self.upsert = upsert


class DeleteOne:
    def __init__(self, filter: Dict[str, Any]):
        self.filter = filter
