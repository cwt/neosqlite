"""P1 search index verbs mapped onto NeoSQLite FTS."""

import pytest
from nx_27017.nx_27017 import NeoSQLiteHandler


@pytest.fixture
def handler(tmp_path):
    h = NeoSQLiteHandler(str(tmp_path / "search.db"))
    yield h
    h.conn.close()


def _cmd(handler, body, req=1):
    _, res = handler.handle_command(
        {"request_id": req, "sections": [("body", body)]}
    )
    return res


class TestSearchIndexVerbs:
    def test_create_list_drop_roundtrip(self, handler):
        res = _cmd(
            handler,
            {
                "createSearchIndexes": "docs",
                "indexes": [
                    {
                        "name": "tidx",
                        "definition": {
                            "mappings": {
                                "fields": {"title": {"type": "string"}}
                            }
                        },
                    }
                ],
                "$db": "test",
            },
        )
        assert res["ok"] == 1
        assert res["indexesCreated"] == [{"name": "tidx"}]

        res = _cmd(
            handler,
            {
                "aggregate": "docs",
                "pipeline": [{"$listSearchIndexes": {}}],
                "$db": "test",
            },
        )
        names = [d["name"] for d in res["cursor"]["firstBatch"]]
        assert any("title" in name for name in names)

    def test_create_from_plain_field_list(self, handler):
        res = _cmd(
            handler,
            {
                "createSearchIndexes": "docs",
                "indexes": [{"definition": {"fields": ["body"]}}],
                "$db": "test",
            },
        )
        assert res["ok"] == 1

    def test_unparseable_definition_errors(self, handler):
        res = _cmd(
            handler,
            {
                "createSearchIndexes": "docs",
                "indexes": [{"definition": {"mappings": {}}}],
                "$db": "test",
            },
        )
        assert res["ok"] == 0

    def test_update_and_drop(self, handler):
        _cmd(
            handler,
            {
                "createSearchIndexes": "docs",
                "indexes": [{"definition": {"field": "title"}}],
                "$db": "test",
            },
        )
        res = _cmd(
            handler,
            {
                "updateSearchIndex": "docs",
                "name": "title",
                "definition": {
                    "mappings": {"fields": {"title": {"type": "string"}}}
                },
                "$db": "test",
            },
        )
        assert res["ok"] == 1
        res = _cmd(
            handler,
            {"dropSearchIndex": "docs", "name": "title", "$db": "test"},
        )
        assert res["ok"] == 1

    def test_drop_requires_name(self, handler):
        res = _cmd(handler, {"dropSearchIndex": "docs", "$db": "test"})
        assert res["ok"] == 0
