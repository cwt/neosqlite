from datetime import datetime, timezone

from neosqlite.collection.json_helpers import neosqlite_json_loads


def test_neosqlite_json_loads_dates_in_lists():
    # Test date decoding inside simple lists with $date marker
    json_str = '{"dates": [{"$date": "2026-07-13T22:56:21Z"}, "not-a-date"]}'
    res = neosqlite_json_loads(json_str)
    assert isinstance(res["dates"][0], datetime)
    assert res["dates"][0] == datetime(
        2026, 7, 13, 22, 56, 21, tzinfo=timezone.utc
    )
    assert res["dates"][1] == "not-a-date"

    # Test plain ISO strings are NOT converted to datetime (BUG-16: no data mutation)
    json_str_plain = '{"dates": ["2026-07-13T22:56:21Z", "not-a-date"]}'
    res_plain = neosqlite_json_loads(json_str_plain)
    assert isinstance(res_plain["dates"][0], str)
    assert res_plain["dates"][0] == "2026-07-13T22:56:21Z"

    # Test date decoding inside nested lists
    json_str_nested = '{"matrix": [[{"$date": "2026-07-13T22:56:21Z"}]]}'
    res_nested = neosqlite_json_loads(json_str_nested)
    assert isinstance(res_nested["matrix"][0][0], datetime)
    assert res_nested["matrix"][0][0] == datetime(
        2026, 7, 13, 22, 56, 21, tzinfo=timezone.utc
    )

    # Test date decoding at top-level list
    json_str_top_list = '[{"$date": "2026-07-13T22:56:21Z"}]'
    res_top_list = neosqlite_json_loads(json_str_top_list)
    assert isinstance(res_top_list[0], datetime)
    assert res_top_list[0] == datetime(
        2026, 7, 13, 22, 56, 21, tzinfo=timezone.utc
    )


def test_plain_iso_strings_not_mutated_in_collection():
    import neosqlite

    with neosqlite.Connection(":memory:") as conn:
        coll = conn.test_iso_dates
        iso_str = "2026-07-13T22:56:21Z"
        dt = datetime(2026, 7, 13, 22, 56, 21, tzinfo=timezone.utc)

        coll.insert_one({"plain": iso_str, "date_obj": dt})
        doc = coll.find_one({})
        assert doc is not None
        assert isinstance(doc["plain"], str)
        assert doc["plain"] == iso_str
        assert isinstance(doc["date_obj"], datetime)
        assert doc["date_obj"] == dt
