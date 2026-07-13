from datetime import datetime, timezone

from neosqlite.collection.json_helpers import neosqlite_json_loads


def test_neosqlite_json_loads_dates_in_lists():
    # Test date decoding inside simple lists
    json_str = '{"dates": ["2026-07-13T22:56:21Z", "not-a-date"]}'
    res = neosqlite_json_loads(json_str)
    assert isinstance(res["dates"][0], datetime)
    assert res["dates"][0] == datetime(
        2026, 7, 13, 22, 56, 21, tzinfo=timezone.utc
    )
    assert res["dates"][1] == "not-a-date"

    # Test date decoding inside nested lists
    json_str_nested = '{"matrix": [["2026-07-13T22:56:21Z"]]}'
    res_nested = neosqlite_json_loads(json_str_nested)
    assert isinstance(res_nested["matrix"][0][0], datetime)
    assert res_nested["matrix"][0][0] == datetime(
        2026, 7, 13, 22, 56, 21, tzinfo=timezone.utc
    )

    # Test date decoding at top-level list
    json_str_top_list = '["2026-07-13T22:56:21Z"]'
    res_top_list = neosqlite_json_loads(json_str_top_list)
    assert isinstance(res_top_list[0], datetime)
    assert res_top_list[0] == datetime(
        2026, 7, 13, 22, 56, 21, tzinfo=timezone.utc
    )
