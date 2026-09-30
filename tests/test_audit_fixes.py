"""Regression tests for critical audit fixes (#87+).

Each test class documents the pre-fix failure and asserts MongoDB-consistent
behavior after the fix.
"""

import pytest

import neosqlite
from neosqlite.exceptions import MalformedQueryException


class TestUpdateManyIncMulValidation:
    """#87: update_many's direct-SQL path skipped $inc/$mul type validation.

    SQLite coerces 'hello' + 1 to 1, so update_many silently rewrote string
    fields to numbers. It now applies the same validation as update_one and
    falls back to the Python tier, which raises MalformedQueryException.
    """

    def test_inc_on_string_field_raises_instead_of_corrupting(self, connection):
        c = connection.t
        c.insert_many([{"a": "hello"}, {"a": "world"}])
        with pytest.raises(MalformedQueryException):
            c.update_many({}, {"$inc": {"a": 1}})
        # No partial corruption from the fast path
        assert sorted(d["a"] for d in c.find({})) == ["hello", "world"]

    def test_mul_on_string_field_raises(self, connection):
        c = connection.t
        c.insert_many([{"a": "hello"}])
        with pytest.raises(MalformedQueryException):
            c.update_many({}, {"$mul": {"a": 2}})

    def test_numeric_inc_fast_path_unaffected(self, connection):
        c = connection.t
        c.insert_many([{"n": 1}, {"n": 2}])
        res = c.update_many({}, {"$inc": {"n": 10}})
        assert res.modified_count == 2
        assert [d["n"] for d in c.find({})] == [11, 12]

    def test_filtered_validation_only_checks_matched_docs(self, connection):
        """Docs excluded by the filter must not block a valid $inc."""
        c = connection.t
        c.insert_many([{"kind": "num", "v": 5}, {"kind": "str", "v": "x"}])
        res = c.update_many({"kind": "num"}, {"$inc": {"v": 1}})
        assert res.modified_count == 1
        docs = {d["kind"]: d["v"] for d in c.find({})}
        assert docs == {"num": 6, "str": "x"}


class TestMinMaxMissingField:
    """#88: SQL-tier $min/$max wrote JSON null when the field was missing."""

    def test_min_sets_value_when_field_missing(self, connection):
        c = connection.t
        c.insert_one({"_id": 1})
        c.update_one({"_id": 1}, {"$min": {"score": 5}})
        assert c.find_one({"_id": 1})["score"] == 5

    def test_max_sets_value_when_field_missing(self, connection):
        c = connection.t
        c.insert_one({"_id": 1})
        c.update_one({"_id": 1}, {"$max": {"score": 5}})
        assert c.find_one({"_id": 1})["score"] == 5

    def test_min_keeps_smaller_existing_value(self, connection):
        c = connection.t
        c.insert_one({"_id": 1, "score": 3})
        c.update_one({"_id": 1}, {"$min": {"score": 5}})
        assert c.find_one({"_id": 1})["score"] == 3

    def test_max_keeps_larger_existing_value(self, connection):
        c = connection.t
        c.insert_one({"_id": 1, "score": 9})
        c.update_one({"_id": 1}, {"$max": {"score": 5}})
        assert c.find_one({"_id": 1})["score"] == 9


class TestAddToSetNoDocumentNesting:
    """#89: the fallback $addToSet clause assigned the whole document into
    the array field — for existing elements via THEN data, and for new ones
    because json_insert(data, ...) returns the full modified document."""

    def _apply_clause(self, connection, col, value):
        clause, params = col.query_engine.helpers._build_sql_update_clause(
            "$addToSet", value
        )
        wrapped = f"jsonb_set(data, {clause[0]})"
        connection.db.execute(
            f"UPDATE {col.name} SET data = {wrapped} WHERE id = 1", params
        )
        connection.db.commit()

    def test_add_to_set_existing_element_keeps_array_intact(self, connection):
        c = connection.t
        c.insert_one({"_id": 1, "arr": [1, 2]})
        self._apply_clause(connection, c, {"arr": 1})
        doc = c.find_one({"_id": 1})
        assert doc["arr"] == [1, 2], f"got {doc!r}"

    def test_add_to_set_new_element_appends_via_clause(self, connection):
        c = connection.t
        c.insert_one({"_id": 1, "arr": [1, 2]})
        self._apply_clause(connection, c, {"arr": 3})
        assert sorted(c.find_one({"_id": 1})["arr"]) == [1, 2, 3]

    def test_add_to_set_via_update_one_stays_correct(self, connection):
        c = connection.t
        c.insert_one({"_id": 1, "arr": [1, 2]})
        c.update_one({"_id": 1}, {"$addToSet": {"arr": 3}})
        c.update_one({"_id": 1}, {"$addToSet": {"arr": 3}})
        assert sorted(c.find_one({"_id": 1})["arr"]) == [1, 2, 3]


class TestNullMatchSemantics:
    """#90: {f: null} must match null-or-missing; $ne must not exclude
    documents whose field is missing — in both SQL tiers."""

    @pytest.fixture
    def docs(self, connection):
        c = connection.t
        c.insert_many([{"a": 1, "b": None}, {"a": 2}, {"a": 3, "b": "x"}])
        return c

    def test_equality_null_matches_null_and_missing(self, docs):
        found = sorted(d["a"] for d in docs.find({"b": None}))
        assert found == [1, 2]

    def test_eq_operator_null_matches_null_and_missing(self, docs):
        found = sorted(d["a"] for d in docs.find({"b": {"$eq": None}}))
        assert found == [1, 2]

    def test_ne_excludes_only_matching_values_not_missing(self, docs):
        found = sorted(d["a"] for d in docs.find({"b": {"$ne": "x"}}))
        assert found == [1, 2]

    def test_ne_still_excludes_the_value_itself(self, docs):
        found = sorted(d["a"] for d in docs.find({"b": {"$ne": None}}))
        assert found == [2, 3]

    def test_tier1_group_pipeline_match_null(self, connection):
        """Tier-1 CTE match builder honors the same semantics."""
        c = connection.t1
        c.insert_many([{"g": None}, {"g": "v"}, {}])
        rows = list(
            c.aggregate(
                [
                    {"$match": {"g": {"$ne": "v"}}},
                    {"$group": {"_id": None, "n": {"$sum": 1}}},
                ]
            )
        )
        assert rows and rows[0]["n"] == 2

    def test_tier1_match_equality_null(self, connection):
        c = connection.t1
        c.insert_many([{"g": None}, {"g": "v"}, {}])
        found = list(c.aggregate([{"$match": {"g": None}}]))
        # Both the explicit-null and the missing-field docs match
        assert len(found) == 2
        assert all(d.get("g") is None for d in found)

    def test_ne_null_in_aggregate_and_python_fallback(self, docs):
        # In aggregation match stage
        rows = list(docs.aggregate([{"$match": {"b": {"$ne": None}}}]))
        assert sorted(d["a"] for d in rows) == [2, 3]

        # In Python fallback
        from neosqlite.collection.query_helper import set_force_fallback

        set_force_fallback(True)
        try:
            py_rows = list(docs.find({"b": {"$ne": None}}))
            assert sorted(d["a"] for d in py_rows) == [2, 3]
        finally:
            set_force_fallback(False)


class TestTextSearchCombinedFilters:
    """#91: $text used to silently drop every sibling filter condition."""

    @pytest.fixture
    def news(self, connection):
        c = connection.news
        c.insert_many(
            [
                {"title": "war report", "category": "politics"},
                {"title": "war economy", "category": "business"},
            ]
        )
        c.create_search_index("title")
        return c

    def test_text_plus_filter_returns_only_matching_both(self, news):
        found = sorted(
            d["category"]
            for d in news.find(
                {"category": "politics", "$text": {"$search": "war"}}
            )
        )
        assert found == ["politics"]

    def test_text_alone_still_matches_all_indexes(self, news):
        found = sorted(
            d["category"] for d in news.find({"$text": {"$search": "war"}})
        )
        assert found == ["business", "politics"]

    def test_text_plus_nonmatching_filter_returns_nothing(self, news):
        found = list(
            news.find({"category": "sports", "$text": {"$search": "war"}})
        )
        assert found == []


class TestTier1GroupCteInvariant:
    """#94: Tier-1 $group/$bucket SQL violated the (id,_id,data) CTE
    invariant, so every such pipeline silently fell back to Python."""

    @pytest.fixture
    def sales(self, connection):
        c = connection.sales
        c.insert_many(
            [
                {"cat": "a", "amt": 10},
                {"cat": "a", "amt": 20},
                {"cat": "b", "amt": 5},
            ]
        )
        return c

    def test_group_executes_in_tier1_with_correct_results(self, sales):
        rows = list(
            sales.aggregate(
                [
                    {
                        "$group": {
                            "_id": "$cat",
                            "total": {"$sum": "$amt"},
                            "n": {"$sum": 1},
                        }
                    }
                ]
            )
        )
        assert {d["_id"]: d["total"] for d in rows} == {"a": 30, "b": 5}
        assert {d["_id"]: d["n"] for d in rows} == {"a": 2, "b": 1}

    def test_group_output_feeds_downstream_match(self, sales):
        rows = list(
            sales.aggregate(
                [
                    {"$group": {"_id": "$cat", "total": {"$sum": "$amt"}}},
                    {"$match": {"total": {"$gt": 10}}},
                ]
            )
        )
        assert [d["_id"] for d in rows] == ["a"]

    def test_group_push_returns_real_array(self, connection):
        c = connection.t
        c.insert_many([{"cat": "a", "tag": "x"}, {"cat": "a", "tag": None}])
        rows = list(
            c.aggregate(
                [{"$group": {"_id": "$cat", "tags": {"$push": "$tag"}}}]
            )
        )
        assert rows == [{"_id": "a", "tags": ["x", None]}]

    def test_constant_key_group_on_empty_input_yields_no_rows(self, connection):
        c = connection.empty
        rows = list(c.aggregate([{"$group": {"_id": None, "n": {"$sum": 1}}}]))
        assert rows == []

    def test_bucket_boundaries_and_accumulators(self, sales):
        rows = list(
            sales.aggregate(
                [
                    {
                        "$bucket": {
                            "groupBy": "$amt",
                            "boundaries": [0, 10, 100],
                            "default": "other",
                            "output": {"count": {"$sum": 1}},
                        }
                    }
                ]
            )
        )
        assert rows == [{"_id": 0, "count": 1}, {"_id": 10, "count": 2}]


class TestBucketAutoAccumulatesSpecifiedField:
    """#95: $bucketAuto output accumulators summed the groupBy field
    instead of the accumulator's own field expression."""

    def test_sum_uses_target_field(self, connection):
        c = connection.b
        c.insert_many(
            [
                {"g": 1, "qty": 10},
                {"g": 1, "qty": 10},
                {"g": 2, "qty": 10},
                {"g": 2, "qty": 10},
            ]
        )
        rows = list(
            c.aggregate(
                [
                    {
                        "$bucketAuto": {
                            "groupBy": "$g",
                            "buckets": 2,
                            "output": {"totalQty": {"$sum": "$qty"}},
                        }
                    }
                ]
            )
        )
        assert sorted(d["totalQty"] for d in rows) == [20, 20]

    def test_id_reports_min_max_boundaries(self, connection):
        c = connection.b3
        c.insert_many([{"qty": 1}, {"qty": 2}, {"qty": 9}, {"qty": 10}])
        rows = list(
            c.aggregate([{"$bucketAuto": {"groupBy": "$qty", "buckets": 2}}])
        )
        pairs = sorted((d["_id"]["min"], d["_id"]["max"]) for d in rows)
        assert pairs == [(1, 2), (9, 10)]

    def test_degenerate_single_value_single_bucket(self, connection):
        c = connection.b2
        c.insert_many([{"qty": 10}, {"qty": 10}, {"qty": 10}])
        rows = list(
            c.aggregate([{"$bucketAuto": {"groupBy": "$qty", "buckets": 2}}])
        )
        assert len(rows) == 1


class TestGroupLiteralAccumulatorParity:
    """#94 follow-up: tier-3 accumulators treated string literals as field
    paths; constants must be pushed as-is to match the SQL tier."""

    def test_addtoset_literal_matches_across_tiers(self, connection):
        from neosqlite.collection.query_helper import set_force_fallback

        c = connection.lit
        c.insert_many([{"k": "A"}, {"k": "A"}])
        pipeline = [
            {"$group": {"_id": "$k", "vals": {"$addToSet": "constant"}}}
        ]
        set_force_fallback(False)
        t1 = list(c.aggregate(pipeline))
        set_force_fallback(True)
        t3 = list(c.aggregate(pipeline))
        set_force_fallback(False)
        assert t1 == t3 == [{"_id": "A", "vals": ["constant"]}]

    def test_object_literal_push_falls_back_instead_of_corrupting(
        self, connection
    ):
        c = connection.lit2
        c.insert_many([{"cat": "A", "name": "n1"}, {"cat": "A", "name": "n2"}])
        pipeline = [
            {
                "$group": {
                    "_id": "$cat",
                    "items": {"$push": {"name": "$name", "kind": "x"}},
                }
            }
        ]
        rows = list(c.aggregate(pipeline))
        assert len(rows) == 1
        items = rows[0]["items"]
        assert items == [
            {"name": "n1", "kind": "x"},
            {"name": "n2", "kind": "x"},
        ]


class TestIdColumnReferences:
    """#113: parse_json_path('_id') returns a bare token, so callers that
    interpolated it into json_extract(data, ...) generated invalid SQL.
    _id lives in a dedicated column; those sites now reference it directly
    (and $_id type expressions fall back to the Python tier)."""

    @pytest.fixture
    def numbered(self, connection):
        c = connection.t
        for i in (1, 2, 3):
            c.insert_one({"_id": i, "v": i})
        return c

    def test_min_max_on_id(self, numbered):
        assert list(numbered.find().min([("_id", 2)]))[0]["_id"] == 2
        assert list(numbered.find().max([("_id", 2)]))[0]["_id"] == 1

    def test_create_index_on_id(self, numbered):
        numbered.create_index("_id")  # must not raise bad JSON path

    def test_expr_on_id_falls_back_to_python(self, numbered):
        rows = list(
            numbered.aggregate([{"$match": {"$expr": {"$gt": ["$_id", 0]}}}])
        )
        assert len(rows) == 3

    def test_raw_batch_sort_by_id(self, numbered):
        import json

        rbc = numbered.find_raw_batches()
        rbc._sort = {"_id": -1}
        docs = []
        for batch in rbc:
            docs.extend(
                json.loads(line) for line in batch.decode().splitlines()
            )
        assert [d["v"] for d in docs] == [3, 2, 1]


class TestDateSerialization:
    """#112: NeoSQLiteJSONEncoder only special-cased datetime.datetime;
    plain datetime.date raised TypeError on insert."""

    def test_insert_date(self, connection):
        import datetime

        c = connection.t
        c.insert_one({"d": datetime.date(2023, 1, 15)})
        assert c.find_one({})["d"] == "2023-01-15"

    def test_datetime_still_round_trips(self, connection):
        import datetime

        c = connection.t
        c.insert_one({"ts": datetime.datetime(2024, 6, 1, 12, 30)})
        assert isinstance(c.find_one({})["ts"], datetime.datetime)


class TestBsonOrderedSort:
    """#102: Python-tier sorts crashed on missing/mixed-type sort keys.
    All four copies (cursor fallback, tier-3 $sort, window operators,
    fill stage) now share a BSON-ordered key: missing/null first ascending,
    then numbers, strings, objects, arrays, booleans, datetimes."""

    @pytest.fixture
    def mixed(self, connection):
        c = connection.mixed
        c.insert_many(
            [
                {"_id": 1, "a": 5},
                {"_id": 2},  # missing
                {"_id": 3, "a": "txt"},
                {"_id": 4, "a": None},
                {"_id": 5, "a": 2.5},
            ]
        )
        return c

    def test_cursor_fallback_sort_mixed_types(self, mixed):
        rows = list(
            mixed.find({"$or": [{"a": {"$exists": True}}, {"x": 1}]}).sort("a")
        )
        vals = [d.get("a") for d in rows]
        # None/missing first, numbers next, string last — never TypeError
        assert vals[0] is None
        assert [v for v in vals if isinstance(v, (int, float))] == [
            2.5,
            5,
        ]
        assert vals[-1] == "txt"

    def test_cursor_fallback_descending_puts_missing_last(self, mixed):
        rows = list(
            mixed.find({"$or": [{"a": {"$exists": True}}, {"x": 1}]}).sort(
                [("a", -1)]
            )
        )
        assert rows[-1].get("a") is None

    def test_tier3_group_sort_mixed(self, mixed):
        from neosqlite.collection.query_helper import set_force_fallback

        set_force_fallback(True)
        try:
            rows = list(mixed.aggregate([{"$sort": {"a": 1}}]))
        finally:
            set_force_fallback(False)
        assert rows[0].get("a") is None
        assert rows[-1]["a"] == "txt"

    def test_fill_stage_sort_survives_missing_keys(self, connection):
        from neosqlite.collection.query_helper import set_force_fallback

        c = connection.fs
        c.insert_many([{"t": 2, "v": 20}, {"t": 1, "v": 10}])
        pipeline = [
            {
                "$fill": {
                    "sortBy": {"t": 1},
                    "output": {"v": {"method": "locf"}},
                }
            }
        ]
        set_force_fallback(True)
        try:
            rows = list(c.aggregate(pipeline))
        finally:
            set_force_fallback(False)
        assert [d["v"] for d in sorted(rows, key=lambda d: d["t"])] == [
            10,
            20,
        ]

    def test_bson_sort_key_total_order(self):
        import datetime

        from neosqlite.collection.type_utils import bson_sort_key

        values = [None, 3, "s", True, [1], {"k": 1}, datetime.datetime.now()]
        keys = [bson_sort_key(v) for v in values]
        ordered = sorted(range(len(values)), key=lambda i: keys[i])
        ranks = [keys[i][0] for i in ordered]
        assert ranks == sorted(ranks)  # total order, no exceptions


class TestUnwindScalars:
    """#96: $unwind dropped documents whose field held a scalar (tier-3)
    and crashed on them (tier-2). MongoDB unwinds non-null scalars as a
    single element; null/missing/empty follow preserveNullAndEmptyArrays."""

    @pytest.fixture
    def docs(self, connection):
        c = connection.u
        c.insert_many(
            [
                {"_id": 1, "a": 5},
                {"_id": 2, "a": [1, 2]},
                {"_id": 3, "a": []},
                {"_id": 4, "b": 9},
                {"_id": 5, "a": None},
                {"_id": 6, "a": {"n": 1}},
            ]
        )
        return c

    def test_tier3_scalar_unwinds_as_single_element(self, docs):
        from neosqlite.collection.query_helper import set_force_fallback

        set_force_fallback(True)
        try:
            rows = sorted(d["_id"] for d in docs.aggregate([{"$unwind": "$a"}]))
        finally:
            set_force_fallback(False)
        assert rows == [1, 2, 2, 6]

    def test_tier3_preserve_keeps_null_missing_empty(self, docs):
        from neosqlite.collection.query_helper import set_force_fallback

        set_force_fallback(True)
        try:
            rows = list(
                docs.aggregate(
                    [
                        {
                            "$unwind": {
                                "path": "$a",
                                "preserveNullAndEmptyArrays": True,
                            }
                        }
                    ]
                )
            )
        finally:
            set_force_fallback(False)
        ids = sorted(d["_id"] for d in rows)
        assert ids == [1, 2, 2, 3, 4, 5, 6]
        assert all("a" not in d for d in rows if d["_id"] in (3, 4))

    def test_tier2_matches_tier3(self, docs):
        from neosqlite.collection.query_helper import set_force_fallback
        from neosqlite.collection.temporary_table_aggregation import (
            TemporaryTableAggregationProcessor,
        )

        proc = TemporaryTableAggregationProcessor(docs)
        for spec in (
            {"$unwind": "$a"},
            {
                "$unwind": {
                    "path": "$a",
                    "preserveNullAndEmptyArrays": True,
                }
            },
            {"$unwind": {"path": "$a", "includeArrayIndex": "idx"}},
        ):
            set_force_fallback(True)
            try:
                t3 = sorted(
                    (d["_id"], str(d.get("a")), d.get("idx"))
                    for d in docs.aggregate([spec])
                )
            finally:
                set_force_fallback(False)
            t2 = sorted(
                (d["_id"], str(d.get("a")), d.get("idx"))
                for d in proc.process_pipeline([spec])
            )
            assert t3 == t2, spec


class TestPushPositionAndSlice:
    """#97/#98: $push $position generated LIMIT-before-UNION-ALL (rejected
    by SQLite, hard-crashing update_many) and negative $slice silently kept
    everything. A shared clause builder now assembles ordered blocks and
    implements MongoDB slice semantics in all three SQL builders."""

    @pytest.fixture
    def doc(self, connection):
        c = connection.t
        c.insert_one({"_id": 1, "arr": [1, 2, 3]})
        return c

    def test_position_inserts_at_index(self, doc):
        doc.update_one(
            {"_id": 1}, {"$push": {"arr": {"$each": [9], "$position": 1}}}
        )
        assert doc.find_one({"_id": 1})["arr"] == [1, 9, 2, 3]

    def test_negative_position_counts_from_end(self, doc):
        # MongoDB: insertion index = len(arr) + position
        doc.update_one(
            {"_id": 1}, {"$push": {"arr": {"$each": [7], "$position": -2}}}
        )
        assert doc.find_one({"_id": 1})["arr"] == [1, 7, 2, 3]

    def test_positive_slice_keeps_head(self, doc):
        doc.update_one(
            {"_id": 1}, {"$push": {"arr": {"$each": [4, 5], "$slice": 3}}}
        )
        assert doc.find_one({"_id": 1})["arr"] == [1, 2, 3]

    def test_negative_slice_keeps_tail(self, doc):
        doc.update_one(
            {"_id": 1},
            {"$push": {"arr": {"$each": [4, 5], "$slice": -3}}},
        )
        assert doc.find_one({"_id": 1})["arr"] == [3, 4, 5]

    def test_zero_slice_empties_array(self, doc):
        doc.update_one(
            {"_id": 1}, {"$push": {"arr": {"$each": [9], "$slice": 0}}}
        )
        assert doc.find_one({"_id": 1})["arr"] == []

    def test_update_many_no_longer_crashes_on_position(self, doc):
        doc.update_many({}, {"$push": {"arr": {"$each": [55], "$position": 1}}})
        assert doc.find_one({"_id": 1})["arr"] == [1, 55, 2, 3]

    def test_plain_push_unaffected(self, doc):
        doc.update_one({"_id": 1}, {"$push": {"arr": 42}})
        assert doc.find_one({"_id": 1})["arr"] == [1, 2, 3, 42]


class TestGroupByComplexKeys:
    """#103: $group by array/document _id crashed (tier-3) or returned
    corrupted binary keys. Complex values now group by canonical structure
    while the original value round-trips as _id."""

    @pytest.fixture
    def docs(self, connection):
        c = connection.g
        c.insert_many(
            [
                {"tags": [1, 2], "n": 1},
                {"tags": [1, 2], "n": 2},
                {"tags": [3], "n": 5},
            ]
        )
        return c

    def test_tier3_array_keys(self, docs):
        from neosqlite.collection.query_helper import set_force_fallback

        set_force_fallback(True)
        try:
            rows = list(
                docs.aggregate([{"$group": {"_id": "$tags", "n": {"$sum": 1}}}])
            )
        finally:
            set_force_fallback(False)
        by_key = {tuple(sorted(d["_id"])): d["n"] for d in rows}
        assert by_key == {(1, 2): 2, (3,): 1}

    def test_tier3_document_keys(self, connection):
        from neosqlite.collection.query_helper import set_force_fallback

        c = connection.g2
        c.insert_many([{"m": {"k": 1}}, {"m": {"k": 1}}, {"m": {"k": 2}}])
        set_force_fallback(True)
        try:
            rows = list(
                c.aggregate([{"$group": {"_id": "$m", "n": {"$sum": 1}}}])
            )
        finally:
            set_force_fallback(False)
        assert len(rows) == 2

    def test_tiers_agree_on_array_keys(self, docs):
        from neosqlite.collection.query_helper import set_force_fallback
        from neosqlite.collection.temporary_table_aggregation import (
            TemporaryTableAggregationProcessor,
        )

        pipeline = [{"$group": {"_id": "$tags", "total": {"$sum": "$n"}}}]
        proc = TemporaryTableAggregationProcessor(docs)
        t2 = sorted(
            (tuple(d["_id"]), d["total"])
            for d in proc.process_pipeline(pipeline)
        )
        set_force_fallback(True)
        try:
            t3 = sorted(
                (tuple(d["_id"]), d["total"]) for d in docs.aggregate(pipeline)
            )
        finally:
            set_force_fallback(False)
        assert t2 == t3 == [((1, 2), 3), ((3,), 5)]


class TestTier2EmptyGroup:
    """#104: a constant-key $group over empty input emitted a phantom
    aggregate row (SQLite bare aggregates always return one row)."""

    def test_constant_key_group_empty_input(self, connection):
        from neosqlite.collection.temporary_table_aggregation import (
            TemporaryTableAggregationProcessor,
        )

        c = connection.g
        proc = TemporaryTableAggregationProcessor(c)
        rows = proc.process_pipeline(
            [
                {"$match": {"g": "nope"}},
                {"$group": {"_id": None, "n": {"$sum": 1}}},
            ]
        )
        assert rows == []

    def test_populated_groups_unaffected(self, connection):
        from neosqlite.collection.temporary_table_aggregation import (
            TemporaryTableAggregationProcessor,
        )

        c = connection.g
        c.insert_many([{"g": "x", "n": 1}, {"g": "x", "n": 2}])
        proc = TemporaryTableAggregationProcessor(c)
        assert proc.process_pipeline(
            [{"$group": {"_id": None, "n": {"$sum": "$n"}}}]
        ) == [{"_id": None, "n": 3}]


class TestTier2FirstLast:
    """#105: tier-2 $first/$last generated invalid SQL (the group-key
    expression was qualified with a table alias) and never worked."""

    @pytest.fixture
    def docs(self, connection):
        c = connection.f
        c.insert_many(
            [
                {"cat": "a", "v": 10, "tag": "a1"},
                {"cat": "a", "v": 20, "tag": "a2"},
                {"cat": "b", "v": 5, "tag": "b1"},
            ]
        )
        return c

    def test_first_last_per_group(self, docs):
        from neosqlite.collection.temporary_table_aggregation import (
            TemporaryTableAggregationProcessor,
        )

        proc = TemporaryTableAggregationProcessor(docs)
        rows = proc.process_pipeline(
            [
                {
                    "$group": {
                        "_id": "$cat",
                        "firstV": {"$first": "$v"},
                        "lastV": {"$last": "$v"},
                    }
                }
            ]
        )
        got = {d["_id"]: d for d in rows}
        assert got["a"]["firstV"] == 10 and got["a"]["lastV"] == 20
        assert got["b"]["firstV"] == 5 and got["b"]["lastV"] == 5

    def test_constant_key_first_last(self, docs):
        from neosqlite.collection.temporary_table_aggregation import (
            TemporaryTableAggregationProcessor,
        )

        proc = TemporaryTableAggregationProcessor(docs)
        assert proc.process_pipeline(
            [
                {
                    "$group": {
                        "_id": None,
                        "f": {"$first": "$v"},
                        "l": {"$last": "$v"},
                    }
                }
            ]
        ) == [{"_id": None, "f": 10, "l": 5}]

    def test_null_group_keys_do_not_break(self, docs):
        from neosqlite.collection.temporary_table_aggregation import (
            TemporaryTableAggregationProcessor,
        )

        docs.insert_one({"v": 99})
        proc = TemporaryTableAggregationProcessor(docs)
        rows = proc.process_pipeline(
            [{"$group": {"_id": "$cat", "l": {"$last": "$v"}}}]
        )
        assert any(d["_id"] is None and d["l"] == 99 for d in rows)


class TestTier2LookupPipeline:
    """#107: $lookup with a sub-pipeline inserted every row with id=0 into
    an INTEGER PRIMARY KEY temp table, so any second matching row raised
    UNIQUE violation and the whole tier always fell back."""

    def test_multiple_matches_survive(self, connection):
        from neosqlite.collection.temporary_table_aggregation import (
            TemporaryTableAggregationProcessor,
        )

        users = connection.users
        orders = connection.orders
        users.insert_many([{"_id": 1, "name": "A"}, {"_id": 2, "name": "B"}])
        orders.insert_many(
            [
                {"uid": 1, "amt": 10},
                {"uid": 1, "amt": 20},
                {"uid": 1, "amt": 30},
                {"uid": 2, "amt": 5},  # filtered out by the pipeline
            ]
        )
        proc = TemporaryTableAggregationProcessor(users)
        rows = proc.process_pipeline(
            [
                {
                    "$lookup": {
                        "from": "orders",
                        "localField": "_id",
                        "foreignField": "uid",
                        "pipeline": [{"$match": {"amt": {"$gte": 10}}}],
                        "as": "ords",
                    }
                }
            ]
        )
        counts = {d["_id"]: len(d["ords"]) for d in rows}
        assert counts == {1: 3, 2: 0}


class TestHybridAddFieldsPreservesGroupId:
    """#108: after $group, the Python-hybrid $addFields wrote NULL into the
    _id column; a following stage that rebuilt data from columns (like
    $match) then nulled out every group key."""

    def test_group_addfields_match_keeps_keys(self, connection):
        from neosqlite.collection.temporary_table_aggregation import (
            TemporaryTableAggregationProcessor,
        )

        c = connection.h
        c.insert_many([{"cat": "a", "n": 3}, {"cat": "b", "n": 2}])
        proc = TemporaryTableAggregationProcessor(c)
        rows = proc.process_pipeline(
            [
                {"$group": {"_id": "$cat", "total": {"$sum": "$n"}}},
                {"$addFields": {"dbl": {"$multiply": ["$total", 2]}}},
                {"$match": {"total": {"$gt": 0}}},
            ]
        )
        assert {d["_id"]: d["dbl"] for d in rows} == {"a": 6, "b": 4}


class TestTier2Densify:
    """#106: tier-2 $densify built a malformed multi-row INSERT (never
    executed) and would have discarded original documents."""

    def test_densify_preserves_originals_and_fills_gaps(self, connection):
        from neosqlite.collection.temporary_table_aggregation import (
            TemporaryTableAggregationProcessor,
        )

        c = connection.d
        c.insert_many([{"t": 1, "v": "a"}, {"t": 3, "v": "b"}])
        proc = TemporaryTableAggregationProcessor(c)
        rows = proc.process_pipeline(
            [
                {
                    "$densify": {
                        "field": "t",
                        "range": {"step": 1, "bounds": [0, 4]},
                    }
                }
            ]
        )
        pairs = sorted((d.get("t"), d.get("v")) for d in rows)
        assert pairs == [(0, None), (1, "a"), (2, None), (3, "b"), (4, None)]


class TestPositionalDollarTargeting:
    """#99: positional $ resolved its array condition from the immediate
    path segment only, so dotted filters updated element 0 (or nothing).
    It now resolves the full prefix and refuses when the query constrains
    nothing on the array — matching MongoDB instead of corrupting data."""

    def test_dotted_filter_targets_matching_element(self, connection):
        c = connection.pd
        c.insert_one({"a": {"scores": [70, 90, 80]}})
        res = c.update_one({"a.scores": 90}, {"$set": {"a.scores.$": 100}})
        assert res.modified_count == 1
        assert c.find_one({})["a"]["scores"] == [70, 100, 80]

    def test_top_level_array_containment_filter(self, connection):
        c = connection.pd2
        c.insert_one({"scores": [80, 90, 100]})
        res = c.update_one({"scores": 90}, {"$set": {"scores.$": 95}})
        assert res.modified_count == 1
        assert c.find_one({})["scores"] == [80, 95, 100]

    def test_nested_element_field_targeting(self, connection):
        c = connection.pd3
        c.insert_one(
            {
                "students": [
                    {"name": "Alice", "grade": 85},
                    {"name": "Bob", "grade": 90},
                ]
            }
        )
        res = c.update_one(
            {"students.name": "Bob"}, {"$set": {"students.$.grade": 95}}
        )
        assert res.modified_count == 1
        doc = c.find_one({})
        assert doc["students"][1]["grade"] == 95

    def test_unrelated_condition_refuses_instead_of_element_zero(
        self, connection
    ):
        c = connection.pd4
        c.insert_one({"items": [1, 2, 3], "tags": ["x"]})
        res = c.update_one({"tags": "x"}, {"$set": {"items.$": 99}})
        assert res.matched_count == 1
        assert res.modified_count == 0, "must not write element 0"
        assert c.find_one({})["items"] == [1, 2, 3]


class TestArrayFilterOperatorValidation:
    """#100: unknown operators in arrayFilters silently matched every
    element; they now raise, and common operators ($size/$exists/$type)
    are supported."""

    def test_unknown_operator_raises(self):
        from neosqlite.collection.query_helper.positional_update import (
            _matches_query_operators,
        )

        with pytest.raises(ValueError):
            _matches_query_operators([1, 2], {"$weird": 1})

    def test_size_exists_type_supported(self):
        from neosqlite.collection.query_helper.positional_update import (
            _matches_query_operators,
        )

        assert _matches_query_operators([1, 2, 3], {"$size": 3})
        assert _matches_query_operators(7, {"$exists": True})
        assert not _matches_query_operators(7, {"$exists": False})
        assert _matches_query_operators(3, {"$type": "int"})


class TestUpsertBaseDocument:
    """#101: upsert copied the filter verbatim as the base document,
    storing operator dicts ({"age": {"$gte": 18}}) as literal data and
    keeping dotted keys flat. Equality-only extraction now matches
    MongoDB."""

    def test_operator_filters_not_stored(self, connection):
        c = connection.u
        c.update_one({"age": {"$gte": 18}}, {"$set": {"ok": 1}}, upsert=True)
        doc = c.find_one({"ok": 1})
        assert doc is not None
        assert "age" not in doc, "non-equality filter keys must be dropped"

    def test_dotted_equality_nests(self, connection):
        c = connection.u2
        c.update_one({"a.b": 1}, {"$set": {"c": 2}}, upsert=True)
        assert c.find_one({})["a"] == {"b": 1}

    def test_eq_operator_unwrapped(self, connection):
        c = connection.u3
        c.update_one(
            {"k": {"$eq": "v"}, "n": 5}, {"$set": {"x": 1}}, upsert=True
        )
        doc = c.find_one({})
        assert doc["k"] == "v" and doc["n"] == 5

    def test_and_flattened(self, connection):
        c = connection.u4
        c.update_one(
            {"$and": [{"p": 1}, {"q": 2}]}, {"$set": {"z": 9}}, upsert=True
        )
        doc = c.find_one({"z": 9})
        assert doc["p"] == 1 and doc["q"] == 2


class TestChangeStreamIsolation:
    """#110/#111: concurrent streams shared collection-scoped triggers and
    deleted rows on read (stealing each other's events); unconsumed rows
    accumulated forever. Streams now share refcounted triggers, consume via
    per-stream watermarks, and the last close drops triggers while retaining
    bounded recent history for resume_after replay."""

    def test_both_streams_see_same_events(self, connection):
        c = connection.cs
        s1, s2 = c.watch(), c.watch()
        c.insert_one({"n": 1})
        c.insert_one({"n": 2})
        ids1 = [next(s1)["documentKey"]["_id"], next(s1)["documentKey"]["_id"]]
        ids2 = [next(s2)["documentKey"]["_id"], next(s2)["documentKey"]["_id"]]
        assert ids1 == ids2
        s1.close()
        s2.close()

    def test_stream_survives_other_close(self, connection):
        c = connection.cs2
        s1, s2 = c.watch(), c.watch()
        c.insert_one({"n": 1})
        next(s1)
        s1.close()  # must NOT drop shared triggers
        c.insert_one({"n": 2})
        ev = next(s2)
        assert ev["documentKey"]["_id"] is not None
        s2.close()

    def test_new_stream_starts_from_now(self, connection):
        c = connection.cs3
        s0 = c.watch()
        c.insert_one({"n": 1})
        next(s0)
        s_late = c.watch()
        with pytest.raises(StopIteration):
            next(s_late)  # no replay of pre-open events
        s0.close()
        s_late.close()

    def test_last_close_purges_events_and_triggers(self, connection):
        from neosqlite.changestream import _RESUME_RETENTION_ROWS

        c = connection.cs4
        s = c.watch()
        c.insert_one({"n": 1})  # left unconsumed
        s.close()
        rows = connection.db.execute(
            "SELECT COUNT(*) FROM _neosqlite_changestream"
            " WHERE collection_name = ?",
            ("cs4",),
        ).fetchone()[0]
        triggers = connection.db.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger'"
            " AND name LIKE '%cs4%'"
        ).fetchone()[0]
        # Triggers are dropped, but bounded recent history is retained so a
        # later watch(resume_after=token) can still replay crash-restart gaps.
        assert rows <= _RESUME_RETENTION_ROWS
        assert triggers == 0

    def test_last_close_retains_history_for_resume(self, connection):
        c = connection.cs5
        s = c.watch()
        c.insert_one({"n": 1})
        first = next(s)
        token = s.resume_token
        s.close()
        replayed = c.watch(resume_after=token, max_await_time_ms=200)
        c.insert_one({"n": 2})
        try:
            second = next(replayed)
        finally:
            replayed.close()
        assert second["documentKey"]["_id"] != first["documentKey"]["_id"]


class TestUserBinaryDictionaryPreservation:
    """User dictionaries containing '__neosqlite_binary__' must not be treated as corrupted."""

    def test_user_dict_with_binary_key_missing_data(self, connection):
        c = connection.t_user_binary_1
        c.insert_one(
            {"user_shape": {"__neosqlite_binary__": True, "tag": "test"}}
        )
        doc = c.find_one({})
        assert doc is not None
        assert "__neosqlite_corrupted__" not in doc
        assert doc["user_shape"] == {
            "__neosqlite_binary__": True,
            "tag": "test",
        }

    def test_user_dict_with_binary_key_invalid_base64(self, connection):
        c = connection.t_user_binary_2
        c.insert_one(
            {
                "user_shape": {
                    "__neosqlite_binary__": True,
                    "data": "not_valid_b64!!!",
                }
            }
        )
        doc = c.find_one({})
        assert doc is not None
        assert "__neosqlite_corrupted__" not in doc
        assert doc["user_shape"] == {
            "__neosqlite_binary__": True,
            "data": "not_valid_b64!!!",
        }

    def test_actual_storage_corruption_still_detected(self, connection):
        c = connection.t_corrupt
        c.insert_one({"x": 1})
        connection.db.execute("UPDATE t_corrupt SET data = '{not valid json'")
        connection.db.commit()
        doc = c.find_one({})
        assert doc is not None
        assert doc.get("__neosqlite_corrupted__") is True


class TestFtsQuerySanitization:
    """FTS5 $text query sanitization tests for quotes, colons, hyphens, and leading NOT."""

    @pytest.fixture
    def fts_coll(self, connection):
        coll = connection.t_fts_test
        coll.insert_many(
            [
                {
                    "title": "Doc1",
                    "content": "hello:world test:term unclosed word",
                },
                {"title": "Doc2", "content": "hyphen-word and normal text"},
                {"title": "Doc3", "content": "leading NOT search text"},
                {"title": "Doc4", "content": "exact phrase match here"},
                {
                    "title": "Doc5",
                    "content": "test:term unclosed -word and NOT in text",
                },
            ]
        )
        coll.create_index("content", fts=True)
        return coll

    def test_fts_colon_in_query(self, fts_coll):
        results = list(fts_coll.find({"$text": {"$search": "test:term"}}))
        titles = [d["title"] for d in results]
        assert "Doc1" in titles
        assert "Doc5" in titles

        # Solitary colon should not crash
        results = list(fts_coll.find({"$text": {"$search": ":"}}))
        assert len(results) == 0

    def test_fts_quotes_in_query(self, fts_coll):
        # Unclosed quote
        results = list(fts_coll.find({"$text": {"$search": '"unclosed'}}))
        titles = [d["title"] for d in results]
        assert "Doc1" in titles
        assert "Doc5" in titles

        # Solitary quote
        results = list(fts_coll.find({"$text": {"$search": '"'}}))
        assert len(results) == 0

        # Exact phrase in quotes
        results = list(fts_coll.find({"$text": {"$search": '"exact phrase"'}}))
        assert len(results) == 1
        assert results[0]["title"] == "Doc4"

    def test_fts_hyphen_in_query(self, fts_coll):
        results = list(fts_coll.find({"$text": {"$search": "-word"}}))
        assert len(results) >= 1

        # Solitary hyphen should not crash
        results = list(fts_coll.find({"$text": {"$search": "-"}}))
        assert len(results) == 0

    def test_fts_leading_not_in_query(self, fts_coll):
        results = list(fts_coll.find({"$text": {"$search": "NOT search"}}))
        assert len(results) == 1
        assert results[0]["title"] == "Doc3"

        # Solitary NOT should not crash
        results = list(fts_coll.find({"$text": {"$search": "NOT"}}))
        titles = [d["title"] for d in results]
        assert "Doc3" in titles
        assert "Doc5" in titles

    def test_fts_complex_malformed_string(self, fts_coll):
        # Combination of colon, quote, hyphen, and NOT
        results = list(
            fts_coll.find(
                {"$text": {"$search": 'test:term "unclosed -word NOT'}}
            )
        )
        assert len(results) == 1
        assert results[0]["title"] == "Doc5"

    def test_fts_aggregation_pipeline_sanitization(self, fts_coll):
        # In aggregation temp-table text search stage
        pipeline = [{"$match": {"$text": {"$search": 'test:term "unclosed'}}}]
        results = list(fts_coll.aggregate(pipeline))
        titles = [d["title"] for d in results]
        assert "Doc1" in titles
        assert "Doc5" in titles

        # Empty/whitespace search in aggregation
        pipeline = [{"$match": {"$text": {"$search": '   "'}}}]
        results = list(fts_coll.aggregate(pipeline))
        assert len(results) == 0


class TestFindOneAndProjection:
    """find_one_and_* methods must apply projection in SQL and fallback paths."""

    def test_find_one_and_delete_projection(self, connection):
        c = connection.t_fop_delete
        c.insert_one({"name": "Alice", "secret": "s3cr3t", "role": "admin"})

        doc = c.find_one_and_delete(
            {"name": "Alice"},
            projection={"secret": 0},
        )
        assert doc is not None
        assert "secret" not in doc
        assert doc["name"] == "Alice"
        assert doc["role"] == "admin"
        assert "_id" in doc

        # Exclude _id
        c.insert_one({"name": "Bob", "secret": "s3cr3t", "role": "user"})
        doc2 = c.find_one_and_delete(
            {"name": "Bob"},
            projection={"_id": 0, "name": 1},
        )
        assert doc2 == {"name": "Bob"}

    def test_find_one_and_replace_projection(self, connection):
        c = connection.t_fop_replace
        c.insert_one({"name": "Alice", "secret": "s3cr3t", "role": "admin"})

        # return_document=False (returns original document with projection)
        doc = c.find_one_and_replace(
            {"name": "Alice"},
            {"name": "AliceUpdated", "secret": "new_secret", "role": "admin"},
            projection={"secret": 0},
            return_document=False,
        )
        assert doc is not None
        assert "secret" not in doc
        assert doc["name"] == "Alice"

        # return_document=True (returns replaced document with projection)
        doc_after = c.find_one_and_replace(
            {"name": "AliceUpdated"},
            {"name": "AliceFinal", "secret": "final_secret", "role": "admin"},
            projection={"secret": 0},
            return_document=True,
        )
        assert doc_after is not None
        assert "secret" not in doc_after
        assert doc_after["name"] == "AliceFinal"

    def test_find_one_and_update_projection(self, connection):
        c = connection.t_fop_update
        c.insert_one({"name": "Alice", "secret": "s3cr3t", "count": 1})

        # return_document=False (returns original document with projection)
        doc = c.find_one_and_update(
            {"name": "Alice"},
            {"$inc": {"count": 1}},
            projection={"secret": 0},
            return_document=False,
        )
        assert doc is not None
        assert "secret" not in doc
        assert doc["name"] == "Alice"
        assert doc["count"] == 1

        # return_document=True (returns updated document with projection)
        doc_after = c.find_one_and_update(
            {"name": "Alice"},
            {"$inc": {"count": 1}},
            projection={"secret": 0},
            return_document=True,
        )
        assert doc_after is not None
        assert "secret" not in doc_after
        assert doc_after["name"] == "Alice"
        assert doc_after["count"] == 3


class TestInNullSemantics:
    """$in: [null] and $nin: [null] semantics."""

    @pytest.fixture
    def docs(self, connection):
        c = connection.t_in_null
        c.insert_many(
            [
                {"_id": 1, "a": None, "b": 10},
                {"_id": 2, "b": 20},
                {"_id": 3, "a": 100, "b": 30},
                {"_id": 4, "a": [1, None, 2], "b": 40},
                {"_id": 5, "a": [3, 4], "b": 50},
            ]
        )
        return c

    def test_in_only_null(self, docs):
        results = sorted(d["_id"] for d in docs.find({"a": {"$in": [None]}}))
        assert results == [1, 2, 4]

    def test_in_null_and_values(self, docs):
        results = sorted(
            d["_id"] for d in docs.find({"a": {"$in": [100, None]}})
        )
        assert results == [1, 2, 3, 4]

    def test_in_null_aggregation(self, docs):
        results = sorted(
            d["_id"]
            for d in docs.aggregate([{"$match": {"a": {"$in": [None]}}}])
        )
        assert results == [1, 2, 4]

    def test_in_null_python_fallback(self, docs):
        from neosqlite.collection.query_helper import set_force_fallback

        set_force_fallback(True)
        try:
            results = sorted(
                d["_id"] for d in docs.find({"a": {"$in": [None]}})
            )
            assert results == [1, 2, 4]
        finally:
            set_force_fallback(False)

    def test_nin_null(self, docs):
        results = sorted(d["_id"] for d in docs.find({"a": {"$nin": [None]}}))
        assert results == [3, 5]

    def test_nin_null_and_values(self, docs):
        results = sorted(
            d["_id"] for d in docs.find({"a": {"$nin": [100, None]}})
        )
        assert results == [5]

    def test_in_id_null(self, connection):
        c = connection.t_in_id
        c.insert_many([{"_id": 1, "v": "a"}, {"_id": 2, "v": "b"}])
        results = list(c.find({"_id": {"$in": [1, None]}}))
        assert len(results) == 1
        assert results[0]["_id"] == 1


class TestCmpNullComparisons:
    """$cmp operator comparisons with null/None across tiers."""

    @pytest.fixture
    def docs(self, connection):
        c = connection.t_cmp_null
        c.insert_many(
            [
                {"_id": 1, "val": None},
                {"_id": 2, "val": 10},
                {"_id": 3, "val": 20},
                {"_id": 4},
            ]
        )
        return c

    def test_cmp_null_vs_int_python_fallback(self, docs):
        from neosqlite.collection.query_helper import set_force_fallback

        set_force_fallback(True)
        try:
            results = list(
                docs.aggregate(
                    [
                        {
                            "$project": {
                                "_id": 1,
                                "cmp_res": {"$cmp": ["$val", 10]},
                            }
                        },
                        {"$sort": {"_id": 1}},
                    ]
                )
            )
            assert [d["cmp_res"] for d in results] == [-1, 0, 1, -1]
        finally:
            set_force_fallback(False)

    def test_cmp_null_vs_int_sql(self, docs):
        from neosqlite.collection.query_helper import set_force_fallback

        set_force_fallback(False)
        results = list(
            docs.aggregate(
                [
                    {
                        "$project": {
                            "_id": 1,
                            "cmp_res": {"$cmp": ["$val", 10]},
                        }
                    },
                    {"$sort": {"_id": 1}},
                ]
            )
        )
        assert [d["cmp_res"] for d in results] == [-1, 0, 1, -1]

    def test_cmp_null_vs_null(self, docs):
        from neosqlite.collection.query_helper import set_force_fallback

        for fallback in (False, True):
            set_force_fallback(fallback)
            try:
                results = list(
                    docs.aggregate(
                        [
                            {"$match": {"_id": 1}},
                            {
                                "$project": {
                                    "_id": 1,
                                    "cmp_res": {"$cmp": ["$val", None]},
                                }
                            },
                        ]
                    )
                )
                assert results[0]["cmp_res"] == 0
            finally:
                set_force_fallback(False)

    def test_cmp_int_vs_null(self, docs):
        from neosqlite.collection.query_helper import set_force_fallback

        for fallback in (False, True):
            set_force_fallback(fallback)
            try:
                results = list(
                    docs.aggregate(
                        [
                            {"$match": {"_id": 1}},
                            {
                                "$project": {
                                    "_id": 1,
                                    "cmp_res": {"$cmp": [10, "$val"]},
                                }
                            },
                        ]
                    )
                )
                assert results[0]["cmp_res"] == 1
            finally:
                set_force_fallback(False)


class TestUpdateModifiedCount:
    """Accurate modified_count tracking for update_one and update_many."""

    def test_update_one_noop_modified_count(self, connection):
        c = connection.t_up_one_noop
        c.insert_one({"_id": 1, "a": 10})
        res = c.update_one({"_id": 1}, {"$set": {"a": 10}})
        assert res.matched_count == 1
        assert res.modified_count == 0

    def test_update_many_noop_and_partial_modified_count(self, connection):
        c = connection.t_up_many_noop
        c.insert_many(
            [
                {"_id": 1, "a": 10, "b": 1},
                {"_id": 2, "a": 10, "b": 2},
                {"_id": 3, "a": 20, "b": 3},
            ]
        )
        res = c.update_many({}, {"$set": {"a": 10}})
        assert res.matched_count == 3
        assert res.modified_count == 1

        res2 = c.update_many({}, {"$set": {"a": 10}})
        assert res2.matched_count == 3
        assert res2.modified_count == 0

    def test_update_many_array_filters_forwarding(self, connection):
        c = connection.t_up_af
        c.insert_many(
            [
                {"_id": 1, "grades": [75, 80, 85]},
                {"_id": 2, "grades": [90, 95, 100]},
            ]
        )
        res = c.update_many(
            {},
            {"$set": {"grades.$[elem]": 100}},
            array_filters=[{"elem": {"$gte": 85}}],
        )
        assert res.matched_count == 2
        assert res.modified_count == 2
        assert c.find_one({"_id": 1})["grades"] == [75, 80, 100]
        assert c.find_one({"_id": 2})["grades"] == [100, 100, 100]

        res2 = c.update_many(
            {},
            {"$set": {"grades.$[elem]": 100}},
            array_filters=[{"elem": {"$gte": 85}}],
        )
        assert res2.matched_count == 2
        assert res2.modified_count == 0


class TestPullOperatorConditions:
    """$pull operator with query conditions on scalar and document array elements."""

    def test_pull_scalar_condition_operator(self, connection):
        c = connection.t_pull_scalar
        c.insert_many(
            [
                {"_id": 1, "scores": [0, 45, 60, 85, 95]},
                {"_id": 2, "scores": [20, 30, 40]},
            ]
        )
        res = c.update_many({}, {"$pull": {"scores": {"$gte": 80}}})
        assert res.matched_count == 2
        assert res.modified_count == 1
        assert c.find_one({"_id": 1})["scores"] == [0, 45, 60]
        assert c.find_one({"_id": 2})["scores"] == [20, 30, 40]

    def test_pull_document_element_condition(self, connection):
        c = connection.t_pull_doc
        c.insert_one(
            {
                "_id": 1,
                "results": [
                    {"item": "A", "score": 60},
                    {"item": "B", "score": 85},
                    {"item": "C", "score": 90},
                ],
            }
        )
        res = c.update_one(
            {"_id": 1},
            {"$pull": {"results": {"score": {"$gte": 80}}}},
        )
        assert res.matched_count == 1
        assert res.modified_count == 1
        doc = c.find_one({"_id": 1})
        assert doc["results"] == [{"item": "A", "score": 60}]

    def test_pull_nested_field_condition(self, connection):
        c = connection.t_pull_nested
        c.insert_one(
            {
                "_id": 1,
                "data": {
                    "vals": [1, 5, 10, 15],
                },
            }
        )
        res = c.update_one(
            {"_id": 1},
            {"$pull": {"data.vals": {"$gt": 5}}},
        )
        assert res.matched_count == 1
        assert res.modified_count == 1
        doc = c.find_one({"_id": 1})
        assert doc["data"]["vals"] == [1, 5]

    def test_pull_regex_and_type_condition(self, connection):
        c = connection.t_pull_regex
        c.insert_one(
            {
                "_id": 1,
                "tags": ["apple", "banana", "AVOCADO", "cherry", 123, None],
            }
        )
        c.update_one(
            {"_id": 1},
            {"$pull": {"tags": {"$regex": "^a", "$options": "i"}}},
        )
        assert c.find_one({"_id": 1})["tags"] == [
            "banana",
            "cherry",
            123,
            None,
        ]

        c.update_one(
            {"_id": 1},
            {"$pull": {"tags": {"$type": "int"}}},
        )
        assert c.find_one({"_id": 1})["tags"] == ["banana", "cherry", None]

    def test_pull_size_and_in_condition(self, connection):
        c = connection.t_pull_size
        c.insert_one(
            {
                "_id": 1,
                "lists": [[1, 2], [1, 2, 3], [4]],
                "vals": [10, 20, 30, 40],
            }
        )
        c.update_one(
            {"_id": 1},
            {
                "$pull": {
                    "lists": {"$size": 2},
                    "vals": {"$in": [10, 30]},
                }
            },
        )
        doc = c.find_one({"_id": 1})
        assert doc["lists"] == [[1, 2, 3], [4]]
        assert doc["vals"] == [20, 40]


class TestBucketNoPhantomBuckets:
    """Python and SQL tier $bucket must not generate phantom buckets for
    values >= boundaries[-1]. Values outside boundaries must fall into the
    default bucket."""

    def test_bucket_no_phantom_and_default_routing(self, connection):
        c = connection.t_bucket_phantom
        c.insert_many(
            [
                {"val": -5},
                {"val": 5},
                {"val": 15},
                {"val": 20},
                {"val": 25},
                {"val": None},
                {"other": 1},  # missing val
            ]
        )
        pipeline = [
            {
                "$bucket": {
                    "groupBy": "$val",
                    "boundaries": [0, 10, 20],
                    "default": "Other",
                    "output": {"count": {"$sum": 1}},
                }
            }
        ]

        # Test standard execution (SQL / temp-table)
        res_sql = list(c.aggregate(pipeline))
        assert {r["_id"]: r["count"] for r in res_sql} == {
            0: 1,
            10: 1,
            "Other": 5,
        }

        # Test Python fallback execution
        from neosqlite.collection.query_helper.utils import (
            get_force_fallback,
            set_force_fallback,
        )

        old_state = get_force_fallback()
        try:
            set_force_fallback(True)
            res_py = list(c.aggregate(pipeline))
            assert {r["_id"]: r["count"] for r in res_py} == {
                0: 1,
                10: 1,
                "Other": 5,
            }
        finally:
            set_force_fallback(old_state)


class TestDensifyStepValidation:
    """$densify step must be greater than 0; step <= 0 must raise ValueError."""

    def test_densify_step_zero_or_negative_raises_error(self, connection):
        c = connection.t_densify_step
        c.insert_many([{"val": 1}, {"val": 5}])

        pipeline_zero = [
            {
                "$densify": {
                    "field": "val",
                    "range": {"bounds": [1, 5], "step": 0},
                }
            }
        ]
        with pytest.raises(ValueError, match="greater than 0"):
            list(c.aggregate(pipeline_zero))

        from neosqlite.collection.query_helper.utils import (
            get_force_fallback,
            set_force_fallback,
        )

        old_state = get_force_fallback()
        try:
            set_force_fallback(True)
            with pytest.raises(ValueError, match="greater than 0"):
                list(c.aggregate(pipeline_zero))

            pipeline_neg = [
                {
                    "$densify": {
                        "field": "val",
                        "range": {"bounds": [1, 5], "step": -1},
                    }
                }
            ]
            with pytest.raises(ValueError, match="greater than 0"):
                list(c.aggregate(pipeline_neg))
        finally:
            set_force_fallback(old_state)


class TestGroupLiteralAccumulators:
    """$group accumulator expressions with literals (e.g. $first: 1,
    $last: "constant") must not be dropped and must yield the literal values."""

    def test_group_literal_accumulators_in_all_tiers(self, connection):
        c = connection.t_group_literals
        c.insert_many(
            [
                {"grp": "A", "val": 10},
                {"grp": "A", "val": 20},
                {"grp": "B", "val": 30},
            ]
        )
        pipeline = [
            {
                "$group": {
                    "_id": "$grp",
                    "first_lit": {"$first": 1},
                    "last_lit": {"$last": "constant"},
                    "first_str": {"$first": "hello"},
                    "sum_lit": {"$sum": 5},
                    "avg_lit": {"$avg": 10},
                    "min_lit": {"$min": 7},
                    "max_lit": {"$max": 7},
                    "push_lit": {"$push": "item"},
                    "set_lit": {"$addToSet": 99},
                    "first_bool": {"$first": True},
                    "first_null": {"$first": None},
                }
            }
        ]

        def get_id(doc):
            return doc["_id"]

        # Standard execution (SQL / temp-table)
        res_sql = sorted(c.aggregate(pipeline), key=get_id)
        assert len(res_sql) == 2
        assert res_sql[0]["_id"] == "A"
        assert res_sql[0]["first_lit"] == 1
        assert res_sql[0]["last_lit"] == "constant"
        assert res_sql[0]["first_str"] == "hello"
        assert res_sql[0]["sum_lit"] == 10
        assert res_sql[0]["avg_lit"] == 10.0
        assert res_sql[0]["min_lit"] == 7
        assert res_sql[0]["max_lit"] == 7
        assert res_sql[0]["push_lit"] == ["item", "item"]
        assert res_sql[0]["set_lit"] == [99]
        assert res_sql[0]["first_null"] is None

        assert res_sql[1]["_id"] == "B"
        assert res_sql[1]["first_lit"] == 1
        assert res_sql[1]["last_lit"] == "constant"
        assert res_sql[1]["first_str"] == "hello"
        assert res_sql[1]["sum_lit"] == 5
        assert res_sql[1]["avg_lit"] == 10.0
        assert res_sql[1]["min_lit"] == 7
        assert res_sql[1]["max_lit"] == 7
        assert res_sql[1]["push_lit"] == ["item"]
        assert res_sql[1]["set_lit"] == [99]
        assert res_sql[1]["first_null"] is None

        # Python fallback execution
        from neosqlite.collection.query_helper.utils import (
            get_force_fallback,
            set_force_fallback,
        )

        old_state = get_force_fallback()
        try:
            set_force_fallback(True)
            res_py = sorted(c.aggregate(pipeline), key=get_id)
            assert res_py[0]["first_lit"] == res_sql[0]["first_lit"]
            assert res_py[0]["last_lit"] == res_sql[0]["last_lit"]
            assert res_py[0]["push_lit"] == res_sql[0]["push_lit"]
            assert res_py[0]["set_lit"] == res_sql[0]["set_lit"]
            assert res_py[0]["sum_lit"] == res_sql[0]["sum_lit"]
            assert res_py[0]["avg_lit"] == res_sql[0]["avg_lit"]
            assert res_py[0]["first_null"] is None
            assert res_py[1]["push_lit"] == res_sql[1]["push_lit"]
        finally:
            set_force_fallback(old_state)


class TestGroupPushRootVariable:
    """$group with {"$push": "$$ROOT"} must push the full document,
    not nulls."""

    def test_group_push_root_in_python_and_sql_tiers(self, connection):
        c = connection.t_group_root
        c.insert_many(
            [
                {"_id": 1, "grp": "A", "val": 10},
                {"_id": 2, "grp": "A", "val": 20},
                {"_id": 3, "grp": "B", "val": 30},
            ]
        )
        pipeline = [
            {
                "$group": {
                    "_id": "$grp",
                    "items": {"$push": "$$ROOT"},
                    "first_doc": {"$first": "$$ROOT"},
                }
            }
        ]

        def get_id(doc):
            return doc["_id"]

        # Standard tier
        res_sql = sorted(c.aggregate(pipeline), key=get_id)
        assert len(res_sql) == 2
        assert len(res_sql[0]["items"]) == 2
        assert res_sql[0]["items"][0]["val"] == 10
        assert res_sql[0]["items"][1]["val"] == 20
        assert res_sql[0]["first_doc"]["val"] == 10
        assert len(res_sql[1]["items"]) == 1
        assert res_sql[1]["items"][0]["val"] == 30
        assert res_sql[1]["first_doc"]["val"] == 30

        # Python tier
        from neosqlite.collection.query_helper.utils import (
            get_force_fallback,
            set_force_fallback,
        )

        old_state = get_force_fallback()
        try:
            set_force_fallback(True)
            res_py = sorted(c.aggregate(pipeline), key=get_id)
            assert len(res_py) == 2
            assert len(res_py[0]["items"]) == 2
            assert res_py[0]["items"][0]["val"] == 10
            assert res_py[0]["items"][1]["val"] == 20
            assert res_py[0]["first_doc"]["val"] == 10
            assert len(res_py[1]["items"]) == 1
            assert res_py[1]["items"][0]["val"] == 30
            assert res_py[1]["first_doc"]["val"] == 30
        finally:
            set_force_fallback(old_state)

    def test_group_root_template_and_fields(self, connection):
        c = connection.t_group_root_tmpl
        c.insert_many(
            [
                {"_id": 1, "grp": "A", "val": 10},
                {"_id": 2, "grp": "A", "val": 20},
            ]
        )
        pipeline = [
            {
                "$group": {
                    "_id": "$grp",
                    "docs": {
                        "$push": {
                            "doc_id": "$$ROOT._id",
                            "doc_val": "$$CURRENT.val",
                        }
                    },
                    "last_doc": {"$last": "$$ROOT"},
                    "first_val": {"$first": "$$ROOT.val"},
                    "last_val": {"$last": "$$CURRENT.val"},
                }
            }
        ]

        def get_id(doc):
            return doc["_id"]

        from neosqlite.collection.query_helper.utils import (
            get_force_fallback,
            set_force_fallback,
        )

        old_state = get_force_fallback()
        try:
            set_force_fallback(True)
            res = sorted(c.aggregate(pipeline), key=get_id)
            assert len(res) == 1
            assert res[0]["docs"] == [
                {"doc_id": 1, "doc_val": 10},
                {"doc_id": 2, "doc_val": 20},
            ]
            assert res[0]["last_doc"]["val"] == 20
            assert res[0]["first_val"] == 10
            assert res[0]["last_val"] == 20
        finally:
            set_force_fallback(old_state)


class TestDateAddMonthClamping:
    """$dateAdd month arithmetic clamps to last day of target month instead of rolling over."""

    def test_sql_tier_date_add_month_clamping(self, connection):
        import datetime
        from datetime import timezone

        c = connection.date_clamp_sql
        c.insert_many(
            [
                {
                    "_id": 1,
                    "name": "jan31_leap",
                    "d": datetime.datetime(
                        2024, 1, 31, 10, 0, 0, tzinfo=timezone.utc
                    ),
                },
                {
                    "_id": 2,
                    "name": "jan31_nonleap",
                    "d": datetime.datetime(
                        2023, 1, 31, 10, 0, 0, tzinfo=timezone.utc
                    ),
                },
                {
                    "_id": 3,
                    "name": "feb29_leap",
                    "d": datetime.datetime(
                        2024, 2, 29, 10, 0, 0, tzinfo=timezone.utc
                    ),
                },
                {
                    "_id": 4,
                    "name": "mar31",
                    "d": datetime.datetime(
                        2024, 3, 31, 10, 0, 0, tzinfo=timezone.utc
                    ),
                },
                {
                    "_id": 5,
                    "name": "aug31",
                    "d": datetime.datetime(
                        2024, 8, 31, 10, 0, 0, tzinfo=timezone.utc
                    ),
                },
                {
                    "_id": 6,
                    "name": "jan15",
                    "d": datetime.datetime(
                        2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc
                    ),
                },
            ]
        )

        # Test $dateAdd 1 month
        res = list(
            c.find(
                {
                    "$expr": {
                        "$eq": [
                            {"$dateAdd": ["$d", 1, "month"]},
                            datetime.datetime(
                                2024, 2, 29, 10, 0, 0, tzinfo=timezone.utc
                            ),
                        ]
                    }
                }
            )
        )
        assert len(res) == 1
        assert res[0]["_id"] == 1

        res = list(
            c.find(
                {
                    "$expr": {
                        "$eq": [
                            {"$dateAdd": ["$d", 1, "month"]},
                            datetime.datetime(
                                2023, 2, 28, 10, 0, 0, tzinfo=timezone.utc
                            ),
                        ]
                    }
                }
            )
        )
        assert len(res) == 1
        assert res[0]["_id"] == 2

        # Test $dateSubtract 1 month
        res = list(
            c.find(
                {
                    "$expr": {
                        "$eq": [
                            {"$dateSubtract": ["$d", 1, "month"]},
                            datetime.datetime(
                                2024, 2, 29, 10, 0, 0, tzinfo=timezone.utc
                            ),
                        ]
                    }
                }
            )
        )
        assert len(res) == 1
        assert res[0]["_id"] == 4

        # Test $dateAdd 1 year from leap day
        res = list(
            c.find(
                {
                    "$expr": {
                        "$eq": [
                            {"$dateAdd": ["$d", 1, "year"]},
                            datetime.datetime(
                                2025, 2, 28, 10, 0, 0, tzinfo=timezone.utc
                            ),
                        ]
                    }
                }
            )
        )
        assert len(res) == 1
        assert res[0]["_id"] == 3

        # Test non-clamping day preservation
        res = list(
            c.find(
                {
                    "$expr": {
                        "$eq": [
                            {"$dateAdd": ["$d", 1, "month"]},
                            datetime.datetime(
                                2024, 2, 15, 10, 0, 0, tzinfo=timezone.utc
                            ),
                        ]
                    }
                }
            )
        )
        assert len(res) == 1
        assert res[0]["_id"] == 6

        # Test aggregation pipeline
        docs = list(
            c.aggregate(
                [
                    {"$match": {"_id": 1}},
                    {"$project": {"clamped": {"$dateAdd": ["$d", 1, "month"]}}},
                ]
            )
        )
        assert len(docs) == 1
        assert docs[0]["clamped"] == datetime.datetime(
            2024, 2, 29, 10, 0, 0, tzinfo=timezone.utc
        )

    def test_python_tier_date_add_month_clamping(self, connection):
        import datetime
        from datetime import timezone

        from neosqlite.collection.query_helper.utils import (
            get_force_fallback,
            set_force_fallback,
        )

        c = connection.date_clamp_py
        c.insert_many(
            [
                {
                    "_id": 1,
                    "name": "jan31_leap",
                    "d": datetime.datetime(
                        2024, 1, 31, 10, 0, 0, tzinfo=timezone.utc
                    ),
                },
                {
                    "_id": 2,
                    "name": "jan31_nonleap",
                    "d": datetime.datetime(
                        2023, 1, 31, 10, 0, 0, tzinfo=timezone.utc
                    ),
                },
                {
                    "_id": 3,
                    "name": "feb29_leap",
                    "d": datetime.datetime(
                        2024, 2, 29, 10, 0, 0, tzinfo=timezone.utc
                    ),
                },
                {
                    "_id": 4,
                    "name": "mar31",
                    "d": datetime.datetime(
                        2024, 3, 31, 10, 0, 0, tzinfo=timezone.utc
                    ),
                },
            ]
        )

        old_state = get_force_fallback()
        try:
            set_force_fallback(True)
            res = list(
                c.aggregate(
                    [
                        {"$match": {"_id": 1}},
                        {
                            "$project": {
                                "clamped": {"$dateAdd": ["$d", 1, "month"]}
                            }
                        },
                    ]
                )
            )
            assert len(res) == 1
            assert res[0]["clamped"] == datetime.datetime(
                2024, 2, 29, 10, 0, 0, tzinfo=timezone.utc
            )

            res = list(
                c.aggregate(
                    [
                        {"$match": {"_id": 2}},
                        {
                            "$project": {
                                "clamped": {"$dateAdd": ["$d", 1, "month"]}
                            }
                        },
                    ]
                )
            )
            assert len(res) == 1
            assert res[0]["clamped"] == datetime.datetime(
                2023, 2, 28, 10, 0, 0, tzinfo=timezone.utc
            )
        finally:
            set_force_fallback(old_state)


class TestDateToStringMillisecond:
    """$dateToString with %L format specifier produces 3-digit padded milliseconds in both tiers."""

    def test_sql_tier_date_to_string_millisecond(self, connection):
        import datetime
        from datetime import timezone

        c = connection.dt_str_sql
        c.insert_many(
            [
                {
                    "_id": 1,
                    "d": datetime.datetime(
                        2024, 1, 15, 10, 30, 45, 123456, tzinfo=timezone.utc
                    ),
                },
                {
                    "_id": 2,
                    "d": datetime.datetime(
                        2024, 1, 15, 10, 30, 45, 5000, tzinfo=timezone.utc
                    ),
                },
            ]
        )

        res = list(
            c.aggregate(
                [
                    {
                        "$project": {
                            "full": {
                                "$dateToString": {
                                    "format": "%Y-%m-%d %H:%M:%S.%L",
                                    "date": "$d",
                                }
                            },
                            "ms": {
                                "$dateToString": {
                                    "format": "%L",
                                    "date": "$d",
                                }
                            },
                        }
                    }
                ]
            )
        )
        assert len(res) == 2
        docs = {doc["_id"]: doc for doc in res}
        assert docs[1]["full"] == "2024-01-15 10:30:45.123"
        assert docs[1]["ms"] == "123"
        assert docs[2]["full"] == "2024-01-15 10:30:45.005"
        assert docs[2]["ms"] == "005"

    def test_python_tier_date_to_string_millisecond(self, connection):
        import datetime
        from datetime import timezone

        from neosqlite.collection.query_helper.utils import (
            get_force_fallback,
            set_force_fallback,
        )

        c = connection.dt_str_py
        c.insert_many(
            [
                {
                    "_id": 1,
                    "d": datetime.datetime(
                        2024, 1, 15, 10, 30, 45, 123456, tzinfo=timezone.utc
                    ),
                },
                {
                    "_id": 2,
                    "d": datetime.datetime(
                        2024, 1, 15, 10, 30, 45, 5000, tzinfo=timezone.utc
                    ),
                },
            ]
        )

        old_state = get_force_fallback()
        try:
            set_force_fallback(True)
            res = list(
                c.aggregate(
                    [
                        {
                            "$project": {
                                "full": {
                                    "$dateToString": {
                                        "format": "%Y-%m-%d %H:%M:%S.%L",
                                        "date": "$d",
                                    }
                                },
                                "ms": {
                                    "$dateToString": {
                                        "format": "%L",
                                        "date": "$d",
                                    }
                                },
                            }
                        }
                    ]
                )
            )
            assert len(res) == 2
            docs = {doc["_id"]: doc for doc in res}
            assert docs[1]["full"] == "2024-01-15 10:30:45.123"
            assert docs[1]["ms"] == "123"
            assert docs[2]["full"] == "2024-01-15 10:30:45.005"
            assert docs[2]["ms"] == "005"
        finally:
            set_force_fallback(old_state)


class TestObjectIdGenerationTime:
    """ObjectId.generation_time is a property returning a timezone-aware UTC datetime."""

    def test_generation_time_property(self):
        import datetime
        from datetime import timezone

        from neosqlite.objectid import ObjectId

        ts = 1700000000
        oid = ObjectId(ts)
        gen_time = oid.generation_time
        assert isinstance(gen_time, datetime.datetime)
        assert gen_time.tzinfo == timezone.utc
        assert gen_time == datetime.datetime.fromtimestamp(ts, tz=timezone.utc)

        now_before = datetime.datetime.now(timezone.utc) - datetime.timedelta(
            seconds=1
        )
        fresh_oid = ObjectId()
        now_after = datetime.datetime.now(timezone.utc) + datetime.timedelta(
            seconds=1
        )
        assert now_before <= fresh_oid.generation_time <= now_after


class TestWatchInsideTransaction:
    """watch() inside an open transaction does not commit it."""

    def test_watch_does_not_commit_active_transaction(self, connection):
        c = connection.watch_tx_test
        c.insert_one({"init": 1})

        session = connection.start_session()
        session.start_transaction()
        assert connection.db.in_transaction

        c.insert_one({"x": 100}, session=session)
        assert connection.db.in_transaction

        stream = c.watch(session=session)
        assert connection.db.in_transaction

        session.abort_transaction()
        assert not connection.db.in_transaction
        assert list(c.find({"x": 100})) == []
        stream.close()


class TestStartTransactionContextManager:
    """with session.start_transaction(): commits on success and rolls back on exception."""

    def test_start_transaction_context_manager_commit(self, connection):
        users = connection.tx_ctx_users
        with connection.start_session() as session:
            with session.start_transaction():
                users.insert_one({"name": "Alice"}, session=session)
                users.insert_one({"name": "Bob"}, session=session)
        assert users.count_documents({}) == 2

    def test_start_transaction_context_manager_abort(self, connection):
        import pytest

        users = connection.tx_ctx_users_abort
        with connection.start_session() as session:
            with pytest.raises(RuntimeError):
                with session.start_transaction():
                    users.insert_one({"name": "Charlie"}, session=session)
                    raise RuntimeError("forced failure")
        assert users.count_documents({}) == 0


class TestRenameCollectionUnaccessedFTS:
    """Connection.rename_collection() on unaccessed collection renames FTS tables and indexes."""

    def test_rename_unaccessed_collection_renames_fts_and_indexes(
        self, connection
    ):
        c = connection.orig_articles
        c.insert_one({"title": "Advanced Python", "tag": "tech"})
        c.create_index("tag")
        c.create_index("title", fts=True)

        # Evict from Connection._collections to simulate an unaccessed collection
        connection._collections.pop("orig_articles", None)
        assert "orig_articles" not in connection._collections

        # Rename collection via Connection.rename_collection
        connection.rename_collection("orig_articles", "renamed_articles")

        new_c = connection.renamed_articles
        # Verify index was renamed and list_indexes works
        indexes = new_c.list_indexes()
        assert "idx_renamed_articles_tag" in indexes
        assert new_c.list_indexes(as_keys=True) == [["tag"]]

        # Verify FTS tables were renamed
        tables = [
            r[0]
            for r in connection.db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        ]
        assert "renamed_articles_title_fts" in tables
        assert not any(t.startswith("orig_articles") for t in tables)

        # Verify full-text search works on renamed collection
        results = list(new_c.find({"$text": {"$search": "Python"}}))
        assert len(results) == 1
        assert results[0]["title"] == "Advanced Python"

        # Verify drop_collection removes all FTS tables cleanly
        connection.drop_collection("renamed_articles")
        remaining = [
            r[0]
            for r in connection.db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        ]
        assert not any(
            t.startswith("renamed_articles") or t.startswith("orig_articles")
            for t in remaining
        )


def test_collection_find_one_validates_and_passes_session(connection):
    c = connection.test_find_one_session
    c.insert_one({"a": 1, "b": 2})

    session = connection.start_session()
    doc = c.find_one({"a": 1}, session=session)
    assert doc is not None
    assert doc["b"] == 2

    # Verify session from another connection raises ValueError
    other_conn = neosqlite.Connection(":memory:")
    other_session = other_conn.start_session()
    with pytest.raises(
        ValueError, match="Session belongs to a different Connection"
    ):
        c.find_one({"a": 1}, session=other_session)
    other_session.end_session()
    other_conn.close()
    session.end_session()


def test_gridfs_collection_find_one_validates_session(connection):
    from neosqlite.gridfs import GridFSBucket

    bucket = GridFSBucket(connection.db)
    file_id = bucket.upload_from_stream("test.txt", b"hello world")

    files_col = connection["fs_files"]
    session = connection.start_session()
    doc = files_col.find_one({"_id": str(file_id)}, session=session)
    assert doc is not None
    session.end_session()

    other_conn = neosqlite.Connection(":memory:")
    other_session = other_conn.start_session()
    with pytest.raises(
        ValueError, match="Session belongs to a different Connection"
    ):
        files_col.find_one({"_id": str(file_id)}, session=other_session)
    other_session.end_session()
    other_conn.close()


def test_find_negative_and_zero_limit(connection):
    c = connection.test_neg_limit
    docs = [{"_id": i, "val": i * 10} for i in range(10)]
    c.insert_many(docs)

    # find().limit(-1) should return 1 document, not all documents
    res_neg_1 = list(c.find().limit(-1))
    assert len(res_neg_1) == 1

    # find().limit(-3) should return 3 documents
    res_neg_3 = list(c.find().limit(-3))
    assert len(res_neg_3) == 3

    # find(limit=-2) argument
    res_kwarg = list(c.find(limit=-2))
    assert len(res_kwarg) == 2

    # find().limit(0) should return all documents (no limit)
    res_zero = list(c.find().limit(0))
    assert len(res_zero) == 10

    # Fallback Python-eval path with negative limit
    res_fallback = list(c.find({"$expr": {"$gte": ["$val", 0]}}).limit(-2))
    assert len(res_fallback) == 2

    # Cursor.alive property with negative limit
    cursor = c.find().limit(-2)
    assert cursor.alive is True
    list(cursor)
    assert cursor.alive is False


def test_aggregate_negative_limit_raises_error(connection):
    c = connection.test_neg_limit_agg
    c.insert_many([{"_id": i} for i in range(5)])

    with pytest.raises(
        ValueError, match=r"The \$limit stage requires a non-negative integer"
    ):
        list(c.aggregate([{"$limit": -1}]))

    # Zero limit in aggregation should return 0 documents
    res_zero = list(c.aggregate([{"$limit": 0}]))
    assert len(res_zero) == 0
