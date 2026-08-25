"""Regression tests for critical audit fixes (#87+).

Each test class documents the pre-fix failure and asserts MongoDB-consistent
behavior after the fix.
"""

import pytest

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
        found = [d["a"] for d in docs.find({"b": {"$ne": None}})]
        assert found == [3]

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
                {"_id": 2},                # missing
                {"_id": 3, "a": "txt"},
                {"_id": 4, "a": None},
                {"_id": 5, "a": 2.5},
            ]
        )
        return c

    def test_cursor_fallback_sort_mixed_types(self, mixed):
        rows = list(
            mixed.find({"$or": [{"a": {"$exists": True}}, {"x": 1}]}).sort(
                "a"
            )
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
        from neosqlite.collection.type_utils import bson_sort_key
        import datetime

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
            rows = sorted(
                d["_id"] for d in docs.aggregate([{"$unwind": "$a"}])
            )
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
        doc.update_one({"_id": 1}, {"$push": {"arr": {"$each": [9], "$position": 1}}})
        assert doc.find_one({"_id": 1})["arr"] == [1, 9, 2, 3]

    def test_negative_position_counts_from_end(self, doc):
        doc.update_one({"_id": 1}, {"$push": {"arr": {"$each": [7], "$position": -2}}})
        assert doc.find_one({"_id": 1})["arr"] == [1, 2, 7, 3]

    def test_positive_slice_keeps_head(self, doc):
        doc.update_one({"_id": 1}, {"$push": {"arr": {"$each": [4, 5], "$slice": 3}}})
        assert doc.find_one({"_id": 1})["arr"] == [1, 2, 3]

    def test_negative_slice_keeps_tail(self, doc):
        doc.update_one(
            {"_id": 1},
            {"$push": {"arr": {"$each": [4, 5], "$slice": -3}}},
        )
        assert doc.find_one({"_id": 1})["arr"] == [3, 4, 5]

    def test_zero_slice_empties_array(self, doc):
        doc.update_one({"_id": 1}, {"$push": {"arr": {"$each": [9], "$slice": 0}}})
        assert doc.find_one({"_id": 1})["arr"] == []

    def test_update_many_no_longer_crashes_on_position(self, doc):
        doc.update_many({}, {"$push": {"arr": {"$each": [55], "$position": 1}}})
        assert doc.find_one({"_id": 1})["arr"] == [1, 55, 2, 3]

    def test_plain_push_unaffected(self, doc):
        doc.update_one({"_id": 1}, {"$push": {"arr": 42}})
        assert doc.find_one({"_id": 1})["arr"] == [1, 2, 3, 42]
