"""Tests for change-stream hardening (resume tokens, pipeline filter)."""

from datetime import datetime, timedelta, timezone

import pytest

import neosqlite


@pytest.fixture
def connection():
    conn = neosqlite.Connection(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def jobs(connection):
    return connection["jobs"]


def _next(stream, timeout_ms=2000):
    stream._max_await_time_ms = timeout_ms
    return next(stream)


def test_pipeline_match_operation_type(jobs):
    stream = jobs.watch(pipeline=[{"$match": {"operationType": "insert"}}])
    try:
        inserted = jobs.insert_one({"n": 1})
        jobs.update_one({"_id": inserted.inserted_id}, {"$set": {"n": 2}})
        jobs.delete_one({"_id": inserted.inserted_id})
        change = _next(stream)
        assert change["operationType"] == "insert"
        with pytest.raises(StopIteration):
            _next(stream, timeout_ms=200)
    finally:
        stream.close()


def test_pipeline_match_in_operator(jobs):
    stream = jobs.watch(
        pipeline=[{"$match": {"operationType": {"$in": ["update", "delete"]}}}]
    )
    try:
        jobs.insert_one({"n": 1})
        inserted = jobs.insert_one({"n": 2})
        jobs.update_one({"_id": inserted.inserted_id}, {"$set": {"n": 3}})
        change = _next(stream)
        assert change["operationType"] == "update"
    finally:
        stream.close()


def test_pipeline_match_full_document_needs_update_lookup(jobs):
    stream = jobs.watch(
        pipeline=[{"$match": {"fullDocument.status": "pending"}}],
        full_document="updateLookup",
        max_await_time_ms=2000,
    )
    try:
        jobs.insert_one({"status": "pending", "n": 1})
        jobs.insert_one({"status": "done", "n": 2})
        change = next(stream)
        assert change["fullDocument"]["status"] == "pending"
        with pytest.raises(StopIteration):
            _next(stream, timeout_ms=200)
    finally:
        stream.close()


def test_pipeline_match_full_document_absent_without_lookup(jobs):
    stream = jobs.watch(
        pipeline=[{"$match": {"fullDocument.status": "pending"}}],
        max_await_time_ms=200,
    )
    try:
        jobs.insert_one({"status": "pending", "n": 1})
        with pytest.raises(StopIteration):
            next(stream)
    finally:
        stream.close()


def test_pipeline_match_comparison_operator(jobs):
    stream = jobs.watch(
        pipeline=[{"$match": {"fullDocument.attempts": {"$lte": 2}}}],
        full_document="updateLookup",
    )
    try:
        jobs.insert_one({"attempts": 0})
        jobs.insert_one({"attempts": 9})
        change = _next(stream)
        assert change["fullDocument"]["attempts"] == 0
        with pytest.raises(StopIteration):
            _next(stream, timeout_ms=200)
    finally:
        stream.close()


def test_pipeline_unsupported_stage_ignored(jobs):
    stream = jobs.watch(pipeline=[{"$project": {"operationType": 1}}])
    try:
        jobs.insert_one({"n": 1})
        change = _next(stream)
        assert change["operationType"] == "insert"
    finally:
        stream.close()


def test_resume_token_property(jobs):
    stream = jobs.watch()
    try:
        assert stream.resume_token is None
        jobs.insert_one({"n": 1})
        change = _next(stream)
        assert stream.resume_token == change["_id"]
    finally:
        stream.close()


def test_resume_after_replays_without_duplicates(jobs):
    first = jobs.watch()
    try:
        jobs.insert_one({"n": 1})
        jobs.insert_one({"n": 2})
        seen_first = _next(first)
        token = first.resume_token
        assert token is not None
    finally:
        first.close()
    resumed = jobs.watch(resume_after=token)
    try:
        change = _next(resumed)
        assert change["documentKey"] != seen_first["documentKey"]
        with pytest.raises(StopIteration):
            _next(resumed, timeout_ms=200)
    finally:
        resumed.close()


def test_resume_after_crash_misses_zero(jobs):
    stream = jobs.watch()
    seen_ids = []
    try:
        for n in range(3):
            jobs.insert_one({"n": n})
        for _ in range(2):
            seen_ids.append(_next(stream)["documentKey"]["_id"])
        token = stream.resume_token
    finally:
        stream.close()
    # Worker restarts; committed jobs must not be missed or repeated.
    restarted = jobs.watch(resume_after=token)
    try:
        change = _next(restarted)
        assert change["documentKey"]["_id"] not in seen_ids
        seen_ids.append(change["documentKey"]["_id"])
        with pytest.raises(StopIteration):
            _next(restarted, timeout_ms=200)
    finally:
        restarted.close()
    assert len(seen_ids) == 3


def test_start_after_behaves_like_resume_after(jobs):
    first = jobs.watch()
    try:
        jobs.insert_one({"n": 1})
        jobs.insert_one({"n": 2})
        _next(first)
        token = first.resume_token
    finally:
        first.close()
    restarted = jobs.watch(start_after=token)
    try:
        change = _next(restarted)
        assert change["operationType"] == "insert"
    finally:
        restarted.close()


def test_resume_and_start_after_mutually_exclusive(jobs):
    with pytest.raises(ValueError):
        jobs.watch(resume_after={"id": 1}, start_after={"id": 1})


def test_invalid_resume_token_rejected(jobs):
    with pytest.raises(ValueError):
        jobs.watch(resume_after={"_data": "bogus"})


def test_history_retained_after_close(jobs):
    stream = jobs.watch()
    jobs.insert_one({"n": 1})
    _next(stream)
    stream.close()
    rows = jobs.db.execute(
        "SELECT COUNT(*) FROM _neosqlite_changestream WHERE collection_name = ?",
        (jobs.name,),
    ).fetchone()[0]
    assert rows >= 1


def test_job_queue_recipe_drain_then_tail(jobs):
    now = datetime.now(timezone.utc)
    jobs.insert_many(
        [
            {
                "type": "send",
                "status": "pending",
                "attempts": 0,
                "next_run": now - timedelta(seconds=10),
            },
            {
                "type": "send",
                "status": "pending",
                "attempts": 0,
                "next_run": now + timedelta(seconds=3600),
            },
        ]
    )
    # Worker boot: startup drain of due jobs.
    due = list(jobs.find({"status": "pending", "next_run": {"$lte": now}}))
    assert len(due) == 1
    # Then tail new arrivals.
    stream = jobs.watch(
        pipeline=[{"$match": {"operationType": "insert"}}],
        max_await_time_ms=2000,
    )
    try:
        jobs.insert_one(
            {
                "type": "send",
                "status": "pending",
                "attempts": 0,
                "next_run": now,
            }
        )
        change = next(stream)
        assert change["operationType"] == "insert"
        assert change["ns"]["coll"] == "jobs"
    finally:
        stream.close()
