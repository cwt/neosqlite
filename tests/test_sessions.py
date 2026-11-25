from unittest.mock import patch

import pytest

import neosqlite
from neosqlite import DeleteOne, InsertOne, UpdateOne
from neosqlite.client_session import ClientSession
from neosqlite.exceptions import InvalidOperation
from neosqlite.gridfs import GridFSBucket


def test_client_session_basic():
    """Test ClientSession lifecycle."""
    with neosqlite.Connection(":memory:") as conn:
        with conn.start_session() as session:
            assert isinstance(session, ClientSession)
            assert session.client == conn
            assert session.in_transaction is False

            session.start_transaction()
            assert session.in_transaction is True

            coll = conn.test
            coll.insert_one({"name": "tx_test"}, session=session)

            session.commit_transaction()
            assert session.in_transaction is False
            assert coll.count_documents({"name": "tx_test"}) == 1


def test_session_validation_cross_connection():
    """Test that using a session from another connection raises ValueError."""
    with neosqlite.Connection(":memory:") as conn1:
        with neosqlite.Connection(":memory:") as conn2:
            with conn1.start_session() as session1:
                # Should work
                conn1.test.insert_one({"a": 1}, session=session1)

                # Should fail
                with pytest.raises(
                    ValueError,
                    match="Session belongs to a different Connection",
                ):
                    conn2.test.insert_one({"a": 1}, session=session1)

                with pytest.raises(
                    ValueError,
                    match="Session belongs to a different Connection",
                ):
                    list(conn2.test.find({}, session=session1))

                with pytest.raises(
                    ValueError,
                    match="Session belongs to a different Connection",
                ):
                    conn2.test.aggregate([], session=session1)


def test_session_in_aggregation_cursor():
    """Test that session is correctly passed to aggregation cursor."""
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.test
        coll.insert_one({"a": 1})
        with conn.start_session() as session:
            cursor = coll.aggregate([], session=session)
            assert cursor.session == session
            results = list(cursor)
            assert len(results) == 1


def test_session_in_gridfs():
    """Test session support in GridFSBucket."""
    with neosqlite.Connection(":memory:") as conn:
        bucket = GridFSBucket(conn.db)
        with conn.start_session() as session:
            # Test upload
            file_id = bucket.upload_from_stream(
                "test.txt", b"content", session=session
            )
            assert conn.fs_files.find_one({"_id": str(file_id)})

            # Test session validation in GridFS
            with neosqlite.Connection(":memory:") as conn2:
                bucket2 = GridFSBucket(conn2.db)
                with pytest.raises(
                    ValueError,
                    match="Session belongs to a different Connection",
                ):
                    bucket2.upload_from_stream(
                        "fail.txt", b"content", session=session
                    )


def test_session_abort():
    """Test aborting a transaction."""
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.test
        with conn.start_session() as session:
            session.start_transaction()
            coll.insert_one({"name": "to_abort"}, session=session)
            session.abort_transaction()

            assert coll.count_documents({"name": "to_abort"}) == 0


def test_session_with_transaction():
    """Test with_transaction convenience method."""
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.test
        with conn.start_session() as session:

            def callback(s):
                coll.insert_one({"name": "with_tx"}, session=s)
                return "success"

            result = session.with_transaction(callback)
            assert result == "success"
            assert coll.count_documents({"name": "with_tx"}) == 1

            # Test failure
            def failing_callback(s):
                coll.insert_one({"name": "should_rollback"}, session=s)
                raise ValueError("trigger rollback")

            try:
                session.with_transaction(failing_callback)
            except ValueError:
                pass

            assert coll.count_documents({"name": "should_rollback"}) == 0


def test_nested_transactions_savepoints():
    """Test nested ClientSession SAVEPOINT inside Connection.transaction()."""
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.test
        with conn.transaction():
            coll.insert_one({"name": "outer"})

            with conn.start_session() as session:
                session.start_transaction()  # Uses SAVEPOINT
                coll.insert_one({"name": "inner_abort"}, session=session)
                session.abort_transaction()  # Rollback inner only

            with conn.start_session() as session2:
                session2.start_transaction()  # Uses SAVEPOINT
                coll.insert_one({"name": "inner_commit"}, session=session2)
                session2.commit_transaction()

        assert coll.count_documents({"name": "outer"}) == 1
        assert coll.count_documents({"name": "inner_abort"}) == 0
        assert coll.count_documents({"name": "inner_commit"}) == 1


def test_connection_exit_on_exception_rolls_back_session_transaction(tmp_path):
    """Exiting Connection context on exception must roll back open session tx."""
    db_path = str(tmp_path / "bank.db")

    with pytest.raises(RuntimeError, match="application crashed"):
        with neosqlite.Connection(db_path) as db:
            db.accounts.insert_one({"user": "ok"})
            session = db.start_session()
            session.start_transaction()
            db.accounts.insert_one(
                {"user": "PARTIAL_UNCOMMITTED_WRITE"}, session=session
            )
            raise RuntimeError("application crashed mid-transaction")

    with neosqlite.Connection(db_path) as db2:
        docs = list(db2.accounts.find({}, {"_id": 0}))
        assert docs == [{"user": "ok"}]


def test_connection_close_rolls_back_uncommitted_session_transaction(tmp_path):
    """Explicit close() and normal context exit must roll back uncommitted tx."""
    db_path = str(tmp_path / "close_rollback.db")

    # Explicit close() with uncommitted transaction
    db = neosqlite.Connection(db_path)
    db.accounts.insert_one({"step": "committed"})
    session = db.start_session()
    session.start_transaction()
    db.accounts.insert_one({"step": "uncommitted_close"}, session=session)
    db.close()

    assert session.in_transaction is False
    assert db._active_tx_session is None

    # Normal context exit with uncommitted transaction
    with neosqlite.Connection(db_path) as db_ctx:
        s_ctx = db_ctx.start_session()
        s_ctx.start_transaction()
        db_ctx.accounts.insert_one(
            {"step": "uncommitted_ctx_exit"}, session=s_ctx
        )

    with neosqlite.Connection(db_path) as verify_db:
        docs = list(verify_db.accounts.find({}, {"_id": 0}))
        assert docs == [{"step": "committed"}]


def test_clone_close_does_not_rollback_parent_transaction():
    """Closing a with_options() clone must not disturb the parent transaction."""
    with neosqlite.Connection(":memory:") as conn:
        clone = conn.with_options(write_concern={"w": 1})
        with conn.start_session() as session:
            session.start_transaction()
            conn.items.insert_one({"name": "keep"}, session=session)
            clone.close()
            assert session.in_transaction is True
            session.commit_transaction()

        assert conn.items.count_documents({"name": "keep"}) == 1


def test_concurrent_sessions_overlapping_transactions_rejected():
    """Distinct ClientSessions on one Connection cannot overlap transactions."""
    with neosqlite.Connection(":memory:") as conn:
        s1 = conn.start_session()
        s2 = conn.start_session()

        s1.start_transaction()
        conn.items.insert_one({"from": "s1"}, session=s1)

        with pytest.raises(
            InvalidOperation, match="Concurrent transactions across multiple"
        ):
            s2.start_transaction()

        assert s2.in_transaction is False
        assert s1.in_transaction is True
        assert conn._active_tx_session is s1

        s1.commit_transaction()
        assert s1.in_transaction is False
        assert conn._active_tx_session is None

        # Once s1 finishes, s2 can cleanly start and abort/commit a transaction
        s2.start_transaction()
        conn.items.insert_one({"from": "s2_aborted"}, session=s2)
        s2.abort_transaction()

        assert s2.in_transaction is False
        assert conn._active_tx_session is None
        assert list(conn.items.find({}, {"_id": 0})) == [{"from": "s1"}]

        s1.end_session()
        s2.end_session()


def test_concurrent_sessions_across_with_options_clone_rejected():
    """A with_options() clone shares active transaction tracking with parent."""
    with neosqlite.Connection(":memory:") as conn:
        clone = conn.with_options(read_concern={"level": "local"})
        with conn.start_session() as s1, clone.start_session() as s2:
            s1.start_transaction()
            with pytest.raises(
                InvalidOperation,
                match="Concurrent transactions across multiple",
            ):
                s2.start_transaction()
            s1.abort_transaction()


def test_session_has_ended_lifecycle_and_rejects_reuse():
    """Ended sessions report has_ended=True and reject all subsequent usage."""
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.items
        coll.insert_one({"x": 1})
        bucket = GridFSBucket(conn.db)

        session = conn.start_session()
        assert session.has_ended is False

        session.end_session()
        assert session.has_ended is True

        # Idempotent second end_session()
        session.end_session()
        assert session.has_ended is True

        with pytest.raises(InvalidOperation, match="ended ClientSession"):
            session.start_transaction()
        with pytest.raises(InvalidOperation, match="ended ClientSession"):
            session.commit_transaction()
        with pytest.raises(InvalidOperation, match="ended ClientSession"):
            session.abort_transaction()

        def noop_callback(sess: ClientSession) -> None:
            return None

        with pytest.raises(InvalidOperation, match="ended ClientSession"):
            session.with_transaction(noop_callback)

        with pytest.raises(InvalidOperation, match="ended ClientSession"):
            coll.insert_one({"x": 2}, session=session)
        with pytest.raises(InvalidOperation, match="ended ClientSession"):
            coll.find_one({"x": 1}, session=session)
        with pytest.raises(InvalidOperation, match="ended ClientSession"):
            list(coll.find({}, session=session))
        with pytest.raises(InvalidOperation, match="ended ClientSession"):
            coll.update_one({"x": 1}, {"$set": {"x": 2}}, session=session)
        with pytest.raises(InvalidOperation, match="ended ClientSession"):
            coll.delete_one({"x": 1}, session=session)
        with pytest.raises(InvalidOperation, match="ended ClientSession"):
            coll.count_documents({}, session=session)
        with pytest.raises(InvalidOperation, match="ended ClientSession"):
            coll.distinct("x", session=session)
        with pytest.raises(InvalidOperation, match="ended ClientSession"):
            coll.aggregate([], session=session)
        with pytest.raises(InvalidOperation, match="ended ClientSession"):
            coll.watch(session=session)
        with pytest.raises(InvalidOperation, match="ended ClientSession"):
            bucket.upload_from_stream("f.txt", b"abc", session=session)

        # Context manager exit marks session ended as well
        with conn.start_session() as ctx_session:
            assert ctx_session.has_ended is False
        assert ctx_session.has_ended is True


def test_session_exit_does_not_mask_caller_exception():
    """ClientSession.__exit__ must not mask exceptions if abort fails."""
    with neosqlite.Connection(":memory:") as conn:
        session = conn.start_session()
        with patch.object(
            session,
            "abort_transaction",
            side_effect=OSError("abort blew up"),
        ):
            with pytest.raises(RuntimeError, match="original caller error"):
                with session:
                    session.start_transaction()
                    raise RuntimeError("original caller error")

        assert session.has_ended is True
        assert session.in_transaction is False
        assert conn._active_tx_session is None


def test_transaction_context_exit_does_not_mask_caller_exception():
    """_TransactionContext.__exit__ must not mask exceptions if abort fails."""
    with neosqlite.Connection(":memory:") as conn:
        with conn.start_session() as session:
            with patch.object(
                session,
                "abort_transaction",
                side_effect=OSError("abort failed"),
            ):
                with pytest.raises(RuntimeError, match="tx block error"):
                    with session.start_transaction():
                        raise RuntimeError("tx block error")

        assert session.has_ended is True
        assert session.in_transaction is False
        assert conn._active_tx_session is None


def test_abort_and_commit_reset_state_on_sqlite_error():
    """commit/abort must reset session and connection state even if SQL fails."""
    with neosqlite.Connection(":memory:") as conn:
        # Simulate savepoint gone on abort
        with conn.transaction():
            with conn.start_session() as s1:
                s1.start_transaction()
                s1._savepoint_name = "nonexistent_sp"
                with pytest.raises(Exception):
                    s1.abort_transaction()
                assert s1.in_transaction is False
                assert conn._active_tx_session is None

            # Simulate savepoint gone on commit
            with conn.start_session() as s2:
                s2.start_transaction()
                s2._savepoint_name = "nonexistent_sp"
                with pytest.raises(Exception):
                    s2.commit_transaction()
                assert s2.in_transaction is False
                assert conn._active_tx_session is None


def test_crud_methods_with_session_param():
    """Verify session parameter is accepted by all major CRUD methods."""
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.test
        coll.insert_one({"name": "initial", "value": 0})

        with conn.start_session() as session:
            # insert_many
            coll.insert_many([{"a": 1}, {"a": 2}], session=session)

            # update_one / update_many
            coll.update_one({"a": 1}, {"$set": {"b": 1}}, session=session)
            coll.update_many(
                {"a": {"$gt": 0}}, {"$set": {"c": 1}}, session=session
            )

            # replace_one
            coll.replace_one(
                {"a": 2}, {"a": 2, "replaced": True}, session=session
            )

            # delete_one / delete_many
            coll.delete_one({"a": 1}, session=session)
            coll.delete_many({"c": 1}, session=session)

            # find / find_one
            coll.find_one({"name": "initial"}, session=session)
            list(coll.find({}, session=session))

            # find_one_and_...
            coll.find_one_and_update(
                {"name": "initial"}, {"$inc": {"value": 1}}, session=session
            )
            coll.find_one_and_replace(
                {"name": "initial"},
                {"name": "initial", "value": 10},
                session=session,
            )
            coll.find_one_and_delete({"name": "initial"}, session=session)

            # distinct / count / estimated
            coll.distinct("name", session=session)
            coll.count_documents({}, session=session)
            coll.estimated_document_count(session=session)

            # bulk_write
            coll.bulk_write(
                [
                    InsertOne({"bulk": 1}),
                    UpdateOne({"bulk": 1}, {"$set": {"updated": True}}),
                    DeleteOne({"bulk": 1}),
                ],
                session=session,
            )
