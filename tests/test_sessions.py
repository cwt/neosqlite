import neosqlite
from neosqlite.client_session import ClientSession
from neosqlite import InsertOne, UpdateOne, DeleteOne


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


def test_session_abort():
    """Test aborting a transaction."""
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.test
        with conn.start_session() as session:
            session.start_transaction()
            coll.insert_one({"name": "to_abort"}, session=session)
            session.abort_transaction()

            assert coll.count_documents({"name": "to_abort"}) == 0


def test_nested_transactions_savepoints():
    """Test nested transactions using SAVEPOINTs via ClientSession."""
    with neosqlite.Connection(":memory:") as conn:
        coll = conn.test
        with conn.start_session() as session1:
            session1.start_transaction()
            coll.insert_one({"name": "outer"}, session=session1)

            with conn.start_session() as session2:
                session2.start_transaction()  # Should use SAVEPOINT
                coll.insert_one({"name": "inner"}, session=session2)
                session2.abort_transaction()  # Rollback inner only

            session1.commit_transaction()

            assert coll.count_documents({"name": "outer"}) == 1
            assert coll.count_documents({"name": "inner"}) == 0


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
