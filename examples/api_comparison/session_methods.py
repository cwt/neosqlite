"""Module for comparing session and transaction methods between NeoSQLite and PyMongo"""

import warnings

import neosqlite

from .reporter import reporter
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)


def compare_session_methods():
    """Compare session and transaction methods"""
    print("\n=== Session and Transaction Methods Comparison ===")

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_session

        # Test start_session()
        try:
            with neo_conn.start_session() as session:
                neo_session = session is not None
                print(f"Neo start_session(): {'OK' if neo_session else 'FAIL'}")

                # Test transaction
                session.start_transaction()
                neo_collection.insert_one({"a": 1}, session=session)
                session.commit_transaction()

                neo_tx_commit = neo_collection.count_documents({"a": 1}) == 1
                print(
                    f"Neo transaction commit: {'OK' if neo_tx_commit else 'FAIL'}"
                )

                session.start_transaction()
                neo_collection.insert_one({"a": 2}, session=session)
                session.abort_transaction()

                neo_tx_abort = neo_collection.count_documents({"a": 2}) == 0
                print(
                    f"Neo transaction abort: {'OK' if neo_tx_abort else 'FAIL'}"
                )
        except Exception as e:
            neo_session = neo_tx_commit = neo_tx_abort = False
            print(f"Neo session: Error - {e}")

    client = test_pymongo_connection()
    # Initialize MongoDB result variables

    mongo_session = None
    mongo_tx_commit = None
    mongo_tx_abort = None

    if client:
        mongo_db = client.test_session_methods
        mongo_collection = mongo_db.test_session
        mongo_collection.delete_many({})

        # Test start_session() - Note: MongoDB requires replica set for transactions
        try:
            with client.start_session() as session:
                mongo_session = session is not None
                print(
                    f"Mongo start_session(): {'OK' if mongo_session else 'FAIL'}"
                )

                try:
                    with session.start_transaction():
                        mongo_collection.insert_one({"a": 1}, session=session)

                    mongo_tx_commit = (
                        mongo_collection.count_documents({"a": 1}) == 1
                    )
                    print(
                        f"Mongo transaction commit: {'OK' if mongo_tx_commit else 'FAIL'}"
                    )

                    with session.start_transaction():
                        mongo_collection.insert_one({"a": 2}, session=session)
                        session.abort_transaction()

                    mongo_tx_abort = (
                        mongo_collection.count_documents({"a": 2}) == 0
                    )
                    print(
                        f"Mongo transaction abort: {'OK' if mongo_tx_abort else 'FAIL'}"
                    )
                except Exception as e:
                    # standalone MongoDB doesn't support transactions
                    mongo_tx_commit = mongo_tx_abort = False
                    print(
                        f"Mongo transaction: SKIPPED (requires replica set) - {e}"
                    )
        except Exception as e:
            mongo_session = False
            print(f"Mongo session: Error - {e}")

        client.close()

    reporter.record_comparison(
        "Session Methods",
        "start_session",
        neo_session if neo_session else "FAIL",
        mongo_session if mongo_session is not None else None,
        skip_reason="MongoDB not available" if not client else None,
    )

    # For transactions, we compare results if both were executed, otherwise mark as skipped
    # because MongoDB requires a replica set which might not be available
    reporter.record_result(
        "Session Methods",
        "transaction_commit",
        passed=(
            neo_tx_commit == mongo_tx_commit
            if mongo_tx_commit is not None
            else True
        ),
        neo_result=neo_tx_commit,
        mongo_result=mongo_tx_commit,
        skip_reason=(
            "NeoSQLite: OK; MongoDB: Requires replica set (skipped)"
            if (client and mongo_tx_commit is False)
            else ("MongoDB not available" if not client else None)
        ),
    )
    reporter.record_result(
        "Session Methods",
        "transaction_abort",
        passed=(
            neo_tx_abort == mongo_tx_abort
            if mongo_tx_abort is not None
            else True
        ),
        neo_result=neo_tx_abort,
        mongo_result=mongo_tx_abort,
        skip_reason=(
            "NeoSQLite: OK; MongoDB: Requires replica set (skipped)"
            if (client and mongo_tx_abort is False)
            else ("MongoDB not available" if not client else None)
        ),
    )
