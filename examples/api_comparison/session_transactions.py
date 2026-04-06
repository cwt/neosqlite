"""Module for comparing session and transaction methods between NeoSQLite and PyMongo"""

import os
import warnings

import neosqlite

from .reporter import reporter
from .timing import (
    end_mongo_timing,
    end_neo_timing,
    set_accumulation_mode,
    start_mongo_timing,
    start_neo_timing,
)
from .utils import test_pymongo_connection

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)

# Check if we're running against NX-27017 (NeoSQLite backend)
# In this case, we can run transaction tests because NeoSQLite supports them
IS_NX27017_BACKEND = os.environ.get("NX27017_BACKEND", "").lower() == "true"


def compare_session_methods():
    """Compare session and transaction methods"""
    print("\n=== Session and Transaction Methods Comparison ===")

    # Import benchmark_reporter here to get the current instance (not at module load time)
    from .reporter import benchmark_reporter

    # Track whether MongoDB actually ran tests or skipped
    mongo_tx_executed = False

    # Initialize NeoSQLite result variables
    neo_session = False
    neo_tx_commit = False
    neo_tx_abort = False
    neo_with_tx = False

    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_session
        set_accumulation_mode(True)

        # 1. Test start_session()
        start_neo_timing()
        try:
            session = neo_conn.start_session()
            neo_session = session is not None
        except Exception as e:
            print(f"Neo start_session(): Error - {e}")
            neo_session = False
        finally:
            end_neo_timing()

        if neo_session:
            try:
                with session:
                    print("Neo start_session(): OK")

                    # 2. Test transaction commit
                    start_neo_timing()
                    try:
                        session.start_transaction()
                        neo_collection.insert_one({"a": 1}, session=session)
                        session.commit_transaction()
                        neo_tx_commit = (
                            neo_collection.count_documents({"a": 1}) == 1
                        )
                        print(
                            f"Neo transaction commit: {'OK' if neo_tx_commit else 'FAIL'}"
                        )
                    except Exception as e:
                        print(f"Neo transaction commit: Error - {e}")
                        neo_tx_commit = False
                    finally:
                        end_neo_timing()

                    # 3. Test transaction abort
                    start_neo_timing()
                    try:
                        session.start_transaction()
                        neo_collection.insert_one({"a": 2}, session=session)
                        session.abort_transaction()
                        neo_tx_abort = (
                            neo_collection.count_documents({"a": 2}) == 0
                        )
                        print(
                            f"Neo transaction abort: {'OK' if neo_tx_abort else 'FAIL'}"
                        )
                    except Exception as e:
                        print(f"Neo transaction abort: Error - {e}")
                        neo_tx_abort = False
                    finally:
                        end_neo_timing()

                    # 4. Test with_transaction
                    start_neo_timing()
                    try:

                        def callback(s):
                            neo_collection.insert_one({"a": 3}, session=s)

                        session.with_transaction(callback)
                        neo_with_tx = (
                            neo_collection.count_documents({"a": 3}) == 1
                        )
                        print(
                            f"Neo with_transaction: {'OK' if neo_with_tx else 'FAIL'}"
                        )
                    except Exception as e:
                        print(f"Neo with_transaction: Error - {e}")
                        neo_with_tx = False
                    finally:
                        end_neo_timing()
            except Exception as e:
                print(f"Neo session context: Error - {e}")

    # MongoDB part
    client = test_pymongo_connection()
    mongo_session = None
    mongo_tx_commit = None
    mongo_tx_abort = None
    mongo_with_tx = None

    if client:
        try:
            mongo_db = client.test_session_methods
            mongo_collection = mongo_db.test_session
            mongo_collection.delete_many({})
            set_accumulation_mode(True)

            # 1. Test start_session()
            start_mongo_timing()
            try:
                session = client.start_session()
                mongo_session = session is not None
            except Exception as e:
                print(f"Mongo start_session(): Error - {e}")
                mongo_session = False
            finally:
                end_mongo_timing()

            if mongo_session:
                try:
                    with session:
                        print("Mongo start_session(): OK")

                        try:
                            # 2. Test transaction commit
                            start_mongo_timing()
                            try:
                                session.start_transaction()
                                mongo_collection.insert_one(
                                    {"a": 1}, session=session
                                )
                                session.commit_transaction()
                                mongo_tx_commit = (
                                    mongo_collection.count_documents({"a": 1})
                                    == 1
                                )
                                print(
                                    f"Mongo transaction commit: {'OK' if mongo_tx_commit else 'FAIL'}"
                                )
                            finally:
                                end_mongo_timing()

                            # 3. Test transaction abort
                            start_mongo_timing()
                            try:
                                session.start_transaction()
                                mongo_collection.insert_one(
                                    {"a": 2}, session=session
                                )
                                session.abort_transaction()
                                mongo_tx_abort = (
                                    mongo_collection.count_documents({"a": 2})
                                    == 0
                                )
                                print(
                                    f"Mongo transaction abort: {'OK' if mongo_tx_abort else 'FAIL'}"
                                )
                            finally:
                                end_mongo_timing()

                            # 4. Test with_transaction
                            start_mongo_timing()
                            try:

                                def mongo_callback(s):
                                    mongo_collection.insert_one(
                                        {"a": 3}, session=s
                                    )

                                session.with_transaction(mongo_callback)
                                mongo_with_tx = (
                                    mongo_collection.count_documents({"a": 3})
                                    == 1
                                )
                                print(
                                    f"Mongo with_transaction: {'OK' if mongo_with_tx else 'FAIL'}"
                                )
                            finally:
                                end_mongo_timing()

                            mongo_tx_executed = True
                        except Exception as e:
                            # standalone MongoDB doesn't support transactions
                            mongo_tx_commit = mongo_tx_abort = mongo_with_tx = (
                                False
                            )
                            print(
                                f"Mongo transaction: SKIPPED (requires replica set) - {e}"
                            )
                except Exception as e:
                    print(f"Mongo session context: Error - {e}")
        finally:
            # If transactions didn't execute, mark this as a partial benchmark
            # because the main functionality being tested is transactions
            # BUT if we're on NX-27017 backend, don't skip - we should run and report failures
            if not mongo_tx_executed and not IS_NX27017_BACKEND:
                if benchmark_reporter:
                    benchmark_reporter.mark_mongo_skipped(
                        "Session & Transactions",
                        "MongoDB requires replica set for transactions (only start_session() ran)",
                    )
            client.close()
    else:
        # MongoDB not available at all
        if benchmark_reporter:
            benchmark_reporter.mark_mongo_skipped(
                "Session & Transactions", "MongoDB not available"
            )

    # Report results
    reporter.record_comparison(
        "Session & Transactions",
        "start_session",
        neo_session if neo_session else "FAIL",
        mongo_session if mongo_session is not None else None,
        skip_reason="MongoDB not available" if not client else None,
    )

    # For transactions, we compare results if both were executed, otherwise mark as skipped
    # because MongoDB requires a replica set which might not be available
    # BUT if we're on NX-27017 backend, don't skip - it's a bug if transactions fail
    reporter.record_result(
        "Session & Transactions",
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
            if (
                client
                and mongo_tx_commit is False
                and not mongo_tx_executed
                and not IS_NX27017_BACKEND
            )
            else ("MongoDB not available" if not client else None)
        ),
    )

    reporter.record_result(
        "Session & Transactions",
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
            if (
                client
                and mongo_tx_abort is False
                and not mongo_tx_executed
                and not IS_NX27017_BACKEND
            )
            else ("MongoDB not available" if not client else None)
        ),
    )

    reporter.record_result(
        "Session & Transactions",
        "with_transaction",
        passed=(
            neo_with_tx == mongo_with_tx if mongo_with_tx is not None else True
        ),
        neo_result=neo_with_tx,
        mongo_result=mongo_with_tx,
        skip_reason=(
            "NeoSQLite: OK; MongoDB: Requires replica set (skipped)"
            if (
                client
                and mongo_with_tx is False
                and not mongo_tx_executed
                and not IS_NX27017_BACKEND
            )
            else ("MongoDB not available" if not client else None)
        ),
    )
