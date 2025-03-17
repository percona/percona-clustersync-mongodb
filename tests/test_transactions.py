# pylint: disable=missing-docstring,redefined-outer-name
import threading

from mlink import Runner
from testing import Testing


def test_simple(t: Testing):
    with t.run(phase=Runner.Phase.APPLY):
        with t.source.start_session() as sess:
            sess.start_transaction()
            t.source["db_1"]["coll_1"].insert_one({"i": 1}, session=sess)
            t.source["db_2"]["coll_2"].insert_one({"i": 2}, session=sess)
            sess.commit_transaction()

    assert t.source["db_1"]["coll_1"].count_documents({}) == 1
    assert t.source["db_2"]["coll_2"].count_documents({}) == 1

    t.compare_all()


def test_simple_aborted(t: Testing):
    with t.run(phase=Runner.Phase.APPLY):
        with t.source.start_session() as sess:
            sess.start_transaction()
            t.source["db_1"]["coll_1"].insert_one({"i": 1}, session=sess)
            t.source["db_2"]["coll_2"].insert_one({"i": 2}, session=sess)
            sess.abort_transaction()

    assert "db_1" not in t.source.list_database_names()
    assert "db_2" not in t.source.list_database_names()

    t.compare_all()


def test_mixed_with_non_trx_ops(t: Testing):
    mlink = t.run(Runner.Phase.APPLY)
    with t.source.start_session() as sess:
        for trx in range(2):
            sess.start_transaction()
            t.source["db_1"]["coll_1"].insert_one({"i": 1})
            t.source["db_1"]["coll_1"].insert_one({"i": 2, "trx": trx}, session=sess)

            mlink.start()
            t.source["db_1"]["coll_1"].insert_one({"i": 3})

            t.source["db_1"]["coll_1"].insert_one({"i": 4, "trx": trx}, session=sess)
            t.source["db_1"]["coll_1"].insert_one({"i": 5})
            sess.commit_transaction()

            mlink.wait_for_current_optime()
            mlink.finalize()

    assert t.source["db_1"]["coll_1"].count_documents({}) == 10

    t.compare_all()


def test_mixed_with_non_trx_ops_aborted(t: Testing):
    mlink = t.run(None)

    with t.source.start_session() as sess:
        for trx in range(2):
            sess.start_transaction()
            t.source["db_1"]["coll_1"].insert_one({"i": 1})
            t.source["db_1"]["coll_1"].insert_one({"i": 1, "trx": trx}, session=sess)

            mlink.start()
            t.source["db_1"]["coll_1"].insert_one({"i": 2})
            mlink.wait_for_initial_sync()

            t.source["db_1"]["coll_1"].insert_one({"i": 2, "trx": trx}, session=sess)
            t.source["db_1"]["coll_1"].insert_one({"i": 3})
            sess.abort_transaction()

        mlink.finalize()

    assert t.source["db_1"]["coll_1"].count_documents({}) == 6

    t.compare_all()


def test_in_progress(t: Testing):
    with t.source.start_session() as sess:
        sess.start_transaction()
        t.source["db_1"]["coll_1"].insert_one({"i": 1, "trx": 1}, session=sess)

        with t.run(Runner.Phase.APPLY):
            t.source["db_1"]["coll_1"].insert_one({"i": 2, "trx": 1}, session=sess)
            sess.commit_transaction()

    assert t.source["db_1"]["coll_1"].count_documents({}) == 2

    t.compare_all()


def test_in_progress_aborted(t: Testing):
    with t.source.start_session() as sess:
        sess.start_transaction()
        t.source["db_1"]["coll_1"].insert_one({"i": 1, "trx": 1}, session=sess)

        with t.run(Runner.Phase.APPLY):
            t.source["db_1"]["coll_1"].insert_one({"i": 2, "trx": 1}, session=sess)
            sess.abort_transaction()

    assert "db_1" not in t.source.list_database_names()

    t.compare_all()


def test_unfinished(t: Testing):
    with t.source.start_session() as sess:
        sess.start_transaction()
        t.source["db_1"]["coll_1"].insert_one({"i": 1, "trx": 1}, session=sess)

        with t.run(Runner.Phase.APPLY):
            t.source["db_1"]["coll_1"].insert_one({"i": 2, "trx": 1}, session=sess)

        sess.commit_transaction()

    assert t.source["db_1"]["coll_1"].count_documents({}) == 2
    assert "db_1" not in t.target.list_database_names()


def test_unfinished_aborted(t: Testing):
    with t.source.start_session() as sess:
        sess.start_transaction()
        t.source["db_1"]["coll_1"].insert_one({"i": 1, "trx": 1}, session=sess)

        with t.run(Runner.Phase.APPLY):
            t.source["db_1"]["coll_1"].insert_one({"i": 2, "trx": 1}, session=sess)

        sess.abort_transaction()

    assert "db_1" not in t.source.list_database_names()

    t.compare_all()


def test_concurrent_trx(t: Testing):
    event_1 = threading.Event()
    event_2 = threading.Event()

    def run_transaction_1():
        with t.source.start_session() as sess:
            sess.start_transaction()
            t.source["db_1"]["coll_1"].insert_one({"i": 1, "trx": 1}, session=sess)
            event_1.set()
            event_2.wait()
            t.source["db_1"]["coll_1"].insert_one({"i": 2, "trx": 1}, session=sess)
            sess.commit_transaction()

    def run_transaction_2():
        with t.source.start_session() as sess:
            sess.start_transaction()
            event_1.wait()
            t.source["db_2"]["coll_2"].insert_one({"i": 3, "trx": 2}, session=sess)
            t.source["db_2"]["coll_2"].insert_one({"i": 4, "trx": 2}, session=sess)
            event_2.set()
            sess.commit_transaction()

    with t.run(Runner.Phase.APPLY):
        thread1 = threading.Thread(target=run_transaction_1)
        thread2 = threading.Thread(target=run_transaction_2)

        thread1.start()
        thread2.start()

        thread1.join()
        thread2.join()

    assert t.source["db_1"]["coll_1"].count_documents({}) == 2
    assert t.source["db_2"]["coll_2"].count_documents({}) == 2

    t.compare_all()


def test_concurrent_trx_one_aborted(t: Testing):
    event_1 = threading.Event()
    event_2 = threading.Event()

    def run_transaction_1():
        with t.source.start_session() as sess:
            sess.start_transaction()
            t.source["db_1"]["coll_1"].insert_one({"i": 1, "trx": 1}, session=sess)
            event_1.set()
            event_2.wait()
            t.source["db_1"]["coll_1"].insert_one({"i": 2, "trx": 1}, session=sess)
            sess.commit_transaction()

    def run_transaction_2():
        with t.source.start_session() as sess:
            sess.start_transaction()
            event_1.wait()
            t.source["db_2"]["coll_2"].insert_one({"i": 3, "trx": 2}, session=sess)
            t.source["db_2"]["coll_2"].insert_one({"i": 4, "trx": 2}, session=sess)
            event_2.set()
            sess.abort_transaction()

    with t.run(Runner.Phase.APPLY):
        thread1 = threading.Thread(target=run_transaction_1)
        thread2 = threading.Thread(target=run_transaction_2)

        thread1.start()
        thread2.start()

        thread1.join()
        thread2.join()

    assert t.source["db_1"]["coll_1"].count_documents({}) == 2
    assert "db_2" not in t.source.list_database_names()

    t.compare_all()


def test_concurrent_trx_all_aborted(t: Testing):
    event_1 = threading.Event()
    event_2 = threading.Event()

    def run_transaction_1():
        with t.source.start_session() as sess:
            sess.start_transaction()
            t.source["db_1"]["coll_1"].insert_one({"i": 1, "trx": 1}, session=sess)
            event_1.set()
            event_2.wait()
            t.source["db_1"]["coll_1"].insert_one({"i": 2, "trx": 1}, session=sess)
            sess.abort_transaction()

    def run_transaction_2():
        with t.source.start_session() as sess:
            sess.start_transaction()
            event_1.wait()
            t.source["db_2"]["coll_2"].insert_one({"i": 3, "trx": 2}, session=sess)
            t.source["db_2"]["coll_2"].insert_one({"i": 4, "trx": 2}, session=sess)
            event_2.set()
            sess.abort_transaction()

    with t.run(Runner.Phase.APPLY):
        thread1 = threading.Thread(target=run_transaction_1)
        thread2 = threading.Thread(target=run_transaction_2)

        thread1.start()
        thread2.start()

        thread1.join()
        thread2.join()

    assert "db_1" not in t.source.list_database_names()
    assert "db_2" not in t.source.list_database_names()

    t.compare_all()
