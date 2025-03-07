# pylint: disable=missing-docstring,redefined-outer-name
import threading

from _base import BaseTesting
from mlink import Runner


class TestTransaction(BaseTesting):
    def test_simple(self):
        self.drop_all_database()

        with self.perform(phase=Runner.Phase.APPLY):
            with self.source.start_session() as sess:
                sess.start_transaction()
                self.source["db_1"]["coll_1"].insert_one({"i": 1}, session=sess)
                self.source["db_2"]["coll_2"].insert_one({"i": 2}, session=sess)
                sess.commit_transaction()

        assert self.source["db_1"]["coll_1"].count_documents({}) == 1
        assert self.source["db_2"]["coll_2"].count_documents({}) == 1

        self.compare_all()

    def test_simple_aborted(self):
        self.drop_all_database()
        self.create_collection("db_3", "coll_3")

        with self.perform(phase=Runner.Phase.APPLY):
            with self.source.start_session() as sess:
                sess.start_transaction()
                self.source["db_1"]["coll_1"].insert_one({"i": 1}, session=sess)
                self.source["db_2"]["coll_2"].insert_one({"i": 2}, session=sess)
                sess.abort_transaction()

        assert "db_1" not in self.source.list_database_names()
        assert "db_2" not in self.source.list_database_names()

        self.compare_all()

    def test_mixed_with_non_trx_ops(self):
        self.drop_all_database()

        mlink = self.perform(Runner.Phase.APPLY)
        with self.source.start_session() as sess:
            for i in range(2):
                sess.start_transaction()
                self.source["db_1"]["coll_1"].insert_one({"i": 1})
                self.source["db_1"]["coll_1"].insert_one({"i": 2, "trx": i}, session=sess)

                mlink.start()
                self.source["db_1"]["coll_1"].insert_one({"i": 3})

                self.source["db_1"]["coll_1"].insert_one({"i": 4, "trx": i}, session=sess)
                self.source["db_1"]["coll_1"].insert_one({"i": 5})
                sess.commit_transaction()

                mlink.wait_for_current_optime()
                mlink.finalize()

        assert self.source["db_1"]["coll_1"].count_documents({}) == 10

        self.compare_all()

    def test_mixed_with_non_trx_ops_aborted(self):
        self.drop_all_database()

        mlink = self.perform(Runner.Phase.MANUAL)
        with self.source.start_session() as sess:
            for t in range(2):
                sess.start_transaction()
                self.source["db_1"]["coll_1"].insert_one({"i": 1})
                self.source["db_1"]["coll_1"].insert_one({"i": 1, "trx": t}, session=sess)

                mlink.start()
                self.source["db_1"]["coll_1"].insert_one({"i": 2})
                mlink.wait_for_initial_sync()

                self.source["db_1"]["coll_1"].insert_one({"i": 2, "trx": t}, session=sess)
                self.source["db_1"]["coll_1"].insert_one({"i": 3})
                sess.abort_transaction()

            mlink.finalize()

        assert self.source["db_1"]["coll_1"].count_documents({}) == 6

        self.compare_all()

    def test_in_progress(self):
        self.drop_all_database()

        with self.source.start_session() as sess:
            sess.start_transaction()
            self.source["db_1"]["coll_1"].insert_one({"i": 1, "trx": 1}, session=sess)

            with self.perform(Runner.Phase.APPLY):
                self.source["db_1"]["coll_1"].insert_one({"i": 2, "trx": 1}, session=sess)
                sess.commit_transaction()

        assert self.source["db_1"]["coll_1"].count_documents({}) == 2

        self.compare_all()

    def test_in_progress_aborted(self):
        self.drop_all_database()

        with self.source.start_session() as sess:
            sess.start_transaction()
            self.source["db_1"]["coll_1"].insert_one({"i": 1, "trx": 1}, session=sess)

            with self.perform(Runner.Phase.APPLY):
                self.source["db_1"]["coll_1"].insert_one({"i": 2, "trx": 1}, session=sess)
                sess.abort_transaction()

        assert "db_1" not in self.source.list_database_names()

        self.compare_all()

    def test_unfinished(self):
        self.drop_all_database()

        with self.source.start_session() as sess:
            sess.start_transaction()
            self.source["db_1"]["coll_1"].insert_one({"i": 1, "trx": 1}, session=sess)

            with self.perform(Runner.Phase.APPLY):
                self.source["db_1"]["coll_1"].insert_one({"i": 2, "trx": 1}, session=sess)

            sess.commit_transaction()

        assert self.source["db_1"]["coll_1"].count_documents({}) == 2
        assert "db_1" not in self.target.list_database_names()

    def test_unfinished_aborted(self):
        self.drop_all_database()

        with self.source.start_session() as sess:
            sess.start_transaction()
            self.source["db_1"]["coll_1"].insert_one({"i": 1, "trx": 1}, session=sess)

            with self.perform(Runner.Phase.APPLY):
                self.source["db_1"]["coll_1"].insert_one({"i": 2, "trx": 1}, session=sess)

            sess.abort_transaction()

        assert "db_1" not in self.source.list_database_names()

        self.compare_all()

    def test_concurrent_trx(self):
        self.drop_all_database()

        event_1 = threading.Event()
        event_2 = threading.Event()

        def run_transaction_1():
            with self.source.start_session() as sess:
                sess.start_transaction()
                self.source["db_1"]["coll_1"].insert_one({"i": 1, "trx": 1}, session=sess)
                event_1.set()
                event_2.wait()
                self.source["db_1"]["coll_1"].insert_one({"i": 2, "trx": 1}, session=sess)
                sess.commit_transaction()

        def run_transaction_2():
            with self.source.start_session() as sess:
                sess.start_transaction()
                event_1.wait()
                self.source["db_2"]["coll_2"].insert_one({"i": 3, "trx": 2}, session=sess)
                self.source["db_2"]["coll_2"].insert_one({"i": 4, "trx": 2}, session=sess)
                event_2.set()
                sess.commit_transaction()

        with self.perform(Runner.Phase.APPLY):
            thread1 = threading.Thread(target=run_transaction_1)
            thread2 = threading.Thread(target=run_transaction_2)

            thread1.start()
            thread2.start()

            thread1.join()
            thread2.join()

        assert self.source["db_1"]["coll_1"].count_documents({}) == 2
        assert self.source["db_2"]["coll_2"].count_documents({}) == 2

        self.compare_all()

    def test_concurrent_trx_one_aborted(self):
        self.drop_all_database()

        event_1 = threading.Event()
        event_2 = threading.Event()

        def run_transaction_1():
            with self.source.start_session() as sess:
                sess.start_transaction()
                self.source["db_1"]["coll_1"].insert_one({"i": 1, "trx": 1}, session=sess)
                event_1.set()
                event_2.wait()
                self.source["db_1"]["coll_1"].insert_one({"i": 2, "trx": 1}, session=sess)
                sess.commit_transaction()

        def run_transaction_2():
            with self.source.start_session() as sess:
                sess.start_transaction()
                event_1.wait()
                self.source["db_2"]["coll_2"].insert_one({"i": 3, "trx": 2}, session=sess)
                self.source["db_2"]["coll_2"].insert_one({"i": 4, "trx": 2}, session=sess)
                event_2.set()
                sess.abort_transaction()

        with self.perform(Runner.Phase.APPLY):
            thread1 = threading.Thread(target=run_transaction_1)
            thread2 = threading.Thread(target=run_transaction_2)

            thread1.start()
            thread2.start()

            thread1.join()
            thread2.join()

        assert self.source["db_1"]["coll_1"].count_documents({}) == 2
        assert "db_2" not in self.source.list_database_names()

        self.compare_all()

    def test_concurrent_trx_all_aborted(self):
        self.drop_all_database()

        event_1 = threading.Event()
        event_2 = threading.Event()

        def run_transaction_1():
            with self.source.start_session() as sess:
                sess.start_transaction()
                self.source["db_1"]["coll_1"].insert_one({"i": 1, "trx": 1}, session=sess)
                event_1.set()
                event_2.wait()
                self.source["db_1"]["coll_1"].insert_one({"i": 2, "trx": 1}, session=sess)
                sess.abort_transaction()

        def run_transaction_2():
            with self.source.start_session() as sess:
                sess.start_transaction()
                event_1.wait()
                self.source["db_2"]["coll_2"].insert_one({"i": 3, "trx": 2}, session=sess)
                self.source["db_2"]["coll_2"].insert_one({"i": 4, "trx": 2}, session=sess)
                event_2.set()
                sess.abort_transaction()

        with self.perform(Runner.Phase.APPLY):
            thread1 = threading.Thread(target=run_transaction_1)
            thread2 = threading.Thread(target=run_transaction_2)

            thread1.start()
            thread2.start()

            thread1.join()
            thread2.join()

        assert "db_1" not in self.source.list_database_names()
        assert "db_2" not in self.source.list_database_names()

        self.compare_all()
