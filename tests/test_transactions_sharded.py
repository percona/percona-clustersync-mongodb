# pylint: disable=missing-docstring,redefined-outer-name
import threading

from plm import Runner
from testing import Testing


def test_shard_collection_in_tx(t: Testing):
    with t.run(phase=Runner.Phase.APPLY):
        with t.source.start_session() as sess:
            sess.start_transaction()
            t.source["db_1"].create_collection("coll_1")
            t.source.admin.command("shardCollection", "db_1.coll_1", key={"_id": 1})
            t.source["db_1"]["coll_1"].insert_one({"i": 1}, session=sess)
            sess.commit_transaction()

        t.source["db_1"]["coll_1"].insert_one({"i": 2})

    assert t.source["db_1"]["coll_1"].count_documents({}) == 2

    t.compare_all_sharded()


def test_simple(t: Testing):
    t.source["db_1"].create_collection("coll_1")
    t.source.admin.command("shardCollection", "db_1.coll_1", key={"_id": 1})
    t.source["db_2"].create_collection("coll_2")
    t.source.admin.command("shardCollection", "db_2.coll_2", key={"_id": 1})

    with t.run(phase=Runner.Phase.APPLY):
        with t.source.start_session() as sess:
            sess.start_transaction()
            t.source["db_1"]["coll_1"].insert_one({"i": 1}, session=sess)
            t.source["db_2"]["coll_2"].insert_one({"i": 2}, session=sess)
            sess.commit_transaction()

        t.source["db_1"]["coll_1"].insert_one({"i": 2})

    assert t.source["db_1"]["coll_1"].count_documents({}) == 2
    assert t.source["db_2"]["coll_2"].count_documents({}) == 1

    t.compare_all_sharded()


def test_simple_mix(t: Testing):
    t.source["db_1"].create_collection("coll_1")
    t.source.admin.command("shardCollection", "db_1.coll_1", key={"_id": 1})

    t.source["db_2"].create_collection("coll_2")

    with t.run(phase=Runner.Phase.APPLY):
        with t.source.start_session() as sess:
            sess.start_transaction()
            t.source["db_1"]["coll_1"].insert_one({"i": 1}, session=sess)
            t.source["db_2"]["coll_2"].insert_one({"i": 2}, session=sess)
            sess.commit_transaction()

        t.source["db_1"]["coll_1"].insert_one({"i": 2})

    assert t.source["db_1"]["coll_1"].count_documents({}) == 2
    assert t.source["db_2"]["coll_2"].count_documents({}) == 1

    t.compare_all_sharded()


def test_simple_aborted(t: Testing):
    t.source["db_1"].create_collection("coll_1")
    t.source.admin.command("shardCollection", "db_1.coll_1", key={"_id": 1})
    t.source["db_2"].create_collection("coll_2")
    t.source.admin.command("shardCollection", "db_2.coll_2", key={"_id": 1})

    with t.run(phase=Runner.Phase.APPLY):
        with t.source.start_session() as sess:
            sess.start_transaction()
            t.source["db_1"]["coll_1"].insert_one({"i": 1}, session=sess)
            t.source["db_2"]["coll_2"].insert_one({"i": 2}, session=sess)
            sess.abort_transaction()

        t.source["db_1"]["coll_1"].insert_one({"i": 2})

    assert t.source["db_1"]["coll_1"].count_documents({}) == 1

    t.compare_all()


def test_simple_aborted_mix(t: Testing):
    t.source["db_1"].create_collection("coll_1")
    t.source.admin.command("shardCollection", "db_1.coll_1", key={"_id": 1})
    t.source["db_2"].create_collection("coll_2")

    with t.run(phase=Runner.Phase.APPLY):
        with t.source.start_session() as sess:
            sess.start_transaction()
            t.source["db_1"]["coll_1"].insert_one({"i": 1}, session=sess)
            t.source["db_2"]["coll_2"].insert_one({"i": 2}, session=sess)
            sess.abort_transaction()

        t.source["db_1"]["coll_1"].insert_one({"i": 2})

    assert t.source["db_1"]["coll_1"].count_documents({}) == 1

    t.compare_all()


def test_in_progress(t: Testing):
    t.source["db_1"].create_collection("coll_1")
    t.source.admin.command("shardCollection", "db_1.coll_1", key={"_id": 1})

    with t.source.start_session() as sess:
        sess.start_transaction()
        t.source["db_1"]["coll_1"].insert_one({"i": 1, "trx": 1}, session=sess)

        with t.run(Runner.Phase.APPLY):
            t.source["db_1"]["coll_1"].insert_one({"i": 2, "trx": 1}, session=sess)
            sess.commit_transaction()

            t.source["db_1"]["coll_1"].insert_one({"i": 3})

    assert t.source["db_1"]["coll_1"].count_documents({}) == 3

    t.compare_all()


def test_in_progress_aborted(t: Testing):
    t.source["db_1"].create_collection("coll_1")
    t.source.admin.command("shardCollection", "db_1.coll_1", key={"_id": 1})

    with t.source.start_session() as sess:
        sess.start_transaction()
        t.source["db_1"]["coll_1"].insert_one({"i": 1, "trx": 1}, session=sess)

        with t.run(Runner.Phase.APPLY):
            t.source["db_1"]["coll_1"].insert_one({"i": 2, "trx": 1}, session=sess)
            sess.abort_transaction()

    assert t.source["db_1"]["coll_1"].count_documents({}) == 0

    t.compare_all()
