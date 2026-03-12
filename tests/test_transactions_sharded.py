# pylint: disable=missing-docstring,redefined-outer-name

from testing import Testing

from pcsm import Runner


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


def test_cross_shard(t: Testing):
    t.source["db_1"].create_collection("coll_1")
    t.source.admin.command("shardCollection", "db_1.coll_1", key={"_id": "hashed"})

    num_docs = 20
    expected = num_docs + 1

    with t.run(phase=Runner.Phase.APPLY):
        with t.source.start_session() as sess:
            sess.start_transaction()
            t.source["db_1"]["coll_1"].insert_many(
                [{"i": i} for i in range(num_docs)], session=sess
            )
            sess.commit_transaction()

        t.source["db_1"]["coll_1"].insert_one({"i": num_docs})

        # Poll target until all documents are replicated. On sharded clusters with
        # multi-shard transactions, PCSM's optime tracking (used by wait_for_current_optime)
        # can advance past events that haven't been read from all shards yet. Polling the
        # actual target count avoids that race.
        t.wait_target_count("db_1", "coll_1", expected)

    assert t.source["db_1"]["coll_1"].count_documents({}) == expected

    t.compare_all_sharded()
