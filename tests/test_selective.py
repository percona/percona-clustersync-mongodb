# pylint: disable=missing-docstring,redefined-outer-name
import pytest
import testing
from plm import Runner
from pymongo import MongoClient


def perform_with_options(source, plm, phase: Runner.Phase, include_ns=None, exclude_ns=None):
    """Perform the PLM operation with the given options."""
    plm_options = {}
    if include_ns:
        plm_options["include_namespaces"] = include_ns
    if exclude_ns:
        plm_options["exclude_namespaces"] = exclude_ns

    return Runner(source, plm, phase, plm_options)


def check_if_target_is_subset(source: MongoClient, target: MongoClient):
    """Check if the target MongoDB is a subset of the source MongoDB."""
    source_dbs = set(testing.list_databases(source))
    target_dbs = set(testing.list_databases(target))
    assert set(target_dbs).issubset(source_dbs)

    for db in target_dbs:
        source_colls = set(testing.list_collections(source, db))
        target_colls = set(testing.list_collections(target, db))
        assert set(target_colls).issubset(source_colls)

        for coll in target_colls:
            testing.compare_namespace(source, target, db, coll)


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_collection_with_include_only(t: testing.Testing, phase: Runner.Phase):
    with perform_with_options(
        t.source,
        t.plm,
        phase,
        include_ns=["db_0.*", "db_1.coll_0", "db_1.coll_1", "db_2.coll_0", "db_2.coll_1"],
    ):
        for db in range(3):
            for coll in range(3):
                t.source["db_1"]["coll_1"].create_index({"i": 1})
                t.source[f"db_{db}"][f"coll_{coll}"].insert_one({})

    expected = {
        "db_0.coll_0",
        "db_0.coll_1",
        "db_0.coll_2",
        "db_1.coll_0",
        "db_1.coll_1",
        # "db_1.coll_2",
        "db_2.coll_0",
        "db_2.coll_1",
        # "db_2.coll_2",
    }

    assert expected == set(testing.list_all_namespaces(t.target))
    check_if_target_is_subset(t.source, t.target)


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_collection_with_exclude_only(t: testing.Testing, phase: Runner.Phase):
    with perform_with_options(
        t.source, t.plm, phase, exclude_ns=["db_0.*", "db_1.coll_0", "db_1.coll_1"]
    ):
        for db in range(3):
            for coll in range(3):
                t.source["db_1"]["coll_1"].create_index({"i": 1})
                t.source[f"db_{db}"][f"coll_{coll}"].insert_one({})

    expected = {
        # "db_0.coll_0",
        # "db_0.coll_1",
        # "db_0.coll_2",
        # "db_1.coll_0",
        # "db_1.coll_1",
        "db_1.coll_2",
        "db_2.coll_0",
        "db_2.coll_1",
        "db_2.coll_2",
    }

    assert expected == set(testing.list_all_namespaces(t.target))
    check_if_target_is_subset(t.source, t.target)


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_collection(t: testing.Testing, phase: Runner.Phase):
    with perform_with_options(
        t.source,
        t.plm,
        phase,
        include_ns=["db_0.*", "db_1.coll_0", "db_1.coll_1", "db_2.coll_0", "db_2.coll_1"],
        exclude_ns=["db_0.*", "db_1.coll_0", "db_3.coll_1"],
    ):
        for db in range(4):
            for coll in range(3):
                t.source["db_1"]["coll_1"].create_index({"i": 1})
                t.source[f"db_{db}"][f"coll_{coll}"].insert_one({})

    expected = {
        # "db_0.coll_0",
        # "db_0.coll_1",
        # "db_0.coll_2",

        # "db_1.coll_0",
        "db_1.coll_1",
        # "db_1.coll_2",

        "db_2.coll_0",
        "db_2.coll_1",
        # "db_2.coll_2",

        "db_3.coll_0",
        # "db_3.coll_1",
        "db_3.coll_2",
    }

    assert expected == set(testing.list_all_namespaces(t.target))
    check_if_target_is_subset(t.source, t.target)
