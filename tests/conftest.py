# pylint: disable=missing-docstring,redefined-outer-name
import os
import subprocess
import threading
import time

import pytest
import testing
from mlink import MongoLink
from pymongo import MongoClient


def pytest_addoption(parser):
    """Add custom command-line options to pytest."""
    parser.addoption("--source-uri", help="MongoDB URI for source")
    parser.addoption("--target-uri", help="MongoDB URI for target")
    parser.addoption("--mongolink-url", help="MongoLink url")
    parser.addoption("--mongolink-bin", help="Path to the MongoLink binary")


@pytest.fixture(scope="session")
def source_conn(request: pytest.FixtureRequest):
    """Provide a MongoClient connection to the source MongoDB."""
    uri = request.config.getoption("--source-uri") or os.environ["TEST_SOURCE_URI"]
    with MongoClient(uri) as conn:
        yield conn


@pytest.fixture(scope="session")
def target_conn(request: pytest.FixtureRequest):
    """Provide a MongoClient connection to the target MongoDB."""
    uri = request.config.getoption("--target-uri") or os.environ["TEST_TARGET_URI"]
    with MongoClient(uri) as conn:
        yield conn


@pytest.fixture(scope="session")
def mlink(request: pytest.FixtureRequest):
    """Provide a mongolink instance."""
    url = request.config.getoption("--mongolink-url") or os.environ["TEST_MONGOLINK_URL"]
    return MongoLink(url)


@pytest.fixture(scope="session")
def mlink_bin(request: pytest.FixtureRequest):
    """Provide the path to the MongoLink binary."""
    return request.config.getoption("--mongolink-bin") or os.getenv("TEST_MONGOLINK_BIN")


@pytest.fixture(scope="session")
def t(source_conn: MongoClient, target_conn: MongoClient, mlink: MongoLink):
    return testing.Testing(source_conn, target_conn, mlink)


@pytest.fixture(autouse=True)
def drop_all_database(source_conn: MongoClient, target_conn: MongoClient):
    """Drop all databases in the source and target MongoDB before each test."""
    for db in testing.list_databases(source_conn):
        source_conn.drop_database(db)
    for db in testing.list_databases(target_conn):
        target_conn.drop_database(db)


MLINK_PROC: subprocess.Popen = None


def start_mongolink(mlink_bin: str):
    return subprocess.Popen([mlink_bin, "--reset-state"])


def stop_mongolink(proc: subprocess.Popen):
    proc.terminate()
    return proc.wait()


@pytest.fixture(scope="session", autouse=True)
def manage_mongolink_process(request: pytest.FixtureRequest, mlink_bin: str):
    """Start mongolink before tests and terminate it after all tests."""
    if not mlink_bin:
        yield
        return

    global MLINK_PROC  # pylint: disable=W0603
    MLINK_PROC = start_mongolink(mlink_bin)

    def teardown():
        if MLINK_PROC and MLINK_PROC.poll() is None:
            stop_mongolink(MLINK_PROC)

    request.addfinalizer(teardown)
    yield MLINK_PROC


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Attach test results to each test item for later inspection."""
    outcome = yield
    rep = outcome.get_result()
    setattr(item, "rep_" + rep.when, rep)


@pytest.fixture(autouse=True)
def restart_mongolink_on_failure(request: pytest.FixtureRequest, mlink_bin: str):
    yield

    if hasattr(request.node, "rep_call") and request.node.rep_call.failed:
        # the test failed. restart mongolink process with a new state
        global MLINK_PROC  # pylint: disable=W0603
        if MLINK_PROC and mlink_bin:
            stop_mongolink(MLINK_PROC)
            MLINK_PROC = start_mongolink(mlink_bin)
