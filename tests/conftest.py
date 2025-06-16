# pylint: disable=missing-docstring,redefined-outer-name
import os
import subprocess
import time

import pytest
import testing
from plm import PLM
from pymongo import MongoClient


def pytest_addoption(parser):
    """Add custom command-line options to pytest."""
    parser.addoption("--source-uri", help="MongoDB URI for source")
    parser.addoption("--target-uri", help="MongoDB URI for target")
    parser.addoption("--plm_url", help="PLM url")
    parser.addoption("--plm-bin", help="Path to the PLM binary")
    parser.addoption("--runslow", action="store_true", default=False, help="run slow tests")


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow")


def pytest_collection_modifyitems(config, items):
    """This allows users to control whether slow tests are included in the test run.

    If the `--runslow` option is not provided, tests marked with the "slow" keyword
    will be skipped with a message indicating the need for the `--runslow` option.
    """
    if not config.getoption("--runslow"):
        skip_slow = pytest.mark.skip(reason="need --runslow option to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)

def source_uri(request: pytest.FixtureRequest):
    """Provide the source MongoDB URI."""
    return request.config.getoption("--source-uri") or os.environ["TEST_SOURCE_URI"]

def target_uri(request: pytest.FixtureRequest):
    """Provide the target MongoDB URI."""
    return request.config.getoption("--target-uri") or os.environ["TEST_TARGET_URI"]

@pytest.fixture(scope="session")
def source_conn(request: pytest.FixtureRequest):
    """Provide a MongoClient connection to the source MongoDB."""
    with MongoClient(source_uri(request)) as conn:
        yield conn


@pytest.fixture(scope="session")
def target_conn(request: pytest.FixtureRequest):
    """Provide a MongoClient connection to the target MongoDB."""
    with MongoClient(target_uri(request)) as conn:
        yield conn


@pytest.fixture(scope="session")
def plm(request: pytest.FixtureRequest):
    """Provide a plm instance."""
    url = request.config.getoption("--plm_url") or os.environ["TEST_PLM_URL"]
    return PLM(url)


@pytest.fixture(scope="session")
def plm_bin(request: pytest.FixtureRequest):
    """Provide the path to the PLM binary."""
    return request.config.getoption("--plm-bin") or os.getenv("TEST_PLM_BIN")


@pytest.fixture(scope="session")
def t(source_conn: MongoClient, target_conn: MongoClient, plm: PLM):
    return testing.Testing(source_conn, target_conn, plm)


@pytest.fixture(autouse=True)
def drop_all_database(source_conn: MongoClient, target_conn: MongoClient):
    """Drop all databases in the source and target MongoDB before each test."""
    testing.drop_all_database(source_conn)
    testing.drop_all_database(target_conn)


PLM_PROC: subprocess.Popen = None


def start_plm(plm_bin: str, request: pytest.FixtureRequest):
    source = source_uri(request)
    target = target_uri(request)
    rv = subprocess.Popen([plm_bin,"--source", source ,"--target", target, "--reset-state", "--log-level=trace"])
    time.sleep(1)
    return rv


def stop_plm(proc: subprocess.Popen):
    proc.terminate()
    return proc.wait()


@pytest.fixture(scope="session", autouse=True)
def manage_plm_process(request: pytest.FixtureRequest, plm_bin: str):
    """Start plm before tests and terminate it after all tests."""
    if not plm_bin:
        yield
        return

    global PLM_PROC  # pylint: disable=W0603
    PLM_PROC = start_plm(plm_bin, request)

    def teardown():
        if PLM_PROC and PLM_PROC.poll() is None:
            stop_plm(PLM_PROC)

    request.addfinalizer(teardown)
    yield PLM_PROC


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):  # pylint: disable=W0613
    """Attach test results to each test item for later inspection."""
    outcome = yield
    rep = outcome.get_result()
    setattr(item, "rep_" + rep.when, rep)


@pytest.fixture(autouse=True)
def restart_plm_on_failure(request: pytest.FixtureRequest, plm_bin: str):
    yield

    if hasattr(request.node, "rep_call") and request.node.rep_call.failed:
        # the test failed. restart plm process with a new state
        global PLM_PROC  # pylint: disable=W0603
        if PLM_PROC and plm_bin:
            stop_plm(PLM_PROC)
            PLM_PROC = start_plm(plm_bin, request)
