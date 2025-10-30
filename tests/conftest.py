# pylint: disable=missing-docstring,redefined-outer-name
import os
import subprocess
import time

import pytest
import testing
from pcsm import PCSM
from pymongo import MongoClient


def pytest_addoption(parser):
    """Add custom command-line options to pytest."""
    parser.addoption("--source-uri", help="MongoDB URI for source")
    parser.addoption("--target-uri", help="MongoDB URI for target")
    parser.addoption("--pcsm_url", help="PCSM url")
    parser.addoption("--pcsm-bin", help="Path to the PCSM binary")
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
def pcsm(request: pytest.FixtureRequest):
    """Provide a pcsm instance."""
    url = request.config.getoption("--pcsm_url") or os.environ["TEST_PCSM_URL"]
    return PCSM(url)


@pytest.fixture(scope="session")
def pcsm_bin(request: pytest.FixtureRequest):
    """Provide the path to the PCSM binary."""
    return request.config.getoption("--pcsm-bin") or os.getenv("TEST_PCSM_BIN")


@pytest.fixture(scope="session")
def t(source_conn: MongoClient, target_conn: MongoClient, pcsm: PCSM):
    return testing.Testing(source_conn, target_conn, pcsm)


@pytest.fixture(autouse=True)
def drop_all_database(source_conn: MongoClient, target_conn: MongoClient):
    """Drop all databases in the source and target MongoDB before each test."""
    testing.drop_all_database(source_conn)
    testing.drop_all_database(target_conn)


PCSM_PROC: subprocess.Popen = None


def start_pcsm(pcsm_bin: str, request: pytest.FixtureRequest):
    source = source_uri(request)
    target = target_uri(request)
    rv = subprocess.Popen([pcsm_bin,"--source", source ,"--target", target, "--reset-state", "--log-level=trace"])
    time.sleep(1)
    return rv


def stop_pcsm(proc: subprocess.Popen):
    proc.terminate()
    return proc.wait()


@pytest.fixture(scope="session", autouse=True)
def manage_pcsm_process(request: pytest.FixtureRequest, pcsm_bin: str):
    """Start pcsm before tests and terminate it after all tests."""
    if not pcsm_bin:
        yield
        return

    global PCSM_PROC  # pylint: disable=W0603
    PCSM_PROC = start_pcsm(pcsm_bin, request)

    def teardown():
        if PCSM_PROC and PCSM_PROC.poll() is None:
            stop_pcsm(PCSM_PROC)

    request.addfinalizer(teardown)
    yield PCSM_PROC


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):  # pylint: disable=W0613
    """Attach test results to each test item for later inspection."""
    outcome = yield
    rep = outcome.get_result()
    setattr(item, "rep_" + rep.when, rep)


@pytest.fixture(autouse=True)
def restart_pcsm_on_failure(request: pytest.FixtureRequest, pcsm_bin: str):
    yield

    if hasattr(request.node, "rep_call") and request.node.rep_call.failed:
        # the test failed. restart pcsm process with a new state
        global PCSM_PROC  # pylint: disable=W0603
        if PCSM_PROC and pcsm_bin:
            stop_pcsm(PCSM_PROC)
            PCSM_PROC = start_pcsm(pcsm_bin, request)
