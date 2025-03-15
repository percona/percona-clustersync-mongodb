# pylint: disable=missing-docstring,redefined-outer-name
import os

import pytest
import testing
from mlink import MongoLink
from pymongo import MongoClient


def pytest_addoption(parser):
    """Add custom command-line options to pytest."""
    parser.addoption("--source-uri", help="MongoDB URI for source")
    parser.addoption("--target-uri", help="MongoDB URI for target")
    parser.addoption("--mongolink-url", help="MongoLink url")


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


@pytest.fixture
def mlink(request: pytest.FixtureRequest):
    """Provide a class-scoped mongolink instance."""
    url = request.config.getoption("--mongolink-url") or os.environ["TEST_MONGOLINK_URL"]
    return MongoLink(url)


@pytest.fixture
def t(source_conn: MongoClient, target_conn: MongoClient, mlink: MongoLink):
    return testing.Testing(source_conn, target_conn, mlink)


@pytest.fixture(autouse=True)
def drop_all_database(source_conn: MongoClient, target_conn: MongoClient):
    """Drop all databases in the source and target MongoDB."""
    for db in testing.list_databases(source_conn):
        source_conn.drop_database(db)
    for db in testing.list_databases(target_conn):
        target_conn.drop_database(db)
