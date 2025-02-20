# pylint: disable=missing-docstring,redefined-outer-name
import os

import pytest
from pymongo import MongoClient

from mlink import MLink


def pytest_addoption(parser):
    """Add custom command-line options to pytest."""
    parser.addoption("--source-uri", help="MongoDB URI for source")
    parser.addoption("--target-uri", help="MongoDB URI for target")
    parser.addoption("--mlink-url", help="mongolink url")


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


@pytest.fixture(scope="class")
def cls_source(request: pytest.FixtureRequest, source_conn: MongoClient):
    """Provide a class-scoped MongoClient connection to the source MongoDB."""
    request.cls.source = source_conn


@pytest.fixture(scope="class")
def cls_target(request: pytest.FixtureRequest, target_conn: MongoClient):
    """Provide a class-scoped MongoClient connection to the target MongoDB."""
    request.cls.target = target_conn


@pytest.fixture(scope="class")
def cls__mlink(request: pytest.FixtureRequest):
    """Provide a class-scoped MLink instance."""
    url = request.config.getoption("--mlink-url") or os.environ["TEST_MLINK_URL"]
    request.cls._mlink = MLink(url)  # pylint: disable=protected-access
