import os
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def drop_all_database():
    yield


def run_script(script: str, *args: str, env: dict[str, str] | None = None):
    command_env = os.environ.copy()
    command_env["PATH"] = os.pathsep.join((str(Path(sys.executable).parent), command_env["PATH"]))
    if env:
        command_env.update(env)

    return subprocess.run(
        [PROJECT_ROOT / "hack" / script, *args],
        cwd=PROJECT_ROOT,
        env=command_env,
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )


def test_compare_all_help_exits_without_connecting():
    result = run_script(
        "compare-all.py",
        "--help",
        env={
            "SRC_URI": "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=100",
            "TGT_URI": "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=100",
        },
    )

    assert result.returncode == 0, result.stderr
    assert result.stderr == ""


def test_oplog_stream_help_exits_without_connecting(tmp_path: Path):
    (tmp_path / "sitecustomize.py").write_text(
        "import pymongo\n"
        "def reject_connection(*args, **kwargs):\n"
        "    raise RuntimeError('unexpected MongoDB connection')\n"
        "pymongo.MongoClient = reject_connection\n"
    )

    result = run_script(
        "oplog_stream.py",
        "--help",
        env={"PYTHONPATH": str(tmp_path)},
    )

    assert result.returncode == 0, result.stderr
    assert result.stderr == ""


def test_oplog_stream_uses_mongo_uri(tmp_path: Path):
    (tmp_path / "sitecustomize.py").write_text(
        "import os\n"
        "import pymongo\n"
        "class EmptyCollection:\n"
        "    def find(self):\n"
        "        return ()\n"
        "class LocalDatabase:\n"
        "    def __getitem__(self, name):\n"
        "        return EmptyCollection()\n"
        "class FakeClient:\n"
        "    def __init__(self, uri, **kwargs):\n"
        "        if uri != os.environ['EXPECTED_MONGO_URI']:\n"
        "            raise RuntimeError('unexpected MongoDB URI')\n"
        "        self.local = LocalDatabase()\n"
        "pymongo.MongoClient = FakeClient\n"
    )
    uri = "mongodb://example.invalid:27017"

    result = run_script(
        "oplog_stream.py",
        env={
            "EXPECTED_MONGO_URI": uri,
            "MONGO_URI": uri,
            "PYTHONPATH": str(tmp_path),
        },
    )

    assert result.returncode == 0, result.stderr
    assert result.stderr == ""
