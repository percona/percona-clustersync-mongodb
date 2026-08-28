import importlib.util
import os
import selectors
import signal
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_script(name: str):
    path = PROJECT_ROOT / "hack" / name
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(path.parent))
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.pop(0)
    return module


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


def wait_for_output(process: subprocess.Popen, marker: bytes, timeout: float = 5) -> None:
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    deadline = time.monotonic() + timeout
    output = b""
    while marker not in output:
        events = selector.select(max(0, deadline - time.monotonic()))
        if not events:
            raise TimeoutError(f"process did not print {marker!r}")
        chunk = os.read(process.stdout.fileno(), 4096)
        if not chunk:
            raise RuntimeError(f"process exited before printing {marker!r}: {output!r}")
        output += chunk


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


def test_compare_namespace_skips_indexes_when_only_target_is_view():
    compare_all = load_script("compare-all.py")

    class FakeCollection:
        def __init__(self, options):
            self._options = options

        def options(self):
            return self._options

        def index_information(self):
            raise AssertionError("view indexes must not be read")

        def find_raw_batches(self, **kwargs):
            return ()

    class FakeClient:
        def __init__(self, options):
            self.database = FakeDatabase(options)

        def __getitem__(self, name):
            return self.database

    class FakeDatabase:
        def __init__(self, options):
            self.collection = FakeCollection(options)

        def __getitem__(self, name):
            return self.collection

    mismatches = compare_all.compare_namespace(
        FakeClient({}),
        FakeClient({"viewOn": "items"}),
        "db",
        "items",
    )

    assert len(mismatches) == 1


def test_demo_writer_pause_uses_last_acknowledged_write_time(monkeypatch):
    demo_writer = load_script("demo_writer.py")
    args = SimpleNamespace(
        source_label="SOURCE",
        target_label="TARGET",
        database="db",
        collection="items",
        rate=2.0,
        seed=42,
        doc_size=demo_writer.BASE_DOC_SIZE,
    )
    writer = demo_writer.DemoWriter(object(), object(), args)
    clock = {"now": 10.0, "sleeps": 0}

    monkeypatch.setattr(writer, "_do_op", lambda: True)
    monkeypatch.setattr(demo_writer.time, "monotonic", lambda: clock["now"])

    def advance_to_pause(interval):
        clock["sleeps"] += 1
        if clock["sleeps"] == 1:
            clock["now"] = 11.0
            writer._pause = True
        else:
            writer._stop = True

    monkeypatch.setattr(demo_writer.time, "sleep", advance_to_pause)

    writer.run()

    assert writer.t0 == 10.0


@pytest.mark.parametrize("rate", ["0", "-1"])
def test_demo_writer_rejects_nonpositive_rate(rate: str):
    result = run_script("demo_writer.py", "--rate", rate)

    assert result.returncode == 2


def test_demo_writer_rejects_document_smaller_than_base():
    result = run_script("demo_writer.py", "--doc-size", "519")

    assert result.returncode == 2


def test_demo_writer_sigint_exits_interactive_process(tmp_path: Path):
    (tmp_path / "sitecustomize.py").write_text(
        "import pymongo\n"
        "class Admin:\n"
        "    def command(self, name):\n"
        "        return {'ok': 1}\n"
        "class Collection:\n"
        "    def insert_one(self, doc):\n"
        "        return None\n"
        "class Database:\n"
        "    def __getitem__(self, name):\n"
        "        return Collection()\n"
        "class FakeClient:\n"
        "    admin = Admin()\n"
        "    def __init__(self, uri):\n"
        "        pass\n"
        "    def __getitem__(self, name):\n"
        "        return Database()\n"
        "    def close(self):\n"
        "        pass\n"
        "pymongo.MongoClient = FakeClient\n"
    )
    command_env = os.environ.copy()
    command_env.update(
        {
            "PATH": os.pathsep.join((str(Path(sys.executable).parent), command_env["PATH"])),
            "PYTHONPATH": str(tmp_path),
            "PYTHONUNBUFFERED": "1",
            "SOURCE_URI": "mongodb://source.invalid",
            "TARGET_URI": "mongodb://target.invalid",
        }
    )
    process = subprocess.Popen(
        [PROJECT_ROOT / "hack" / "demo_writer.py"],
        cwd=PROJECT_ROOT,
        env=command_env,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    try:
        wait_for_output(process, b"Commands: pause | repoint | resume | status | quit")
        process.stdin.write(b"status\n")
        process.stdin.flush()
        wait_for_output(process, b"WRITING  side=SOURCE")
        process.send_signal(signal.SIGINT)
        try:
            returncode = process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            returncode = None
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()

    assert returncode == 0


def test_loader_prints_redacted_configuration_before_drop(monkeypatch, capsys):
    loader = load_script("loader.py")
    uri = "mongodb://user:secret@localhost:27017"
    observed = {}

    class StopAfterDrop(Exception):
        pass

    class FakeAdmin:
        def command(self, name):
            return {"ok": 1}

    class FakeClient:
        admin = FakeAdmin()

    monkeypatch.setattr(
        loader,
        "parse_args",
        lambda: SimpleNamespace(
            uri=None,
            size=1,
            databases=1,
            collections_per_db=1,
            drop=True,
            sharded=False,
            workers=1,
            batch_size=1,
        ),
    )
    monkeypatch.setattr(loader, "resolve_uri", lambda value: uri)
    monkeypatch.setattr(loader.pymongo, "MongoClient", lambda *args, **kwargs: FakeClient())
    monkeypatch.setattr(
        loader,
        "calculate_parameters",
        lambda *args: {
            "num_databases": 1,
            "collections_per_db": 1,
            "total_collections": 1,
            "total_docs": 1,
            "work_items": [],
        },
    )

    def stop_after_drop(*args):
        observed["output"] = capsys.readouterr().out
        raise StopAfterDrop

    monkeypatch.setattr(loader, "drop_collections", stop_after_drop)

    with pytest.raises(StopAfterDrop):
        loader.main()

    assert "mongodb://***@localhost:27017" in observed["output"]


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
