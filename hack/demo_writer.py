#!/usr/bin/env python3
"""
PCSM Demo Writer

Continuous application-style write load for a live migration demo. Writes a mix of
inserts, updates and deletes into a single namespace, and can be paused, repointed
from the source cluster to the target cluster, and resumed with a single command.

The pause/repoint/resume sequence measures the application write gap during
cutover: the stopwatch starts once the last write to the source is acknowledged
and stops once the first write to the target is acknowledged.

Connection strings are read from the environment and never printed: the output
shows cluster labels only.

Usage:
    export SOURCE_URI="mongodb+srv://..."   # or SRC_URI
    export TARGET_URI="mongodb://..."       # or TGT_URI
    hack/demo_writer.py --source-label atlas-m20 --target-label psmdb-rs

Commands (type and press Enter):
    pause      stop writing, start the stopwatch
    repoint    switch to the target cluster (only while paused)
    resume     start writing again, stop the stopwatch
    status     print the current state
    quit       stop the writer and exit

Options:
    -r, --rate          Operations per second (default: 2)
    --database          Database to write to (default: db_live)
    --collection        Collection to write to (default: collection_0)
    --doc-size          Target document size in bytes (default: 5120)
    --seed              Random seed for document generation (default: 42)
    --source-label      Label shown for the source cluster (default: $SOURCE_LABEL or SOURCE)
    --target-label      Label shown for the target cluster (default: $TARGET_LABEL or TARGET)

Environment variables:
    SOURCE_URI / SRC_URI   MongoDB connection string for the source cluster
    TARGET_URI / TGT_URI   MongoDB connection string for the target cluster
    SOURCE_LABEL           Default label for the source cluster
    TARGET_LABEL           Default label for the target cluster
"""

import argparse
import os
import random
import sys
import threading
import time
from collections import deque
from datetime import UTC, datetime

import pymongo
from generator import BASE_DOC_SIZE, DEFAULT_DOC_SIZE, DEFAULT_SEED, generate_document
from pymongo.errors import PyMongoError

DEFAULT_DATABASE = "db_live"
DEFAULT_COLLECTION = "collection_0"
DEFAULT_RATE = 2.0
ID_BUFFER_SIZE = 500
PAUSE_TICK_INTERVAL = 5
OP_KINDS = ["insert", "update", "delete"]
OP_WEIGHTS = [70, 20, 10]
STATUSES = ["active", "pending", "archived"]
ACK_TIMEOUT = 30


def parse_args():
    parser = argparse.ArgumentParser(
        description="PCSM Demo Writer - pausable, repointable write load",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    hack/demo_writer.py
    hack/demo_writer.py -r 5 --source-label atlas-m20 --target-label psmdb-rs

Environment variables:
    SOURCE_URI / SRC_URI   MongoDB connection string for the source cluster
    TARGET_URI / TGT_URI   MongoDB connection string for the target cluster
        """,
    )
    parser.add_argument(
        "-r",
        "--rate",
        type=float,
        default=DEFAULT_RATE,
        help=f"Operations per second (default: {DEFAULT_RATE})",
    )
    parser.add_argument(
        "--database",
        type=str,
        default=DEFAULT_DATABASE,
        help=f"Database to write to (default: {DEFAULT_DATABASE})",
    )
    parser.add_argument(
        "--collection",
        type=str,
        default=DEFAULT_COLLECTION,
        help=f"Collection to write to (default: {DEFAULT_COLLECTION})",
    )
    parser.add_argument(
        "--doc-size",
        type=int,
        default=DEFAULT_DOC_SIZE,
        help=f"Target document size in bytes (default: {DEFAULT_DOC_SIZE})",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=f"Random seed for document generation (default: {DEFAULT_SEED})",
    )
    parser.add_argument(
        "--source-label",
        type=str,
        default=os.environ.get("SOURCE_LABEL", "SOURCE"),
        help="Label shown for the source cluster (default: $SOURCE_LABEL or SOURCE)",
    )
    parser.add_argument(
        "--target-label",
        type=str,
        default=os.environ.get("TARGET_LABEL", "TARGET"),
        help="Label shown for the target cluster (default: $TARGET_LABEL or TARGET)",
    )
    args = parser.parse_args()
    if args.rate <= 0:
        parser.error("--rate must be greater than 0")
    if args.doc_size < BASE_DOC_SIZE:
        parser.error(f"--doc-size must be at least {BASE_DOC_SIZE} bytes")
    return args


def resolve_cluster_uri(name: str, *env_vars: str) -> str:
    """Return the first connection string set in the given environment variables."""
    for var in env_vars:
        value = os.environ.get(var)
        if value:
            return value

    print(f"ERROR: no {name} URI: set {' or '.join(env_vars)}")
    sys.exit(1)


def timestamp() -> str:
    """Wall clock timestamp with millisecond precision."""
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


class DemoWriter:
    """Single-threaded write loop with pause, repoint and resume controls."""

    def __init__(self, source, target, args):
        self.source = source
        self.target = target
        self.client = source
        self.side = "source"
        self.labels = {"source": args.source_label, "target": args.target_label}

        self.database = args.database
        self.collection = args.collection
        self.namespace = f"{args.database}.{args.collection}"
        self.interval = 1.0 / args.rate
        self.seed = args.seed
        self.padding = max(0, args.doc_size - BASE_DOC_SIZE)

        self.seq = 0
        self.ids = deque(maxlen=ID_BUFFER_SIZE)
        self.last_ack_at = None
        self.t0 = None
        self.t1 = None

        self._pause = False
        self._stop = False
        self._await_resume = False
        self._paused_signal = threading.Event()
        self._resumed_signal = threading.Event()

    # Write loop ---------------------------------------------------------------

    @property
    def label(self) -> str:
        return self.labels[self.side]

    def _coll(self):
        return self.client[self.database][self.collection]

    def _log(self, op: str, seq: int) -> None:
        print(f"{timestamp()}  {self.label:<12} {op:<6}  {self.namespace}  seq={seq}")

    def _insert(self) -> int:
        self.seq += 1
        doc = generate_document(self.seq, self.seed, self.padding)
        doc["written_at"] = datetime.now(UTC)
        doc["metadata"]["source"] = "demo"
        self._coll().insert_one(doc)
        self.ids.append(doc["_id"])
        return self.seq

    def _update(self) -> None:
        doc_id = random.choice(self.ids)
        result = self._coll().update_one(
            {"_id": doc_id},
            {
                "$set": {
                    "status": random.choice(STATUSES),
                    "written_at": datetime.now(UTC),
                }
            },
        )
        if result.matched_count == 0:
            self.ids.remove(doc_id)

    def _delete(self) -> None:
        doc_id = random.choice(self.ids)
        self._coll().delete_one({"_id": doc_id})
        self.ids.remove(doc_id)

    def _do_op(self) -> bool:
        op = random.choices(OP_KINDS, weights=OP_WEIGHTS)[0]
        if op != "insert" and not self.ids:
            op = "insert"

        try:
            if op == "insert":
                self._log(op, self._insert())
            elif op == "update":
                self._update()
                self._log(op, self.seq)
            else:
                self._delete()
                self._log(op, self.seq)
        except PyMongoError as e:
            print(f"{timestamp()}  {self.label:<12} ERROR  {op}: {type(e).__name__}")
            return False

        return True

    def run(self) -> None:
        """Write loop. Runs on its own thread until stop() is called."""
        while not self._stop:
            if self._pause:
                if not self._paused_signal.is_set():
                    self.t0 = self.last_ack_at if self.last_ack_at is not None else time.monotonic()
                    self._paused_signal.set()
                time.sleep(0.02)
                continue

            acked = self._do_op()
            if acked:
                self.last_ack_at = time.monotonic()
                if self._await_resume and not self._resumed_signal.is_set():
                    self.t1 = self.last_ack_at
                    self._resumed_signal.set()

            time.sleep(self.interval)

    # Controls -----------------------------------------------------------------

    def pause(self) -> None:
        """Stop writing and start the stopwatch once the last write is acknowledged."""
        self._paused_signal.clear()
        self._pause = True
        if not self._paused_signal.wait(timeout=ACK_TIMEOUT):
            raise TimeoutError("writer did not acknowledge pause")

    def repoint(self) -> None:
        """Switch to the target cluster. Only swaps after the target answers a ping."""
        self.target.admin.command("ping")
        self.client = self.target
        self.side = "target"

    def resume(self) -> float:
        """Start writing again and stop the stopwatch on the first acknowledged write."""
        self._await_resume = True
        self._resumed_signal.clear()
        self._pause = False
        if not self._resumed_signal.wait(timeout=ACK_TIMEOUT):
            raise TimeoutError("writer did not acknowledge resume")

        self._await_resume = False
        if self.t0 is None or self.t1 is None:
            raise RuntimeError("writer stopwatch is incomplete")
        return self.t1 - self.t0

    def stop(self) -> None:
        self._stop = True
        self._pause = False


class PauseTicker:
    """Prints how long writes have been paused, once every PAUSE_TICK_INTERVAL seconds."""

    def __init__(self):
        self._stop = threading.Event()
        self._thread = None

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        started = time.monotonic()
        while not self._stop.wait(PAUSE_TICK_INTERVAL):
            print(f"{timestamp()}  ... writes paused for {time.monotonic() - started:.0f}s")

    def stop(self) -> None:
        self._stop.set()


HELP = """Commands: pause | repoint | resume | status | quit"""


def handle_command(command: str, writer: DemoWriter, ticker: PauseTicker) -> bool:
    """Run one command. Returns False when the writer should exit."""
    if command in ("quit", "exit"):
        return False

    if command == "status":
        state = "PAUSED" if writer._pause else "WRITING"
        print(
            f"  {state}  side={writer.label}  namespace={writer.namespace}  "
            f"seq={writer.seq}  buffered_ids={len(writer.ids)}"
        )
        return True

    if command == "pause":
        if writer._pause:
            print("  REJECTED: already paused")
            return True
        writer.pause()
        print(f"\n  WRITES PAUSED on {writer.label} - stopwatch running\n")
        ticker.start()
        return True

    if command == "repoint":
        if not writer._pause:
            print("  REJECTED: pause before repointing")
            return True
        if writer.side == "target":
            print(f"  REJECTED: already writing to {writer.label}")
            return True
        try:
            writer.repoint()
        except PyMongoError as e:
            print(f"  REJECTED: target did not answer ping: {type(e).__name__}")
            return True
        print(f"  REPOINT -> {writer.label}  ping ok")
        return True

    if command == "resume":
        if not writer._pause:
            print("  REJECTED: not paused")
            return True
        elapsed = writer.resume()
        ticker.stop()
        print("")
        print("  " + "=" * 46)
        print(f"  APPLICATION WRITES PAUSED FOR  {elapsed:.3f} s")
        print("  " + "=" * 46)
        print(f"  now writing to {writer.label}\n")
        return True

    print(f"  unknown command: {command!r}")
    print(f"  {HELP}")
    return True


def main():
    args = parse_args()

    source_uri = resolve_cluster_uri("source", "SOURCE_URI", "SRC_URI")
    target_uri = resolve_cluster_uri("target", "TARGET_URI", "TGT_URI")

    source = None
    target = None
    try:
        source = pymongo.MongoClient(source_uri)
        source.admin.command("ping")
        target = pymongo.MongoClient(target_uri)
        target.admin.command("ping")
    except PyMongoError as e:
        if source is not None:
            source.close()
        if target is not None:
            target.close()
        print(f"ERROR: unable to connect to source or target cluster: {type(e).__name__}")
        sys.exit(1)

    writer = DemoWriter(source, target, args)
    ticker = PauseTicker()

    print("PCSM Demo Writer")
    print(f"  source:     {args.source_label}")
    print(f"  target:     {args.target_label}")
    print(f"  namespace:  {writer.namespace}")
    print(f"  rate:       {args.rate} ops/s")
    print(f"  {HELP}\n")

    thread = threading.Thread(target=writer.run, daemon=True)
    thread.start()

    try:
        while True:
            command = input().strip().lower()
            if not command:
                continue
            if not handle_command(command, writer, ticker):
                break
    except (EOFError, KeyboardInterrupt):
        pass
    finally:
        ticker.stop()
        writer.stop()
        thread.join(timeout=5)
        source.close()
        target.close()
        print("\nWriter stopped.")


if __name__ == "__main__":
    main()
