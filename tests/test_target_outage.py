"""Single-instance regression coverage for a target primary outage (PCSM-378).

The outage is forced deterministically without Docker: every secondary is
frozen (`replSetFreeze`), the primary steps down, and after the requested
duration one secondary is unfrozen and stepped up (`replSetStepUp`) so the
election runs immediately instead of after `electionTimeoutMillis`. This is
the `ReplicaSetNoPrimary` topology from the ticket, with a controlled length.
"""

import logging
import threading
import time

import pymongo
import pytest
import requests
from pymongo import MongoClient
from pymongo.errors import AutoReconnect, PyMongoError
from test_ha import _seed_clone_dataset
from testing import Testing

from pcsm import PCSM, Runner

DB_NAME = "target_outage"
COLL_NAME = "events"
ROLE_TRANSITIONS = "percona_clustersync_mongodb_ha_role_transitions_total"

# HA defaults: lease TTL 10s (configurable with --ha-lease-ttl), renewal
# interval 3s, operation timeout 5s. The lease deadline starts at the last
# successful renewal attempt, up to 3s before the outage, so the short outage
# plus election must stay under ~7s. The long outage exceeds the default TTL
# but stays below the 60s TTL used by the configured cases.
SHORT_OUTAGE_SECS = 5
LONG_OUTAGE_SECS = 15
RECOVERY_TIMEOUT_SECS = 60
# Freeze/step-down period; always ended early by the explicit step-up.
FREEZE_SECS = 120


class _Writer:
    """Keep source writes flowing, with bounded operations and explicit shutdown."""

    def __init__(self, source: MongoClient):
        self._coll = source[DB_NAME][COLL_NAME]
        self._stop = threading.Event()
        self.ready = threading.Event()
        self.writes = 0
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        while not self._stop.is_set():
            try:
                with pymongo.timeout(2):
                    self._coll.insert_one({"n": self.writes})
                self.writes += 1
                self.ready.set()
            except PyMongoError as exc:
                logging.getLogger(__name__).warning("Source writer retrying after error: %s", exc)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *_):
        self._stop.set()
        self._thread.join(timeout=5)
        assert not self._thread.is_alive(), "source writer did not stop"


def _metric(pcsm: PCSM, name: str) -> float:
    response = requests.get(f"{pcsm.uri}/metrics", timeout=5)
    response.raise_for_status()
    for line in response.text.splitlines():
        if line.startswith(f"{name} "):
            return float(line.split()[1])
    raise AssertionError(f"{name} missing from /metrics")


def _role_transitions(pcsm: PCSM) -> float:
    return _metric(pcsm, ROLE_TRANSITIONS)


def _member(host: str) -> MongoClient:
    return MongoClient(f"mongodb://{host}", directConnection=True, timeoutMS=5_000)


def _unfreeze(hosts: list[str]):
    """Best-effort unfreeze of every host; failures are logged, not raised."""
    for host in hosts:
        try:
            with _member(host) as member:
                member.admin.command("replSetFreeze", 0)
        except PyMongoError as exc:
            logging.getLogger(__name__).warning("unfreeze %s failed: %s", host, exc)


def _step_up(host: str):
    """Make `host` run an election now, retrying until it wins."""
    deadline = time.monotonic() + RECOVERY_TIMEOUT_SECS
    last = None
    with _member(host) as member:
        while time.monotonic() < deadline:
            try:
                member.admin.command("replSetStepUp")
                return
            except PyMongoError as exc:
                last = str(exc)
    raise AssertionError(f"{host} did not step up: {last}")


def _force_no_primary(target: MongoClient, secs: int, *, cloning_pcsm: PCSM | None = None):
    hello = target.admin.command("hello")
    primary = hello["primary"]
    secondaries = [h for h in hello["hosts"] if h != primary]
    assert secondaries, f"target replica set has no secondaries to freeze: {hello}"

    try:
        for host in secondaries:
            with _member(host) as member:
                member.admin.command("replSetFreeze", FREEZE_SECS)

        with _member(primary) as member:
            try:
                member.admin.command("replSetStepDown", FREEZE_SECS, force=True)
            except AutoReconnect as exc:
                # The stepped-down primary drops client connections; the
                # command still took effect. Any other error is a real
                # setup failure and propagates.
                logging.getLogger(__name__).info("replSetStepDown disconnected: %s", exc)

        time.sleep(secs)  # The outage duration is the behavior under test.
        if cloning_pcsm is not None:
            status = cloning_pcsm.status()
            assert status["state"] == PCSM.State.RUNNING, status
            assert status["initialSync"]["cloneCompleted"] is False, status
    finally:
        # Unfreeze everything first so a failed step-up cannot leave the set
        # frozen for FREEZE_SECS, then force the election.
        _unfreeze(hello["hosts"])
        _step_up(secondaries[0])


def _wait_for_primary(target: MongoClient):
    deadline = time.monotonic() + RECOVERY_TIMEOUT_SECS
    last = None
    while time.monotonic() < deadline:
        try:
            last = target.admin.command("hello").get("primary")
            if last:
                return last
        except PyMongoError as exc:
            last = str(exc)
    raise AssertionError(f"target replica set did not elect a primary: {last}")


def _wait_for_running(pcsm: PCSM):
    deadline = time.monotonic() + RECOVERY_TIMEOUT_SECS
    last = None
    while time.monotonic() < deadline:
        try:
            code, body = pcsm.raw_status()
            last = (code, body)
            if code == 200 and body.get("state") == PCSM.State.RUNNING:
                return
        except requests.RequestException as exc:
            last = str(exc)
    raise AssertionError(f"PCSM did not return to running within {RECOVERY_TIMEOUT_SECS}s: {last}")


def _check_target_outage(t: Testing, outage_secs: int, expected_transitions: int):
    hello = t.target.admin.command("hello")
    if hello.get("msg") == "isdbgrid":
        pytest.skip("requires a replica-set target to step down its primary")

    t.source[DB_NAME][COLL_NAME].insert_one({"_id": "seed"})
    # Do not use Runner.__exit__: its strict status call would mask the 409
    # regression failure. The session fixture restarts PCSM on test failure.
    runner = t.run(Runner.Phase.MANUAL, wait_timeout=RECOVERY_TIMEOUT_SECS)
    runner.start()
    runner.wait_for_initial_sync()
    runner.wait_for_current_optime()
    before = _role_transitions(t.pcsm)

    with _Writer(t.source) as writer:
        assert writer.ready.wait(timeout=5), "source writer did not produce a document"
        writes_before = writer.writes
        _force_no_primary(t.target, outage_secs)
        writes_after = writer.writes
        _wait_for_primary(t.target)

    assert writes_after > writes_before, "source writer made no progress during the outage"
    _wait_for_running(t.pcsm)
    runner.wait_for_current_optime()
    after = _role_transitions(t.pcsm)
    assert after - before == expected_transitions, (
        f"expected {expected_transitions} HA role transitions: {before=}, {after=}"
    )
    assert t.pcsm.status()["state"] == PCSM.State.RUNNING
    runner.finalize()
    t.compare_all()


def _check_clone_target_outage(t: Testing):
    """Interrupt a proven in-flight clone, then require completion in the same tenure."""
    hello = t.target.admin.command("hello")
    if hello.get("msg") == "isdbgrid":
        pytest.skip("requires a replica-set target to step down its primary")

    _seed_clone_dataset(t.source, DB_NAME, ["c0", "c1", "c2", "c3"])
    runner = t.run(
        Runner.Phase.MANUAL,
        wait_timeout=300,
        options={
            "clone_num_parallel_collections": 1,
            "clone_num_read_workers": 1,
            "clone_num_insert_workers": 1,
        },
    )
    runner.start()
    deadline = time.monotonic() + RECOVERY_TIMEOUT_SECS
    status = None
    while time.monotonic() < deadline:
        status = t.pcsm.status()
        initial_sync = status.get("initialSync", {})
        assert not initial_sync.get("cloneCompleted"), (
            f"clone completed before outage; increase seed size: {status}"
        )
        if status["state"] == PCSM.State.RUNNING and initial_sync.get("clonedSizeBytes", 0) > 0:
            break
    else:
        raise AssertionError(f"PCSM never reached an in-flight clone: {status}")

    before = _role_transitions(t.pcsm)
    term = _metric(t.pcsm, "percona_clustersync_mongodb_ha_term")
    _force_no_primary(t.target, LONG_OUTAGE_SECS, cloning_pcsm=t.pcsm)
    _wait_for_primary(t.target)
    _wait_for_running(t.pcsm)
    runner.wait_for_initial_sync()
    assert t.pcsm.status()["initialSync"]["completed"] is True
    assert _role_transitions(t.pcsm) == before
    assert _metric(t.pcsm, "percona_clustersync_mongodb_ha_term") == term
    runner.finalize()
    t.compare_all()


@pytest.mark.timeout(180)
def test_target_outage_shorter_than_lease_ttl(t: Testing):
    """An outage shorter than the lease TTL must not demote the instance."""
    _check_target_outage(t, outage_secs=SHORT_OUTAGE_SECS, expected_transitions=0)


@pytest.mark.timeout(180)
def test_target_outage_longer_than_lease_ttl(t: Testing):
    """An outage longer than the lease TTL demotes; the next tenure restores from the checkpoint."""
    _check_target_outage(t, outage_secs=LONG_OUTAGE_SECS, expected_transitions=2)


@pytest.mark.slow
@pytest.mark.timeout(600)
@pytest.mark.parametrize("managed_pcsm_args", [["--ha-lease-ttl=60s"]], indirect=True)
def test_clone_target_outage_with_raised_lease_ttl(t: Testing, managed_pcsm_args):
    """A 15s target outage during cloning completes without restarting the run."""
    _check_clone_target_outage(t)


@pytest.mark.timeout(180)
@pytest.mark.parametrize("managed_pcsm_args", [["--ha-lease-ttl=60s"]], indirect=True)
def test_repl_target_outage_with_raised_lease_ttl(t: Testing, managed_pcsm_args):
    """A 15s target outage during replication keeps the ACTIVE tenure."""
    _check_target_outage(t, outage_secs=LONG_OUTAGE_SECS, expected_transitions=0)
