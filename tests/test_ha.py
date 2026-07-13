# pylint: disable=missing-docstring,redefined-outer-name
"""Multi-instance HA E2E tests.

These are isolated from the rest of the suite: the whole module is marked slow,
so it is skipped by default (and in CI) and only runs with --runslow, e.g.

    .venv/bin/pytest tests/test_ha.py --runslow

They require TEST_PCSM_BIN (the module skips otherwise) and running source/target
clusters. The tests spin up their own 3-instance PCSM group on dedicated ports;
they temporarily suspend the session-managed single PCSM (port 2242) so it does
not compete for the same lease.
"""

import os
import threading
import time

import pytest
import testing
from pymongo import MongoClient

from ha import PCSMCluster

pytestmark = pytest.mark.slow

# Dedicated ports for the HA group, away from the session server's 2242.
HA_PORTS = [int(p) for p in os.getenv("TEST_PCSM_HA_PORTS", "2252,2253,2254").split(",")]


@pytest.fixture(scope="module", autouse=True)
def _require_bin(pcsm_bin: str):
    """Skip the whole module unless a managed binary is available."""
    if not pcsm_bin:
        pytest.skip("TEST_PCSM_BIN not set; HA tests need a managed binary")


@pytest.fixture
def ha_cluster(
    pcsm_bin,
    source_uri_str,
    target_uri_str,
    suspend_managed_pcsm,  # noqa: ARG001 - stops the singleton PCSM for these tests
    drop_all_database,  # noqa: ARG001 - ordering dep: wipe runs before the group starts
):
    """A fresh 3-instance HA group per test.

    suspend_managed_pcsm stops the session singleton (which would otherwise
    compete for the lease); drop_all_database orders the source/target wipe
    before the group starts. The group is torn down afterwards.
    """
    cluster = PCSMCluster(pcsm_bin, source_uri_str, target_uri_str, HA_PORTS)
    cluster.start()
    try:
        yield cluster
    finally:
        cluster.stop()


class _Writer:
    """Background thread inserting incrementing docs into one source collection."""

    def __init__(self, source: MongoClient, db: str, coll: str):
        self._coll = source[db][coll]
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self.inserted = 0

    def _run(self):
        i = 0
        while not self._stop.is_set():
            try:
                self._coll.insert_one({"_id": i, "n": i})
                self.inserted = i + 1
                i += 1
            except Exception:  # noqa: BLE001 - keep writing through transient blips
                pass
            time.sleep(0.002)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *_):
        self._stop.set()
        self._thread.join(timeout=10)


def _wait_source_target_count(source, target, db, coll, timeout=60):
    """Wait until target count catches up to the (now-frozen) source count."""
    expected = source[db][coll].count_documents({})
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if target[db][coll].count_documents({}) >= expected:
            return expected
        time.sleep(0.5)
    actual = target[db][coll].count_documents({})
    raise AssertionError(f"target {db}.{coll}: expected {expected}, got {actual} after {timeout}s")


@pytest.mark.timeout(120)
def test_single_active_invariant(ha_cluster: PCSMCluster):
    """Exactly one ACTIVE across the group, preserved across a failover."""
    active1 = ha_cluster.active()
    assert len(ha_cluster.standbys()) == len(HA_PORTS) - 1

    ha_cluster.kill_active()
    ha_cluster.wait_for_single_active()

    active2 = ha_cluster.active()
    assert active2.is_alive
    # A different instance took over (the killed one is dead).
    assert active2.port != active1.port


@pytest.mark.timeout(120)
def test_standby_rejects_writes(ha_cluster: PCSMCluster):
    """Write commands on a STANDBY return HTTP 409 with a not_active body."""
    standbys = ha_cluster.standbys()
    assert standbys, "expected at least one STANDBY"

    code, body = standbys[0].client.start_expect_conflict()
    assert code == 409, body
    assert body.get("error") == "not_active", body
    assert body.get("role") == "STANDBY", body
    # The 409 envelope should point at the ACTIVE instance.
    assert "message" in body


@pytest.mark.timeout(600)
def test_failover_data_integrity(ha_cluster: PCSMCluster, source_conn, target_conn):
    """Data survives repeated ACTIVE crashes during replication.

    Start replication on the ACTIVE, stream writes into the source while killing
    and restarting the ACTIVE several times, then finalize and compare source and
    target. This is the deterministic reproducer for failover-time data loss.
    """
    db, coll = "ha_db", "events"

    active = ha_cluster.active()
    active.client.start()

    # Wait for initial sync to complete on the current ACTIVE.
    _wait_initial_sync(active)

    with _Writer(source_conn, db, coll):
        for _ in range(3):
            killed = ha_cluster.kill_active()
            ha_cluster.wait_for_single_active()
            # The new ACTIVE must actually be replicating, not sitting idle.
            _assert_active_running(ha_cluster)
            # Bring the killed instance back so it rejoins as STANDBY.
            killed.start()
            ha_cluster.wait_for_single_active()
            time.sleep(2)  # let writes flow under the new ACTIVE

    # Writer stopped: source count is now frozen. Wait for target to catch up.
    _wait_source_target_count(source_conn, target_conn, db, coll)

    # Finalize on whoever is ACTIVE now, then compare everything.
    active = ha_cluster.active()
    active.client.finalize()

    t = testing.Testing(source_conn, target_conn, ha_cluster.active().client)
    t.compare_all()


def _assert_active_running(cluster, timeout=15):
    """Assert the current ACTIVE reaches state 'running' (resumed replication).

    Catches the failure mode where a promoted instance stays idle because it
    could not resume the run the previous ACTIVE began.
    """
    active = cluster.active()
    deadline = time.monotonic() + timeout
    state = None
    while time.monotonic() < deadline:
        payload = active.try_status()
        if payload:
            state = payload.get("state")
            if state == "running":
                return
        time.sleep(0.5)
    raise AssertionError(f"promoted ACTIVE on port {active.port} not running: state={state}")


def _wait_initial_sync(inst, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        payload = inst.try_status()
        if payload and payload.get("initialSync", {}).get("completed"):
            return
        time.sleep(0.5)
    raise AssertionError("initial sync did not complete in time")
