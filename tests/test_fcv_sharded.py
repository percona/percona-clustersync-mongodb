"""Startup FCV gate behind mongos (PCSM-394).

mongos has no FCV of its own: `getParameter` through it reports the binary's
latest FCV, so only `admin.system.version` shows a lowered FCV. This test lowers
the target FCV on a sharded cluster and asserts PCSM refuses to start, then
starts once the FCV is raised back.

Lowering the FCV needs a dedicated config server on the target, which is not
the default topology. Start it with `TGT_DEDICATED_CONFIGSVR=1 hack/sh/run.sh`.
"""

# pylint: disable=missing-docstring,redefined-outer-name
import subprocess
import time

import pytest
from pymongo import MongoClient

from pcsm import PCSM

HIGH_FCV = "8.0"
LOW_FCV = "7.0"
REJECT_TIMEOUT_SECS = 30
READY_TIMEOUT_SECS = 30


def _fcv(client: MongoClient) -> str:
    """FCV as stored in admin.system.version, which mongos routes to the config server."""
    doc = client["admin"]["system.version"].find_one({"_id": "featureCompatibilityVersion"})
    assert doc is not None, "featureCompatibilityVersion document not found"
    return doc["version"]


def _set_fcv(client: MongoClient, version: str):
    client.admin.command("setFeatureCompatibilityVersion", version, confirm=True)
    assert _fcv(client) == version


def _has_config_shard(client: MongoClient) -> bool:
    return any(s["_id"] == "config" for s in client.admin.command("listShards")["shards"])


def _binary_version(client: MongoClient) -> str:
    return client.admin.command("buildInfo")["version"]


def _run_pcsm(pcsm_bin: str, source: str, target: str):
    return subprocess.Popen(
        [pcsm_bin, "--source", source, "--target", target, "--reset-state", "--log-level=debug"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def _wait_for_idle(proc: subprocess.Popen, pcsm: PCSM):
    deadline = time.monotonic() + READY_TIMEOUT_SECS
    last_err = "no response"
    while time.monotonic() < deadline:
        assert proc.poll() is None, f"PCSM exited with code {proc.returncode}"
        try:
            state = pcsm.status().get("state")
            if state == PCSM.State.IDLE:
                return
            last_err = f"state={state}"
        except Exception as exc:  # noqa: BLE001 - the server may not be listening yet
            last_err = str(exc)
        time.sleep(0.2)
    raise TimeoutError(f"PCSM not ready within {READY_TIMEOUT_SECS}s: {last_err}")


@pytest.fixture
def lowered_target_fcv(
    source_conn: MongoClient,
    target_conn: MongoClient,
    pcsm_bin: str,
    suspend_managed_pcsm,
):
    if not pcsm_bin:
        pytest.skip("TEST_PCSM_BIN not set; the test runs its own PCSM process")

    for name, client in (("source", source_conn), ("target", target_conn)):
        if not _binary_version(client).startswith(f"{HIGH_FCV}."):
            pytest.skip(f"needs {name} on {HIGH_FCV} binaries")

        if _fcv(client) != HIGH_FCV:
            pytest.skip(f"needs {name} at FCV {HIGH_FCV}")

    if _has_config_shard(target_conn):
        pytest.skip("target has a config shard; set TGT_DEDICATED_CONFIGSVR=1")

    _set_fcv(target_conn, LOW_FCV)
    try:
        yield
    finally:
        _set_fcv(target_conn, HIGH_FCV)


def test_source_fcv_above_target_behind_mongos(
    lowered_target_fcv,  # pylint: disable=unused-argument
    suspend_managed_pcsm,  # pylint: disable=unused-argument
    pcsm: PCSM,
    pcsm_bin: str,
    target_conn: MongoClient,
    source_uri_str: str,
    target_uri_str: str,
):
    # The reason the test exists: getParameter through mongos does not see the
    # lowered FCV, so PCSM must read admin.system.version instead.
    assert _fcv(target_conn) == LOW_FCV
    reported = target_conn.admin.command("getParameter", 1, featureCompatibilityVersion=1)
    assert reported["featureCompatibilityVersion"]["version"] == HIGH_FCV, (
        "getParameter through mongos is expected to report the binary FCV; "
        "if it now reports the cluster FCV this test no longer guards the mongos path"
    )

    proc = _run_pcsm(pcsm_bin, source_uri_str, target_uri_str)
    try:
        output, _ = proc.communicate(timeout=REJECT_TIMEOUT_SECS)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate()
        pytest.fail("PCSM did not exit although the source FCV is higher than the target FCV")

    assert proc.returncode != 0, output
    assert f"source FCV {HIGH_FCV} is higher than target FCV {LOW_FCV}" in output, output

    _set_fcv(target_conn, HIGH_FCV)

    proc = _run_pcsm(pcsm_bin, source_uri_str, target_uri_str)
    try:
        _wait_for_idle(proc, pcsm)
    finally:
        proc.terminate()
        proc.wait(timeout=30)
