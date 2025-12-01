# pylint: disable=missing-docstring,redefined-outer-name
import time
from enum import StrEnum

import bson
import requests
from pymongo import MongoClient

# default HTTP request read timeout (in seconds)
DFL_REQ_TIMEOUT = 5


class WaitTimeoutError(Exception):
    """Exception raised when a wait operation times out."""


class PCSMServerError(Exception):
    """Exception raised when there is an error with the PCSM service."""

    def __init__(self, message):
        super().__init__(message)

    def __str__(self):
        return self.args[0]


class PCSM:
    """PCSM provides methods to interact with the PCSM service."""

    class State(StrEnum):
        """State represents the state of the PCSM service."""

        FAILED = "failed"
        IDLE = "idle"
        RUNNING = "running"
        PAUSED = "paused"
        FINALIZING = "finalizing"
        FINALIZED = "finalized"

    def __init__(self, uri: str):
        """Initialize PCSM with the given URI."""
        self.uri = uri

    def status(self):
        """Get the current status of the PCSM service."""
        res = requests.get(f"{self.uri}/status", timeout=DFL_REQ_TIMEOUT)
        res.raise_for_status()

        payload = res.json()
        if not payload["ok"]:
            raise PCSMServerError(payload["error"])

        return payload

    def start(self, include_namespaces=None, exclude_namespaces=None, pause_on_initial_sync=False):
        """Start the PCSM service with the given parameters."""
        options = {"pauseOnInitialSync": pause_on_initial_sync}
        if include_namespaces:
            options["includeNamespaces"] = include_namespaces
        if exclude_namespaces:
            options["excludeNamespaces"] = exclude_namespaces

        res = requests.post(f"{self.uri}/start", json=options, timeout=DFL_REQ_TIMEOUT)
        res.raise_for_status()

        payload = res.json()
        if not payload["ok"]:
            raise PCSMServerError(payload["error"])

        return payload

    def pause(self):
        """Pause the PCSM service."""
        res = requests.post(f"{self.uri}/pause", timeout=DFL_REQ_TIMEOUT)
        res.raise_for_status()

        payload = res.json()
        if not payload["ok"]:
            raise PCSMServerError(payload["error"])

        return payload

    def resume(self):
        """Resume the PCSM service."""
        res = requests.post(f"{self.uri}/resume", timeout=DFL_REQ_TIMEOUT)
        res.raise_for_status()

        payload = res.json()
        if not payload["ok"]:
            raise PCSMServerError(payload["error"])

        return payload

    def finalize(self):
        """Finalize the PCSM service."""
        res = requests.post(f"{self.uri}/finalize", timeout=DFL_REQ_TIMEOUT)
        res.raise_for_status()

        payload = res.json()
        if not payload["ok"]:
            raise PCSMServerError(payload["error"])

        return payload


class Runner:
    """Runner manages the lifecycle of the PCSM service."""

    class Phase(StrEnum):
        """Phase represents the phase of the PCSM service."""

        CLONE = "phase:clone"
        APPLY = "phase:apply"
        MANUAL = "manual"  # manual mode

    def __init__(
        self,
        source: MongoClient,
        pcsm: PCSM,
        phase: Phase,
        options: dict,
        wait_timeout=None,
    ):
        self.source: MongoClient = source
        self.pcsm = pcsm
        self.phase = phase
        self.options = options
        self.wait_timeout = wait_timeout or 10

    def __enter__(self):
        if self.phase == self.Phase.APPLY:
            self.start()
            self.wait_for_clone_completed()

        return self

    def __exit__(self, _t, exc, _tb):
        if exc:
            self.finalize(fast=True)
            return

        if self.phase == self.Phase.CLONE:
            self.start(pause_on_initial_sync=True)

        self.finalize()

    def start(self, pause_on_initial_sync=False):
        """Start the PCSM service."""
        self.finalize(fast=True)
        self.pcsm.start(pause_on_initial_sync=pause_on_initial_sync, **self.options)

    def finalize(self, *, fast=False):
        """Finalize the PCSM service."""
        state = self.pcsm.status()

        if state["state"] == PCSM.State.PAUSED:
            if state["initialSync"]["cloneCompleted"]:
                self.pcsm.resume()
                state = self.pcsm.status()

        if state["state"] == PCSM.State.RUNNING:
            if not fast:
                self.wait_for_current_optime()
            self.wait_for_initial_sync()
            self.pcsm.finalize()
            state = self.pcsm.status()

        if state["state"] == PCSM.State.FINALIZING:
            if not fast:
                self.wait_for_state(PCSM.State.FINALIZED)

    def wait_for_state(self, state: PCSM.State):
        """Wait for the PCSM service to reach the specified state."""
        if self.pcsm.status()["state"] == state:
            return

        for _ in range(self.wait_timeout * 2):
            time.sleep(0.5)
            if self.pcsm.status()["state"] == state:
                return

        raise WaitTimeoutError()

    def wait_for_current_optime(self):
        """Wait for the current operation time to be applied."""
        status = self.pcsm.status()
        assert status["state"] == PCSM.State.RUNNING, status

        # Force a write operation to ensure we capture a timestamp after all prior writes
        # Using a dummy write ensures operations are serialized in the oplog
        marker_db = "pcsm_marker_db"
        marker_coll = "optime_marker"
        self.source[marker_db][marker_coll].insert_one({"marker": True})

        # Get the cluster time after the marker write - guaranteed to be after all prior ops
        curr_optime = self.source.server_info()["$clusterTime"]["clusterTime"]

        for _ in range(self.wait_timeout * 2):
            if curr_optime <= self.last_applied_op:
                return

            time.sleep(0.5)
            status = self.pcsm.status()

        raise WaitTimeoutError()

    def wait_for_initial_sync(self):
        """Wait for the PCSM service to be finalizable."""
        status = self.pcsm.status()
        assert status["state"] != PCSM.State.IDLE, status

        if status["initialSync"]["completed"]:
            return

        assert status["state"] == PCSM.State.RUNNING, status

        for _ in range(self.wait_timeout * 2):
            if status["initialSync"]["completed"]:
                return

            time.sleep(0.5)
            status = self.pcsm.status()

        raise WaitTimeoutError()

    def wait_for_clone_completed(self):
        """Wait for the PCSM service completed clone."""
        status = self.pcsm.status()
        assert status["state"] != PCSM.State.IDLE, status

        for _ in range(self.wait_timeout * 2):
            if status["initialSync"]["cloneCompleted"]:
                return

            time.sleep(0.5)
            status = self.pcsm.status()

        raise WaitTimeoutError()

    @property
    def last_applied_op(self):
        """Get the last applied operation time."""
        status = self.pcsm.status()
        last_replicated_op_time = status.get("lastReplicatedOpTime", {}).get("ts")
        if last_replicated_op_time:
            t_s, i_s = last_replicated_op_time.split(".")
            return bson.Timestamp(int(t_s), int(i_s))

        return bson.Timestamp(0, 0)
