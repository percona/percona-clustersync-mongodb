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


class PLMServerError(Exception):
    """Exception raised when there is an error with the PLM service."""

    def __init__(self, message):
        super().__init__(message)

    def __str__(self):
        return self.args[0]


class PLM:
    """PLM provides methods to interact with the PLM service."""

    class State(StrEnum):
        """State represents the state of the PLM service."""

        FAILED = "failed"
        IDLE = "idle"
        RUNNING = "running"
        PAUSED = "paused"
        FINALIZING = "finalizing"
        FINALIZED = "finalized"

    def __init__(self, uri: str):
        """Initialize PLM with the given URI."""
        self.uri = uri

    def status(self):
        """Get the current status of the PLM service."""
        res = requests.get(f"{self.uri}/status", timeout=DFL_REQ_TIMEOUT)
        res.raise_for_status()

        payload = res.json()
        if not payload["ok"]:
            raise PLMServerError(payload["error"])

        return payload

    def start(self, include_namespaces=None, exclude_namespaces=None, pause_on_initial_sync=False):
        """Start the PLM service with the given parameters."""
        options = {"pauseOnInitialSync": pause_on_initial_sync}
        if include_namespaces:
            options["includeNamespaces"] = include_namespaces
        if exclude_namespaces:
            options["excludeNamespaces"] = exclude_namespaces

        res = requests.post(f"{self.uri}/start", json=options, timeout=DFL_REQ_TIMEOUT)
        res.raise_for_status()

        payload = res.json()
        if not payload["ok"]:
            raise PLMServerError(payload["error"])

        return payload

    def pause(self):
        """Pause the PLM service."""
        res = requests.post(f"{self.uri}/pause", timeout=DFL_REQ_TIMEOUT)
        res.raise_for_status()

        payload = res.json()
        if not payload["ok"]:
            raise PLMServerError(payload["error"])

        return payload

    def resume(self):
        """Resume the PLM service."""
        res = requests.post(f"{self.uri}/resume", timeout=DFL_REQ_TIMEOUT)
        res.raise_for_status()

        payload = res.json()
        if not payload["ok"]:
            raise PLMServerError(payload["error"])

        return payload

    def finalize(self):
        """Finalize the PLM service."""
        res = requests.post(f"{self.uri}/finalize", timeout=DFL_REQ_TIMEOUT)
        res.raise_for_status()

        payload = res.json()
        if not payload["ok"]:
            raise PLMServerError(payload["error"])

        return payload


class Runner:
    """Runner manages the lifecycle of the PLM service."""

    class Phase(StrEnum):
        """Phase represents the phase of the PLM service."""

        CLONE = "phase:clone"
        APPLY = "phase:apply"
        MANUAL = "manual"  # manual mode

    def __init__(
        self,
        source: MongoClient,
        plm: PLM,
        phase: Phase,
        options: dict,
        wait_timeout=None,
    ):
        self.source: MongoClient = source
        self.plm = plm
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
        """Start the PLM service."""
        self.finalize(fast=True)
        self.plm.start(pause_on_initial_sync=pause_on_initial_sync, **self.options)

    def finalize(self, *, fast=False):
        """Finalize the PLM service."""
        state = self.plm.status()

        if state["state"] == PLM.State.PAUSED:
            if state["initialSync"]["cloneCompleted"]:
                self.plm.resume()
                state = self.plm.status()

        if state["state"] == PLM.State.RUNNING:
            if not fast:
                self.wait_for_current_optime()
            self.wait_for_initial_sync()
            self.plm.finalize()
            state = self.plm.status()

        if state["state"] == PLM.State.FINALIZING:
            if not fast:
                self.wait_for_state(PLM.State.FINALIZED)

    def wait_for_state(self, state: PLM.State):
        """Wait for the PLM service to reach the specified state."""
        if self.plm.status()["state"] == state:
            return

        for _ in range(self.wait_timeout * 2):
            time.sleep(0.5)
            if self.plm.status()["state"] == state:
                return

        raise WaitTimeoutError()

    def wait_for_current_optime(self):
        """Wait for the current operation time to be applied."""
        status = self.plm.status()
        assert status["state"] == PLM.State.RUNNING, status

        curr_optime = self.source.server_info()["$clusterTime"]["clusterTime"]
        for _ in range(self.wait_timeout * 2):
            if curr_optime <= self.last_applied_op:
                return

            time.sleep(0.5)
            status = self.plm.status()

        raise WaitTimeoutError()

    def wait_for_initial_sync(self):
        """Wait for the PLM service to be finalizable."""
        status = self.plm.status()
        assert status["state"] != PLM.State.IDLE, status

        if status["initialSync"]["completed"]:
            return

        assert status["state"] == PLM.State.RUNNING, status

        for _ in range(self.wait_timeout * 2):
            if status["initialSync"]["completed"]:
                return

            time.sleep(0.5)
            status = self.plm.status()

        raise WaitTimeoutError()

    def wait_for_clone_completed(self):
        """Wait for the PLM service completed clone."""
        status = self.plm.status()
        assert status["state"] != PLM.State.IDLE, status

        for _ in range(self.wait_timeout * 2):
            if status["initialSync"]["cloneCompleted"]:
                return

            time.sleep(0.5)
            status = self.plm.status()

        raise WaitTimeoutError()

    @property
    def last_applied_op(self):
        """Get the last applied operation time."""
        status = self.plm.status()
        if last_replicated_op_time := status.get("lastReplicatedOpTime"):
            t_s, i_s = last_replicated_op_time.split(".")
            return bson.Timestamp(int(t_s), int(i_s))

        return bson.Timestamp(0, 0)
