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


class MongoLinkServerError(Exception):
    """Exception raised when there is an error with the MongoLink service."""

    def __init__(self, message):
        super().__init__(message)

    def __str__(self):
        return self.args[0]


class MongoLink:
    """MLink provides methods to interact with the MongoLink service."""

    class State(StrEnum):
        """State represents the state of the MongoLink service."""

        FAILED = "failed"
        IDLE = "idle"
        RUNNING = "running"
        PAUSED = "paused"
        FINALIZING = "finalizing"
        FINALIZED = "finalized"

    def __init__(self, uri: str):
        """Initialize MLink with the given URI."""
        self.uri = uri

    def status(self):
        """Get the current status of the MongoLink service."""
        res = requests.get(f"{self.uri}/status", timeout=DFL_REQ_TIMEOUT)
        res.raise_for_status()

        payload = res.json()
        if not payload["ok"]:
            raise MongoLinkServerError(payload["error"])

        return payload

    def start(self, include_namespaces=None, exclude_namespaces=None, pause_on_initial_sync=False):
        """Start the MongoLink service with the given parameters."""
        options = {"pauseOnInitialSync": pause_on_initial_sync}
        if include_namespaces:
            options["includeNamespaces"] = include_namespaces
        if exclude_namespaces:
            options["excludeNamespaces"] = exclude_namespaces

        res = requests.post(f"{self.uri}/start", json=options, timeout=DFL_REQ_TIMEOUT)
        res.raise_for_status()

        payload = res.json()
        if not payload["ok"]:
            raise MongoLinkServerError(payload["error"])

        return payload

    def pause(self):
        """Pause the MongoLink service."""
        res = requests.post(f"{self.uri}/pause", timeout=DFL_REQ_TIMEOUT)
        res.raise_for_status()

        payload = res.json()
        if not payload["ok"]:
            raise MongoLinkServerError(payload["error"])

        return payload

    def resume(self):
        """Resume the MongoLink service."""
        res = requests.post(f"{self.uri}/resume", timeout=DFL_REQ_TIMEOUT)
        res.raise_for_status()

        payload = res.json()
        if not payload["ok"]:
            raise MongoLinkServerError(payload["error"])

        return payload

    def finalize(self):
        """Finalize the MongoLink service."""
        res = requests.post(f"{self.uri}/finalize", timeout=DFL_REQ_TIMEOUT)
        res.raise_for_status()

        payload = res.json()
        if not payload["ok"]:
            raise MongoLinkServerError(payload["error"])

        return payload


class Runner:
    """Runner manages the lifecycle of the MongoLink service."""

    class Phase(StrEnum):
        """Phase represents the phase of the MongoLink service."""

        CLONE = "phase:clone"
        APPLY = "phase:apply"
        MANUAL = "manual"  # manual mode

    def __init__(
        self,
        source: MongoClient,
        mlink: MongoLink,
        phase: Phase,
        options: dict,
        wait_timeout=None,
    ):
        self.source: MongoClient = source
        self.mlink = mlink
        self.phase = phase
        self.options = options
        self.wait_timeout = wait_timeout or 10

    def __enter__(self):
        if self.phase == self.Phase.APPLY:
            self.start()

        return self

    def __exit__(self, _t, exc, _tb):
        if exc:
            self.finalize(fast=True)
            return

        if self.phase == self.Phase.CLONE:
            self.start(pause_on_initial_sync=True)

        self.finalize()

    def start(self, pause_on_initial_sync=False):
        """Start the MongoLink service."""
        self.finalize(fast=True)
        self.mlink.start(pause_on_initial_sync=pause_on_initial_sync, **self.options)

    def finalize(self, *, fast=False):
        """Finalize the MongoLink service."""
        state = self.mlink.status()

        if state["state"] == MongoLink.State.PAUSED:
            if state["initialSync"]["completed"]:
                self.mlink.resume()
                state = self.mlink.status()

        if state["state"] == MongoLink.State.RUNNING:
            if not fast:
                self.wait_for_current_optime()
            self.wait_for_initial_sync()
            self.mlink.finalize()
            state = self.mlink.status()

        if state["state"] == MongoLink.State.FINALIZING:
            if not fast:
                self.wait_for_state(MongoLink.State.FINALIZED)

    def wait_for_state(self, state: MongoLink.State):
        """Wait for the MongoLink service to reach the specified state."""
        if self.mlink.status()["state"] == state:
            return

        for _ in range(self.wait_timeout * 2):
            time.sleep(0.5)
            if self.mlink.status()["state"] == state:
                return

        raise WaitTimeoutError()

    def wait_for_current_optime(self):
        """Wait for the current operation time to be applied."""
        status = self.mlink.status()
        assert status["state"] == MongoLink.State.RUNNING, status

        curr_optime = self.source.server_info()["$clusterTime"]["clusterTime"]
        for _ in range(self.wait_timeout * 2):
            if curr_optime <= self.last_applied_op:
                return

            time.sleep(0.5)
            status = self.mlink.status()

        raise WaitTimeoutError()

    def wait_for_initial_sync(self):
        """Wait for the MongoLink service to be finalizable."""
        status = self.mlink.status()
        assert status["state"] != MongoLink.State.IDLE, status

        if status["initialSync"]["completed"]:
            return

        assert status["state"] == MongoLink.State.RUNNING, status

        for _ in range(self.wait_timeout * 2):
            if status["initialSync"]["completed"]:
                return

            time.sleep(0.5)
            status = self.mlink.status()

        raise WaitTimeoutError()

    @property
    def is_paused(self):
        return self.mlink.status()["state"] == MongoLink.State.PAUSED

    @property
    def current_optime(self):
        return self.source.server_info()["$clusterTime"]["clusterTime"]

    @property
    def last_applied_op(self):
        """Get the last applied operation time."""
        status = self.mlink.status()
        if last_replicated_op_time := status.get("lastReplicatedOpTime"):
            t_s, i_s = last_replicated_op_time.split(".")
            return bson.Timestamp(int(t_s), int(i_s))

        return bson.Timestamp(0, 0)
