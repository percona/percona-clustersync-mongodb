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


class MongoLink:
    """MLink provides methods to interact with the MongoLink service."""

    class State(StrEnum):
        """State represents the state of the MongoLink service."""

        FAILED = "failed"
        IDLE = "idle"
        RUNNING = "running"
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
        if not payload["ok"] or payload["state"] == self.State.FAILED:
            raise MongoLinkServerError(payload["error"])

        return payload

    def start(self, include_namespaces=None, exclude_namespaces=None, pause_on_initial_sync=None):
        """Start the MongoLink service with the given parameters."""
        options = {}
        if include_namespaces:
            options["includeNamespaces"] = include_namespaces
        if exclude_namespaces:
            options["excludeNamespaces"] = exclude_namespaces
        if pause_on_initial_sync:
            options["pauseOnInitialSync"] = True

        res = requests.post(f"{self.uri}/start", json=options, timeout=DFL_REQ_TIMEOUT)
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

    def __init__(self, source: MongoClient, mlink: MongoLink, phase: Phase, options: dict):
        """Initialize Runner with the given source, mlink, phase, and options."""
        self.source: MongoClient = source
        self.mlink = mlink
        self.phase = phase
        self.options = options

    def __enter__(self):
        """Enter the context manager."""
        if self.phase is self.Phase.APPLY:
            self.start()

        return self

    def __exit__(self, _t, exc, _tb):
        """Exit the context manager."""
        if exc:
            self.finalize_fast()
            return

        if self.phase is self.Phase.CLONE:
            self.start()

        self.finalize()

    def start(self):
        """Start the MongoLink service."""
        status = self.mlink.status()
        if status["state"] == MongoLink.State.FINALIZING:
            self.wait_for_state(MongoLink.State.FINALIZED)
        elif status["state"] == MongoLink.State.RUNNING:
            if self.phase is self.Phase.APPLY:
                self.wait_for_initial_sync()
            self.finalize_fast()
            self.wait_for_state(MongoLink.State.FINALIZED)

        pause_on_initial_sync = self.phase is not self.Phase.CLONE
        self.mlink.start(pause_on_initial_sync=pause_on_initial_sync, **self.options)
        return self

    def finalize_fast(self):
        """Finalize the MongoLink service quickly."""
        status = self.mlink.status()
        if status["state"] == MongoLink.State.RUNNING:
            if self.phase is self.Phase.APPLY:
                self.wait_for_initial_sync()

            try:
                self.mlink.finalize()
            except MongoLinkServerError as e:
                if e == MongoLink.State.FAILED:
                    return

                raise

    def finalize(self):
        """Finalize the MongoLink service."""
        status = self.mlink.status()
        if status["state"] == MongoLink.State.FINALIZING:
            self.wait_for_state(MongoLink.State.FINALIZED)
        elif status["state"] == MongoLink.State.RUNNING:
            self.wait_for_current_optime()
            if self.phase is self.Phase.APPLY:
                self.wait_for_initial_sync()

            try:
                self.mlink.finalize()
            except MongoLinkServerError as e:
                if e == MongoLink.State.FAILED:
                    return

                raise

            self.wait_for_state(MongoLink.State.FINALIZED)

    def wait_for_state(self, state):
        """Wait for the MongoLink service to reach the specified state."""
        status = self.mlink.status()
        while status["state"] != state:
            time.sleep(0.1)
            status = self.mlink.status()

    def wait_for_current_optime(self, timeout=10):
        """Wait for the current operation time to be applied."""
        status = self.mlink.status()
        assert status["state"] == MongoLink.State.RUNNING, status

        curr_optime = self.source.server_info()["$clusterTime"]["clusterTime"]
        for _ in range(timeout * 10):
            if curr_optime <= self.last_applied_op():
                return

            time.sleep(0.1)
            status = self.mlink.status()

        raise WaitTimeoutError()

    def wait_for_initial_sync(self, timeout=10):
        """Wait for the MongoLink service to be finalizable."""
        status = self.mlink.status()
        if status.get("initialSync", {}).get("completed"):
            return

        assert status["state"] == MongoLink.State.RUNNING, status

        for _ in range(timeout * 10):
            if status.get("initialSync", {}).get("completed"):
                return

            time.sleep(0.1)
            status = self.mlink.status()

        raise WaitTimeoutError()

    def last_applied_op(self):
        """Get the last applied operation time."""
        status = self.mlink.status()
        if last_replicated_op_time := status.get("lastReplicatedOpTime"):
            t_s, i_s = last_replicated_op_time.split(".")
            return bson.Timestamp(int(t_s), int(i_s))

        return bson.Timestamp(0, 0)
