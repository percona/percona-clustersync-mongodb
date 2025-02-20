# pylint: disable=missing-docstring,redefined-outer-name
import time
from enum import StrEnum

import bson
import requests
from pymongo import MongoClient

# default HTTP request read timeout (in seconds)
DFL_REQ_TIMEOUT = 5


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
        return res.json()

    def start(self, params):
        """Start the MongoLink service with the given parameters."""
        res = requests.post(f"{self.uri}/start", json=params, timeout=DFL_REQ_TIMEOUT)
        res.raise_for_status()
        return res.json()

    def finalize(self):
        """Finalize the MongoLink service."""
        res = requests.post(f"{self.uri}/finalize", timeout=DFL_REQ_TIMEOUT)
        res.raise_for_status()
        return res.json()


class WaitTimeoutError(Exception):
    """Exception raised when a wait operation times out."""


class Runner:
    """Runner manages the lifecycle of the MongoLink service."""

    class Phase(StrEnum):
        """Phase represents the phase of the MongoLink service."""

        CLONE = "phase:clone"
        APPLY = "phase:apply"

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
            self.wait_for_finalizable()
            self.mlink.finalize()
            self.wait_for_state(MongoLink.State.FINALIZED)

        self.mlink.start(self.options)
        self.wait_for_state(MongoLink.State.RUNNING)
        return self

    def finalize_fast(self):
        """Finalize the MongoLink service quickly."""
        status = self.mlink.status()
        if status["state"] == MongoLink.State.RUNNING:
            self.wait_for_finalizable()
            self.mlink.finalize()

    def finalize(self):
        """Finalize the MongoLink service."""
        status = self.mlink.status()
        if status["state"] == MongoLink.State.FINALIZING:
            self.wait_for_state(MongoLink.State.FINALIZED)
        elif status["state"] == MongoLink.State.RUNNING:
            self.wait_for_current_optime()
            self.wait_for_finalizable()
            self.mlink.finalize()
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
        assert status["state"] == MongoLink.State.RUNNING

        curr_optime = self.source.server_info()["operationTime"]
        for _ in range(timeout * 10):
            applied_optime = self.last_applied_op()
            if curr_optime <= applied_optime:
                return

            time.sleep(0.1)
            status = self.mlink.status()

        raise WaitTimeoutError()

    def wait_for_finalizable(self, timeout=10):
        """Wait for the MongoLink service to be finalizable."""
        status = self.mlink.status()
        assert status["state"] == MongoLink.State.RUNNING

        for _ in range(timeout * 10):
            if status.get("finalizable"):
                return

            time.sleep(0.1)
            status = self.mlink.status()

        raise WaitTimeoutError()

    def last_applied_op(self):
        """Get the last applied operation time."""
        status = self.mlink.status()
        if applied_optime := status.get("lastAppliedOpTime"):
            t_s, i_s = applied_optime.split(".")
            return bson.Timestamp(int(t_s), int(i_s))

        return bson.Timestamp(0, 0)
