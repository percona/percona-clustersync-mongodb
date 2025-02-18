# pylint: disable=missing-docstring,redefined-outer-name
import time
from enum import StrEnum

import bson
import requests
from pymongo import MongoClient

# default HTTP request read timeout (in seconds)
DFL_REQ_TIMEOUT = 5


class MLink:
    class State(StrEnum):
        FAILED = "failed"
        IDLE = "idle"
        RUNNING = "running"
        FINALIZING = "finalizing"
        FINALIZED = "finalized"

    def __init__(self, uri: str):
        self.uri = uri

    def status(self):
        res = requests.get(f"{self.uri}/status", timeout=DFL_REQ_TIMEOUT)
        res.raise_for_status()
        return res.json()

    def start(self, params):
        res = requests.post(f"{self.uri}/start", json=params, timeout=DFL_REQ_TIMEOUT)
        res.raise_for_status()
        return res.json()

    def finalize(self):
        res = requests.post(f"{self.uri}/finalize", timeout=DFL_REQ_TIMEOUT)
        res.raise_for_status()
        return res.json()


class WaitTimeoutError(Exception):
    pass


class Runner:
    class Phase(StrEnum):
        CLONE = "phase:clone"
        APPLY = "phase:apply"

    def __init__(self, source: MongoClient, mlink: MLink, phase: Phase, options: dict):
        self.source: MongoClient = source
        self.mlink = mlink
        self.phase = phase
        self.options = options

    def __enter__(self):
        if self.phase is self.Phase.APPLY:
            self.start()
        return self

    def __exit__(self, _t, exc, _tb):
        if exc:
            self.finalize_fast()
            return

        if self.phase is self.Phase.CLONE:
            self.start()
        self.finalize()

    def start(self):
        status = self.mlink.status()
        if status["state"] == MLink.State.FINALIZING:
            self.wait_for_state(MLink.State.FINALIZED)
        elif status["state"] == MLink.State.RUNNING:
            self.wait_for_finalizable()
            self.mlink.finalize()
            self.wait_for_state(MLink.State.FINALIZED)

        self.mlink.start(self.options)
        self.wait_for_state(MLink.State.RUNNING)
        return self

    def finalize_fast(self):
        status = self.mlink.status()
        if status["state"] == MLink.State.RUNNING:
            self.wait_for_finalizable()
            self.mlink.finalize()

    def finalize(self):
        status = self.mlink.status()
        if status["state"] == MLink.State.FINALIZING:
            self.wait_for_state(MLink.State.FINALIZED)
        elif status["state"] == MLink.State.RUNNING:
            optime = self.source.server_info()["operationTime"]
            self.wait_for_optime(optime)
            self.wait_for_finalizable()
            self.mlink.finalize()
            self.wait_for_state(MLink.State.FINALIZED)

    def wait_for_state(self, state):
        status = self.mlink.status()
        while status["state"] != state:
            time.sleep(0.1)
            status = self.mlink.status()

    def wait_for_optime(self, ts: bson.Timestamp, timeout=10):
        status = self.mlink.status()
        assert status["state"] == MLink.State.RUNNING

        for _ in range(timeout * 10):
            applied_optime: str = status.get("lastAppliedOpTime")
            if applied_optime:
                t_s, i_s = applied_optime.split(".")
                if ts <= bson.Timestamp(int(t_s), int(i_s)):
                    return

            time.sleep(0.1)
            status = self.mlink.status()

        raise WaitTimeoutError()

    def wait_for_finalizable(self, timeout=10):
        status = self.mlink.status()
        assert status["state"] == MLink.State.RUNNING

        for _ in range(timeout * 10):
            if status.get("finalizable"):
                return

            time.sleep(0.1)
            status = self.mlink.status()

        raise WaitTimeoutError()

    def last_applied_op(self):
        status = self.mlink.status()
        if applied_optime := status.get("lastAppliedOpTime"):
            t_s, i_s = applied_optime.split(".")
            return bson.Timestamp(int(t_s), int(i_s))

        return bson.Timestamp(0, 0)
