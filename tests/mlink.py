# pylint: disable=missing-docstring,redefined-outer-name
import time
from dataclasses import dataclass
from enum import StrEnum

import bson
import requests
from pymongo import MongoClient


class MLink:
    class State(StrEnum):
        IDLE = "idle"
        RUNNING = "running"
        FINALIZING = "finalizing"
        FINALIZED = "finalized"

    def __init__(self, uri: str):
        self.uri = uri

    def status(self):
        res = requests.get(f"{self.uri}/status", timeout=5)
        res.raise_for_status()
        return res.json()

    def start(self, params):
        res = requests.post(f"{self.uri}/start", json=params, timeout=5)
        res.raise_for_status()
        return res.json()

    def finalize(self):
        res = requests.post(f"{self.uri}/finalize", timeout=5)
        res.raise_for_status()
        return res.json()


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
            self.mlink.finalize()
            self.wait_for_state(MLink.State.FINALIZED)

        self.mlink.start(self.options)
        self.wait_for_state(MLink.State.RUNNING)
        return self

    def finalize_fast(self):
        status = self.mlink.status()
        if status["state"] == MLink.State.RUNNING:
            self.mlink.finalize()

    def finalize(self):
        status = self.mlink.status()
        if status["state"] == MLink.State.FINALIZING:
            self.wait_for_state(MLink.State.FINALIZED)
        elif status["state"] == MLink.State.RUNNING:
            optime = self.source.server_info()["operationTime"]
            self.wait_for_optime(optime)
            self.mlink.finalize()
            self.wait_for_state(MLink.State.FINALIZED)

    def wait_for_state(self, state):
        status = self.mlink.status()
        while status["state"] != state:
            time.sleep(0.2)
            status = self.mlink.status()

    def wait_for_optime(self, ts: bson.Timestamp):
        status = self.mlink.status()
        assert status["state"] == MLink.State.RUNNING

        for _ in range(10):  # ~2 secs timeout
            applied_optime: str = status.get("lastAppliedOpTime")
            if applied_optime:
                t_s, i_s = applied_optime.split(".")
                if ts <= bson.Timestamp(int(t_s), int(i_s)):
                    break

            time.sleep(0.2)
            status = self.mlink.status()

    def wait_for_clone_done(self):
        status = self.mlink.status()
        assert status["state"] == MLink.State.RUNNING

        for _ in range(10):  # ~2 secs timeout
            if status.get("lastAppliedOpTime"):
                break

            time.sleep(0.2)
            status = self.mlink.status()
