# pylint: disable=missing-docstring,redefined-outer-name
"""Harness for the multi-instance HA E2E tests in test_ha.py.

Spawns several PCSM server subprocesses competing for one lease, with helpers
to find the ACTIVE, hard-kill it, and wait for a new single ACTIVE to settle.
"""

import os
import signal
import subprocess
import tempfile
import time

from pcsm import PCSM

# Failover budget: lease TTL (10s) plus margin for the new active to renew,
# recover, and publish its role.
FAILOVER_TIMEOUT = 30.0

# Per-instance log directory (for post-mortem when a test fails).
LOG_DIR = os.path.join(tempfile.gettempdir(), "pcsm-ha-logs")


class PCSMInstance:
    """A single PCSM server subprocess and its HTTP client."""

    def __init__(self, pcsm_bin: str, source: str, target: str, port: int):
        self._bin = pcsm_bin
        self._source = source
        self._target = target
        self.port = port
        self.url = f"http://localhost:{port}"
        self.client = PCSM(self.url)
        self.proc: subprocess.Popen | None = None
        self._log = None

    def start(self):
        """Start the server process, logging to LOG_DIR for post-mortem."""
        os.makedirs(LOG_DIR, exist_ok=True)
        self._log = open(  # noqa: SIM115 - closed in _close_log
            os.path.join(LOG_DIR, f"pcsm-{self.port}.log"), "a", encoding="utf-8"
        )
        # Checkpoint often so an in-flight clone is captured quickly; the
        # default 15s is longer than a fast local clone.
        env = {**os.environ, "PCSM_RECOVERY_CHECKPOINT_INTERVAL": "1s"}
        self.proc = subprocess.Popen(
            [
                self._bin,
                "--source",
                self._source,
                "--target",
                self._target,
                "--port",
                str(self.port),
                "--log-level=debug",
            ],
            stdout=self._log,
            stderr=subprocess.STDOUT,
            env=env,
        )
        return self

    def _close_log(self):
        if self._log is not None:
            self._log.close()
            self._log = None

    def kill(self):
        """Hard-kill (SIGKILL) the process to simulate a crash. No lease release."""
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait()
        self._close_log()

    def terminate(self):
        """Gracefully stop the process (releases the lease if ACTIVE)."""
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            self.proc.wait()
        self._close_log()

    @property
    def is_alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def try_status(self):
        """Return the /status body (even a STANDBY's 409 body, which carries
        role and members), or None if unreachable/not ready."""
        try:
            _, body = self.client.raw_status()
        except Exception:  # noqa: BLE001 - any failure during startup is retryable
            return None

        return body


class PCSMCluster:
    """A group of PCSM instances sharing one source/target and one lease."""

    def __init__(self, pcsm_bin: str, source: str, target: str, ports: list[int]):
        self._bin = pcsm_bin
        self._source = source
        self._target = target
        self.instances = [PCSMInstance(pcsm_bin, source, target, p) for p in ports]

    def reset_state(self):
        """Clear target-side HA + recovery state before starting the group."""
        subprocess.run(
            [self._bin, "reset", "--target", self._target],
            check=True,
            timeout=30,
        )

    def start(self):
        """Reset shared state, then start every instance."""
        self.reset_state()
        for inst in self.instances:
            inst.start()
        self._verify_ha_ready()
        self.wait_for_single_active()
        return self

    def _verify_ha_ready(self, timeout: float = 15.0):
        """Fail fast unless every instance answers /status with a `role` field:
        no `role` means a pre-HA TEST_PCSM_BIN; no answer means the instance
        died on startup (see LOG_DIR)."""
        deadline = time.monotonic() + timeout
        pending = list(self.instances)
        while pending and time.monotonic() < deadline:
            still = []
            for inst in pending:
                if not inst.is_alive:
                    raise AssertionError(
                        f"instance on port {inst.port} exited on startup; "
                        f"see {LOG_DIR}/pcsm-{inst.port}.log"
                    )
                payload = inst.try_status()
                if payload is None:
                    still.append(inst)
                    continue
                if "role" not in payload:
                    raise AssertionError(
                        "PCSM binary has no HA support (no 'role' in /status); "
                        "check TEST_PCSM_BIN version"
                    )
            pending = still
            if pending:
                time.sleep(0.3)
        if pending:
            ports = ", ".join(str(i.port) for i in pending)
            raise AssertionError(f"instances not ready within {timeout}s: ports {ports}")

    def stop(self):
        """Terminate every instance (best effort)."""
        for inst in self.instances:
            try:
                inst.terminate()
            except Exception:  # noqa: BLE001 - best-effort teardown
                pass

    def alive_instances(self) -> list[PCSMInstance]:
        return [i for i in self.instances if i.is_alive]

    def active(self, timeout: float = FAILOVER_TIMEOUT) -> PCSMInstance:
        """Return the single ACTIVE instance, waiting until exactly one settles."""
        self.wait_for_single_active(timeout)
        for inst in self.alive_instances():
            payload = inst.try_status()
            if payload and payload.get("role") == "ACTIVE":
                return inst
        raise AssertionError("no ACTIVE instance found")

    def standbys(self) -> list[PCSMInstance]:
        result = []
        for inst in self.alive_instances():
            payload = inst.try_status()
            if payload and payload.get("role") == "STANDBY":
                result.append(inst)
        return result

    def wait_for_single_active(self, timeout: float = FAILOVER_TIMEOUT):
        """Block until exactly one live instance reports role ACTIVE."""
        deadline = time.monotonic() + timeout
        last = "no response"
        while time.monotonic() < deadline:
            actives = 0
            seen = 0
            for inst in self.alive_instances():
                payload = inst.try_status()
                if not payload:
                    continue
                seen += 1
                if payload.get("role") == "ACTIVE":
                    actives += 1
            if seen == len(self.alive_instances()) and actives == 1:
                return
            last = f"actives={actives} seen={seen}/{len(self.alive_instances())}"
            time.sleep(0.5)
        raise TimeoutError(f"no single ACTIVE within {timeout}s: {last}")

    def kill_active(self) -> PCSMInstance:
        """Hard-kill the current ACTIVE and return it (for later restart)."""
        active = self.active()
        active.kill()
        return active
