"""Run real RM, GFD, LFD, and server processes through admission and restart."""

import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]


class AdmissionIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.processes = []
        self.logs = {}
        self.env = {**os.environ, "PYTHONPATH": str(ROOT / "src"), "PRIMARY_ID": "S1"}
        self.ports = {}
        reservations = []
        for name in ("RM", "GFD", "LFD1", "LFD2", "LFD3", "S1", "S2", "S3"):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            reservations.append(sock)
            self.ports[name] = sock.getsockname()[1]
            self.env[name + "_HOST"] = "127.0.0.1"
            self.env[name + "_PORT"] = str(self.ports[name])
        for sock in reservations:
            sock.close()
        self.addCleanup(self.cleanup)

    def cleanup(self):
        for process in reversed(self.processes):
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
        for output in self.logs.values():
            output.close()

    def output(self, name):
        stream = self.logs[name]
        return os.pread(stream.fileno(), os.fstat(stream.fileno()).st_size, 0).decode()

    def wait_for(self, name, text, count=1):
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            if self.output(name).count(text) >= count:
                return
            time.sleep(0.03)
        self.fail(f"Missing {text!r} in {name}:\n" + "\n".join(
            f"{key}:\n{self.output(key)}" for key in self.logs))

    def start(self, name, module, *args):
        if name not in self.logs:
            self.logs[name] = tempfile.TemporaryFile()
        process = subprocess.Popen([sys.executable, "-m", module, *args], cwd=ROOT,
                                   env=self.env, stdout=self.logs[name], stderr=subprocess.STDOUT)
        self.processes.append(process)
        return process

    def start_group(self, mode):
        self.env["REPLICATION_MODE"] = mode
        self.start("RM", "distributed_system.rm.rm")
        self.wait_for("RM", "RM listening")
        self.start("GFD", "distributed_system.gfd.gfd", "--heartbeat_freq", "10", "--timeout", "1")
        self.wait_for("RM", "GFD registered")
        for name in ("LFD1", "LFD2"):
            self.start(name, "distributed_system.lfd.lfd", "--id", name,
                       "--heartbeat_freq", "10", "--timeout", "1")
        self.start("S1", "distributed_system.server.server", "--id", "S1", "--bootstrap",
                   "--checkpoint-freq", "10")
        self.wait_for("S1", "RM confirmed readiness")
        second = self.start("S2", "distributed_system.server.server", "--id", "S2")
        self.wait_for("S2", "RM confirmed readiness")
        return second

    def request(self, replica, number):
        with socket.create_connection(("127.0.0.1", self.ports[replica]), timeout=2) as sock:
            sock.sendall((json.dumps({"type": "request", "client_id": "C1",
                                     "request_num": number}) + "\n").encode())
            with sock.makefile("rb") as stream:
                return json.loads(stream.readline())["state"]

    def restart_backup(self, mode):
        second = self.start_group(mode)
        self.assertEqual(self.request("S1", 1), 1)
        if mode == "active":
            self.assertEqual(self.request("S2", 1), 1)
        second.kill()
        second.wait(timeout=3)
        self.wait_for("RM", "RM: 1 member: S1")
        self.assertEqual(self.request("S1", 2), 2)
        self.start("S2", "distributed_system.server.server", "--id", "S2")
        self.wait_for("S2", "RM confirmed readiness", count=2)
        self.wait_for("RM", "RM: S2 synchronized and ready", count=2)
        if mode == "active":
            # Keep traffic quiescent during the snapshot; catch-up is separate work.
            self.assertEqual(self.request("S2", 3), 3)
            self.assertEqual(self.request("S1", 3), 3)
        else:
            self.wait_for("S2", "my_state = 0 -> 2")
            self.assertEqual(self.request("S1", 3), 3)
            self.wait_for("S2", "my_state = 2 -> 3")

    def test_active_restart_with_quiescent_transfer(self):
        self.restart_backup("active")

    def test_passive_backup_restart(self):
        self.restart_backup("passive")
