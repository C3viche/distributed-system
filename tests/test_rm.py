"""Exercise the RM receiver over TCP without depending on a GFD sender."""

import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest


ROOT = Path(__file__).resolve().parents[1]


class ReplicationManagerTests(unittest.TestCase):
    def setUp(self):
        self.output = tempfile.TemporaryFile()
        self.addCleanup(self.output.close)
        self.process = subprocess.Popen(
            [sys.executable, "-m", "distributed_system.rm.rm", "--host", "127.0.0.1",
             "--port", "0"], cwd=ROOT,
            env={**os.environ, "PYTHONPATH": str(ROOT / "src")},
            stdout=self.output, stderr=subprocess.STDOUT,
        )
        self.addCleanup(self.stop)
        self.wait_for("RM: 0 members")
        self.port = int(re.search(r"RM listening on 127\.0\.0\.1:(\d+)", self.logs())[1])

    def stop(self):
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
            try:
                self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=3)

    def logs(self):
        return os.pread(self.output.fileno(), os.fstat(self.output.fileno()).st_size, 0).decode()

    def wait_for(self, text, count=1):
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline:
            if self.logs().count(text) >= count:
                return
            if self.process.poll() is not None:
                break
            time.sleep(0.02)
        self.fail(f"Missing {text!r}:\n{self.logs()}")

    def connect(self, register=True):
        sock = socket.create_connection(("127.0.0.1", self.port), timeout=2)
        self.addCleanup(sock.close)
        if register:
            self.send(sock, {"type": "register_gfd", "gfd_id": "GFD"})
        return sock

    @staticmethod
    def send(sock, message):
        sock.sendall((json.dumps(message) + "\n").encode())

    def test_initial_empty_and_unchanged_snapshots_are_printed(self):
        sock = self.connect()
        self.wait_for("GFD registered with RM")
        self.assertEqual(self.logs().count("RM: 0 members"), 1)
        self.send(sock, {"type": "membership", "members": [], "member_count": 0})
        self.wait_for("RM: 0 members", count=2)
        snapshot = {"type": "membership", "members": ["S1"], "member_count": 1}
        self.send(sock, snapshot)
        self.wait_for("RM: 1 member: S1")
        self.send(sock, snapshot)
        self.wait_for("RM: 1 member: S1", count=2)
        self.assertNotIn("RM: 1 members:", self.logs())

    def test_membership_is_idempotent_and_framing_handles_partial_lines(self):
        sock = self.connect()
        sock.sendall(b'{"type":"add_replica","replica_')
        sock.sendall(b'id":"S1"}\n{"type":"add_replica","replica_id":"S1"}\n')
        for replica in ("S2", "S3"):
            self.send(sock, {"type": "add_replica", "replica_id": replica})
        self.wait_for("RM: 3 members: S1, S2, S3")
        for _ in range(2):
            self.send(sock, {"type": "delete_replica", "replica_id": "S1"})
        self.send(sock, {"type": "delete_replica", "replica_id": "S2"})
        self.wait_for("RM: 1 member: S3")
        self.assertEqual(self.logs().count("RM: 1 member: S1"), 1)
        self.assertEqual(self.logs().count("RM: 2 members: S2, S3"), 1)

    def test_reconnect_resynchronizes_without_treating_disconnect_as_crash(self):
        sock = self.connect()
        self.send(sock, {"type": "add_replica", "replica_id": "S1"})
        self.wait_for("RM: 1 member: S1")
        sock.close()
        self.wait_for("retaining last reported membership")
        self.assertEqual(self.logs().count("RM: 0 members"), 1)
        replacement = self.connect()
        self.send(replacement, {"type": "membership", "members": ["S2", "S3"],
                                "member_count": 2})
        self.wait_for("RM: 2 members: S2, S3")
        self.send(replacement, {"type": "membership", "members": [], "member_count": 0})
        self.wait_for("RM: 0 members", count=2)

    def test_registration_required_and_invalid_messages_do_not_crash_rm(self):
        invalid = [
            [], {"type": []}, {"type": "register_gfd", "gfd_id": "LFD1"},
            {"type": "add_replica", "replica_id": "S1"},
        ]
        for index, message in enumerate(invalid, 1):
            self.send(self.connect(register=False), message)
            self.wait_for("RM rejected/disconnected peer", count=index)
        self.send(self.connect(), {"type": "add_replica", "replica_id": "S2"})
        self.wait_for("RM: 1 member: S2")

    def test_invalid_snapshot_does_not_partially_replace_membership(self):
        sock = self.connect()
        self.send(sock, {"type": "add_replica", "replica_id": "S1"})
        self.wait_for("RM: 1 member: S1")
        for index, (members, count) in enumerate([
            (["S2", "S9"], 2), (["S2", "S2"], 2), (["S2"], 2), (["S2"], True),
        ], 1):
            self.send(self.connect(), {"type": "membership", "members": members,
                                       "member_count": count})
            self.wait_for("RM rejected/disconnected peer", count=index)
        self.send(self.connect(), {"type": "add_replica", "replica_id": "S3"})
        self.wait_for("RM: 2 members: S1, S3")

    def test_new_registration_replaces_old_connection(self):
        old = self.connect()
        self.wait_for("GFD registered with RM")
        new = self.connect()
        self.wait_for("GFD registered with RM", count=2)
        self.assertEqual(old.recv(1), b"")
        self.send(new, {"type": "add_replica", "replica_id": "S3"})
        self.wait_for("RM: 1 member: S3")


if __name__ == "__main__":
    unittest.main()
