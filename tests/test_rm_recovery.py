"""Exercise replica recovery through RM's TCP control protocol."""

import json
import os
import unittest
from unittest.mock import patch

import test_rm


class RecoveryTests(unittest.TestCase):
    stop = test_rm.ReplicationManagerTests.stop
    logs = test_rm.ReplicationManagerTests.logs
    wait_for = test_rm.ReplicationManagerTests.wait_for
    connect = test_rm.ReplicationManagerTests.connect
    send = staticmethod(test_rm.ReplicationManagerTests.send)
    def setUp(self):
        with patch.dict(os.environ, REPLICATION_MODE="active", PRIMARY_ID="S1"):
            test_rm.ReplicationManagerTests.setUp(self)
        self.gfd = self.connect()
        self.send(self.gfd, {"type": "membership", "members": ["S1", "S2", "S3"],
                             "member_count": 3, "primary_id": "S1"})
        self.wait_for("RM: 3 members")

    def receive(self, sock):
        data = bytearray()
        while not data.endswith(b"\n"):
            chunk = sock.recv(1)
            self.assertTrue(chunk, "RM closed control connection")
            data.extend(chunk)
        message = json.loads(data)
        if message.get("type") == "membership":
            return self.receive(sock)
        return message

    def join(self, replica, bootstrap=False):
        sock = self.connect(register=False)
        self.send(sock, {"type": "join_request", "replica_id": replica, "bootstrap": bootstrap})
        self.assertEqual(self.receive(sock)["type"], "join_accepted")
        return sock

    def complete(self, sock, replica, instruction):
        self.send(sock, {"type": "sync_complete", "replica_id": replica,
                         "recovery_id": instruction["recovery_id"]})
        return self.receive(sock)

    def seed(self):
        sock = self.join("S1", bootstrap=True)
        instruction = self.receive(sock)
        self.assertIsNone(instruction["source_id"])
        self.assertEqual(self.complete(sock, "S1", instruction)["type"], "ready")
        return sock

    def test_join_transfer_ready_and_duplicate_completion(self):
        self.seed()
        sock = self.join("S2")
        instruction = self.receive(sock)
        self.assertEqual(instruction["source_id"], "S1")
        self.assertIn("port", instruction["source_address"])
        self.assertEqual(self.complete(sock, "S2", instruction)["type"], "ready")
        self.assertEqual(self.complete(sock, "S2", instruction)["type"], "ready")
        self.send(sock, {"type": "join_request", "replica_id": "S2"})
        self.assertEqual(self.receive(sock)["type"], "join_accepted")
        self.assertEqual(self.receive(sock)["type"], "ready")

        self.assertEqual(self.logs().count("RM: S2 synchronized and ready"), 1)

    def test_waits_for_ready_donor(self):
        waiting = self.join("S2")
        self.send(waiting, {"type": "sync_complete", "replica_id": "S2", "recovery_id": "fake"})
        self.assertEqual(self.receive(waiting)["type"], "recovery_rejected")
        self.seed()
        self.assertEqual(self.receive(waiting)["source_id"], "S1")

    def test_restart_rejects_old_completion(self):
        self.seed()
        old = self.join("S2")
        previous = self.receive(old)
        new = self.join("S2")
        current = self.receive(new)
        self.assertNotEqual(previous["recovery_id"], current["recovery_id"])
        self.assertEqual(self.complete(new, "S2", previous)["type"], "recovery_rejected")
        self.assertEqual(self.complete(new, "S2", current)["type"], "ready")

    def test_donor_failure_cancels_then_reassigns(self):
        self.seed()
        second = self.join("S2")
        self.complete(second, "S2", self.receive(second))
        third = self.join("S3")
        previous = self.receive(third)
        self.send(self.gfd, {"type": "delete_replica", "replica_id": "S1"})
        self.assertEqual(self.receive(third)["type"], "recovery_cancelled")
        current = self.receive(third)
        self.assertEqual(current["source_id"], "S2")
        self.assertEqual(self.complete(third, "S3", previous)["type"], "recovery_rejected")

    def test_passive_recovery_waits_for_primary_update(self):
        # Configure this subprocess separately so donor selection uses passive mode.
        self.stop()
        with patch.dict(os.environ, REPLICATION_MODE="passive", PRIMARY_ID="S1"):
            test_rm.ReplicationManagerTests.setUp(self)
        self.gfd = self.connect()
        self.send(self.gfd, {"type": "membership", "members": ["S1", "S2", "S3"],
                             "member_count": 3, "primary_id": "S1"})
        self.wait_for("RM: 3 members")
        self.seed()
        second = self.join("S2")
        self.complete(second, "S2", self.receive(second))
        third = self.join("S3")
        previous = self.receive(third)
        self.send(self.gfd, {"type": "delete_replica", "replica_id": "S1"})
        self.assertEqual(self.receive(third)["type"], "recovery_cancelled")
        self.send(self.gfd, {"type": "membership", "members": ["S2", "S3"],
                             "member_count": 2, "primary_id": "S2"})
        current = self.receive(third)
        self.assertEqual(current["source_id"], "S2")
        self.assertNotEqual(previous["recovery_id"], current["recovery_id"])

    def test_gfd_disconnect_requires_snapshot_and_new_attempt(self):
        self.seed()
        sock = self.join("S2")
        previous = self.receive(sock)
        self.gfd.close()
        self.assertEqual(self.receive(sock)["type"], "recovery_cancelled")
        self.assertEqual(self.complete(sock, "S2", previous)["type"], "recovery_rejected")
        self.gfd = self.connect()
        self.send(self.gfd, {"type": "membership", "members": ["S1", "S2"],
                             "member_count": 2, "primary_id": "S1"})
        current = self.receive(sock)
        self.assertNotEqual(previous["recovery_id"], current["recovery_id"])
        self.assertEqual(self.complete(sock, "S2", current)["type"], "ready")

    def test_bootstrap_cannot_bypass_recovery_while_another_replica_is_ready(self):
        seed = self.seed()
        backup = self.join("S2")
        self.assertEqual(self.complete(backup, "S2", self.receive(backup))["type"], "ready")
        seed.close()
        self.send(self.gfd, {"type": "delete_replica", "replica_id": "S1"})
        self.send(self.gfd, {"type": "add_replica", "replica_id": "S1"})
        restarted = self.join("S1", bootstrap=True)
        self.assertEqual(self.receive(restarted)["source_id"], "S2")

    def test_bootstrap_again_after_every_replica_died(self):
        seed = self.seed()
        seed.close()
        self.send(self.gfd, {"type": "delete_replica", "replica_id": "S1"})
        waiting = self.join("S2")
        self.wait_for("RM: S2 waiting for a ready donor (start S1 with --bootstrap)")
        self.send(self.gfd, {"type": "add_replica", "replica_id": "S1"})
        restarted = self.join("S1", bootstrap=True)
        instruction = self.receive(restarted)
        self.assertEqual((instruction["type"], instruction["source_id"]), ("initialize", None))
        self.assertEqual(self.complete(restarted, "S1", instruction)["type"], "ready")
        self.assertEqual(self.receive(waiting)["source_id"], "S1")

    def test_removed_replica_cannot_complete_old_attempt(self):
        self.seed()
        sock = self.join("S2")
        previous = self.receive(sock)
        self.send(self.gfd, {"type": "delete_replica", "replica_id": "S2"})
        self.assertEqual(self.receive(sock)["type"], "recovery_cancelled")
        self.assertEqual(self.complete(sock, "S2", previous)["type"], "recovery_rejected")
