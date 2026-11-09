"""Checkpoint ordering, live request buffering, and donor channel recovery."""

import selectors
import socket
import unittest
from unittest.mock import patch

from distributed_system.common import recv_json, send_json
from distributed_system.config import ReplicaStatus
from distributed_system.server.server import Server


class ActiveRecoveryTests(unittest.TestCase):
    def server(self, name):
        server = Server(name, replication_mode="active")
        self.addCleanup(server.sel.close)
        return server

    def channel(self, server, kind="client"):
        local, remote = socket.socketpair()
        self.addCleanup(local.close)
        self.addCleanup(remote.close)
        remote.settimeout(1)
        server._register(local, kind)
        return local, remote

    def recovery(self):
        target, donor = self.server("S2"), self.server("S1")
        donor.replica_status = ReplicaStatus.SYNCED
        local, remote = self.channel(target, "active_peer")
        donor._register(remote, "backup")
        donor._backups[remote] = "S2"
        target._active_peers[local] = "S1"
        target.handle_rm({"type": "recover", "source_id": "S1", "recovery_id": "transfer"})
        target.try_connect_to_recovery_source()
        return target, donor, local, remote

    def transfer(self, target, donor, local, remote):
        target._write_pending(local)
        donor._read_messages(remote, "backup")
        donor._write_pending(remote)
        target._read_messages(local, "active_peer")

    def test_live_requests_across_snapshot_and_ready_are_applied_once(self):
        target, donor, local, remote = self.recovery()
        client, replies = self.channel(target)
        request = {"type": "request", "client_id": "C1", "request_num": 1}
        donor.process_request(None, request)
        target.handle_client(client, request)
        self.transfer(target, donor, local, remote)
        self.assertEqual(target.state, 1)
        self.assertEqual(target.replica_status, ReplicaStatus.UP)
        self.assertFalse(target._connections[client].has_pending_output)
        # This arrives after the checkpoint but before the RM acknowledgment.
        donor.process_request(None, {**request, "request_num": 2})
        donor._write_pending(remote)
        target._read_messages(local, "active_peer")
        self.assertEqual(target.state, 1)
        self.assertEqual(target.high_watermark_request_num, {"C1": 2})
        target.handle_rm({"type": "ready", "recovery_id": "transfer"})
        self.assertEqual(target.state, 2)
        self.assertEqual(target.request_history, donor.request_history)
        target._write_pending(client)
        self.assertEqual(recv_json(replies)["state"], 1)
        self.assertIn(local, target._connections)
        target.handle_client(client, request)
        self.assertEqual(target.state, 2)

    def test_snapshot_does_not_drop_requests_donor_has_not_seen(self):
        target, donor, local, remote = self.recovery()
        client, _ = self.channel(target)
        target.handle_client(client, {"type": "request", "client_id": "C3", "request_num": 8})
        self.transfer(target, donor, local, remote)
        target.handle_rm({"type": "ready", "recovery_id": "transfer"})
        self.assertEqual(target.state, 1)
        self.assertEqual(target.high_watermark_request_num["C3"], 8)

    def test_stale_snapshot_cannot_replace_state(self):
        target, _, local, remote = self.recovery()
        send_json(remote, {"type": "recovery_snapshot", "replica_id": "S1",
                           "target_id": "S2", "recovery_id": "old", "my_state": 99,
                           "request_history": {}})
        target._handle_socket_event(local, "active_peer", selectors.EVENT_READ)
        self.assertEqual(target.state, 0)
        self.assertIsNone(target._recovery)
        self.assertEqual(target.replica_status, ReplicaStatus.UP)

    def test_timeout_waits_for_reopened_donor_channel(self):
        target, _, local, _ = self.recovery()
        target._recovery_deadline = 10
        with patch("distributed_system.server.server.time.monotonic", return_value=10):
            target.try_connect_to_recovery_source()
        self.assertNotIn(local, target._connections)
        self.assertEqual(target.replica_status, ReplicaStatus.UP)
        reopened, _ = self.channel(target, "active_peer")
        target._active_peers[reopened] = "S1"
        target.try_connect_to_recovery_source()
        self.assertIs(target._recovery, reopened)

    def test_unavailable_donor_does_not_trigger_a_reverse_connection(self):
        target = self.server("S2")
        target.prepare_recovery("S1")
        with patch("distributed_system.server.server.socket.create_connection") as connect:
            target.try_connect_to_recovery_source()
        connect.assert_not_called()
        self.assertEqual(target.replica_status, ReplicaStatus.UP)

    def test_unsynchronized_donor_cannot_send_snapshot(self):
        donor = self.server("S1")
        local, _ = self.channel(donor, "backup")
        with self.assertRaises(ValueError):
            donor.handle_recovery_request(local, {"type": "recovery_request",
                                                  "replica_id": "S2", "source_id": "S1"})
        self.assertFalse(donor._connections[local].has_pending_output)

    def test_donor_reassignment_discards_old_snapshot_but_preserves_requests(self):
        target, _, local, _ = self.recovery()
        client, _ = self.channel(target)
        target.handle_client(client, {"type": "request", "client_id": "C1", "request_num": 1})
        target.handle_rm({"type": "recovery_cancelled", "recovery_id": "transfer"})
        self.assertNotIn(local, target._connections)
        target.handle_rm({"type": "recover", "source_id": "S3", "recovery_id": "replacement"})
        new_channel, _ = self.channel(target, "active_peer")
        target._active_peers[new_channel] = "S3"
        target.try_connect_to_recovery_source()
        target.handle_recovery_snapshot(new_channel, {
            "type": "recovery_snapshot", "replica_id": "S3", "target_id": "S2",
            "recovery_id": "replacement", "my_state": 0, "request_history": {}})
        target.handle_rm({"type": "ready", "recovery_id": "transfer"})
        self.assertEqual(target.replica_status, ReplicaStatus.UP)
        self.assertEqual(target.state, 0)
        target.handle_rm({"type": "ready", "recovery_id": "replacement"})
        self.assertEqual(target.state, 1)

    def test_out_of_order_request_below_watermark_is_not_discarded(self):
        server = self.server("S1")
        server.replica_status = ReplicaStatus.SYNCED
        for number in (3, 1, 2, 3, 1):
            server.process_request(None, {"client_id": "C1", "request_num": number})
        self.assertEqual(server.state, 3)
        self.assertEqual(server.high_watermark_request_num, {"C1": 3})
        self.assertEqual(set(server.request_history["C1"]), {1, 2, 3})
