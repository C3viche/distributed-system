"""Exercise admission and readiness gating over the server control channel."""

# These tests deliberately inspect selector state and mock connection attempts.
# pyright: reportPrivateUsage=false, reportAny=false

import socket
import unittest
from unittest.mock import MagicMock, patch

from distributed_system.common import recv_json, send_json
from distributed_system.config import ReplicaStatus
from distributed_system.server.server import Server


class ServerRMTests(unittest.TestCase):
    def acknowledge(self, server):
        self.assertEqual(server.replica_status, ReplicaStatus.UP)
        server.handle_rm({"type": "ready", "recovery_id": server._recovery_id})

    def initialize(self, server):
        server.handle_rm({"type": "initialize", "recovery_id": "initial"})
        self.acknowledge(server)

    def make_server(self, replica_id: str = "S1", mode: str = "active") -> Server:
        server = Server(replica_id, replication_mode=mode, primary_id="S1")
        self.addCleanup(server.sel.close)
        return server

    def test_join_request_and_async_initialize(self) -> None:
        server = self.make_server()
        replica, manager = socket.socketpair()
        self.addCleanup(replica.close)
        self.addCleanup(manager.close)
        manager.settimeout(1.0)
        with patch(
            "distributed_system.server.server.socket.create_connection",
            return_value=replica,
        ):
            server.try_connect_to_rm()
        server._write_pending(replica)
        self.assertEqual(
            recv_json(manager),
            {
                "type": "join_request",
                "replica_id": "S1",
                "replication_mode": "active",
                "status": "UP",
                "bootstrap": False,
            },
        )
        self.assertEqual(server.replica_status, ReplicaStatus.UP)
        send_json(manager, {"type": "initialize", "recovery_id": "initial"})
        server._read_messages(replica, "rm")
        self.acknowledge(server)
        self.assertEqual(server.replica_status, ReplicaStatus.SYNCED)

        manager.close()
        with patch(
            "distributed_system.server.server.time.monotonic", return_value=10.0
        ):
            server._read_messages(replica, "rm")
        self.assertIsNone(server._rm)
        self.assertEqual(server._rm_retry_at, 11.0)
        self.assertEqual(server.replica_status, ReplicaStatus.UP)

    def test_failed_connection_retries_after_backoff(self) -> None:
        server = self.make_server()
        with (
            patch("distributed_system.server.server.time.monotonic") as now,
            patch(
                "distributed_system.server.server.socket.create_connection",
                side_effect=OSError("offline"),
            ) as connect,
        ):
            now.return_value = 10.0
            server.try_connect_to_rm()
            now.return_value = 10.5
            server.try_connect_to_rm()
            self.assertEqual(connect.call_count, 1)
            server._lfd = MagicMock(spec=socket.socket)
            self.assertEqual(server._seconds_until_next_wakeup(), 0.5)
            now.return_value = 11.0
            server.try_connect_to_rm()
            self.assertEqual(connect.call_count, 2)
        self.assertEqual(server.replica_status, ReplicaStatus.UP)

    def test_recover_and_wait_do_not_authorize_serving(self) -> None:
        server = self.make_server()
        server.handle_rm({"type": "recover", "recovery_id": "recovery", "source_id": "S2"})
        self.assertEqual(server.recovery_source_id, "S2")
        self.assertEqual(server.replica_status, ReplicaStatus.UP)
        server.handle_rm({"type": "wait"})
        self.assertIsNone(server.recovery_source_id)
        self.assertEqual(server.replica_status, ReplicaStatus.UP)

    def test_invalid_instruction_preserves_status(self) -> None:
        server = self.make_server()
        messages: list[dict[str, object]] = [
            {"type": "recover", "recovery_id": "recovery", "source_id": "S1"},
            {"type": "recover", "recovery_id": "recovery", "source_id": "unknown"},
            {"type": "recover"},
            {"type": "membership", "members": []},
        ]
        for message in messages:
            with self.subTest(message=message), self.assertRaises(ValueError):
                server.handle_rm(message)
            self.assertEqual(server.replica_status, ReplicaStatus.UP)
            self.assertIsNone(server.recovery_source_id)

    def test_active_replica_serves_after_explicit_recovery_completion(self) -> None:
        server = self.make_server()
        server.handle_rm({"type": "recover", "recovery_id": "recovery", "source_id": "S2"})
        peer = MagicMock(spec=socket.socket)
        request: dict[str, object] = {
            "type": "request",
            "client_id": "C1",
            "request_num": 1,
        }
        with patch.object(server, "_send_json") as send:
            server.handle_client(peer, request)
            send.assert_not_called()
            self.assertEqual(server.replica_status, ReplicaStatus.UP)

            # Simulate state installation by the future transfer implementation.
            server.state = 7
            server.complete_recovery()
            self.acknowledge(server)
            self.assertEqual(server.state, 7)
            self.assertEqual(server.replica_status, ReplicaStatus.SYNCED)
            self.assertIsNone(server.recovery_source_id)
            server.handle_client(peer, request)
            self.assertEqual(server.state, 8)
            send.assert_called_once_with(
                peer,
                {
                    "type": "reply",
                    "client_id": "C1",
                    "replica_id": "S1",
                    "request_num": 1,
                    "state": 8,
                },
            )

    def test_recovery_completion_requires_an_active_recovery(self) -> None:
        active = self.make_server()
        with self.assertRaises(ValueError):
            active.complete_recovery()
        self.assertEqual(active.replica_status, ReplicaStatus.UP)

        backup = self.make_server(replica_id="S2", mode="passive")
        backup.handle_rm({"type": "recover", "recovery_id": "recovery", "source_id": "S1"})
        with self.assertRaises(ValueError):
            backup.complete_recovery()
        self.assertEqual(backup.replica_status, ReplicaStatus.UP)
        self.assertEqual(backup.recovery_source_id, "S1")

    def test_unsynced_primary_does_not_publish_checkpoint(self) -> None:
        server = self.make_server(mode="passive")
        with patch.object(server, "_send_json") as send:
            server.send_checkpoint()
        send.assert_not_called()
        self.assertEqual(server.checkpoint_count, 1)

    def test_synced_backup_still_ignores_client_requests(self) -> None:
        server = self.make_server(replica_id="S2", mode="passive")
        server.handle_rm({"type": "recover", "source_id": "S1", "recovery_id": "checkpoint"})
        server.handle_checkpoint(
            {
                "type": "checkpoint",
                "replica_id": "S1",
                "my_state": 0,
                "checkpoint_count": 1,
            }
        )
        peer = MagicMock(spec=socket.socket)
        with patch.object(server, "_send_json") as send:
            server.handle_client(
                peer, {"type": "request", "client_id": "C1", "request_num": 1}
            )
        send.assert_not_called()
        self.assertEqual(server.state, 0)

    def test_active_replica_serves_only_after_initialization(self) -> None:
        server = self.make_server()
        peer = MagicMock(spec=socket.socket)
        request: dict[str, object] = {
            "type": "request",
            "client_id": "C1",
            "request_num": 1,
        }
        with patch.object(server, "_send_json") as send:
            server.handle_client(peer, request)
            send.assert_not_called()
            self.assertEqual(server.state, 0)

            self.initialize(server)
            server.handle_client(peer, request)
            self.assertEqual(server.state, 1)
            self.assertIsNone(server.recovery_source_id)
            send.assert_called_once_with(
                peer,
                {
                    "type": "reply",
                    "client_id": "C1",
                    "replica_id": "S1",
                    "request_num": 1,
                    "state": 1,
                },
            )

    def test_initialize_cannot_bypass_recovery_or_backup_checkpoint(self) -> None:
        active = self.make_server()
        active.handle_rm({"type": "recover", "recovery_id": "recovery", "source_id": "S2"})
        active.initialize_replica()
        self.assertEqual(active.replica_status, ReplicaStatus.UP)
        self.assertEqual(active.recovery_source_id, "S2")
        backup = self.make_server(replica_id="S2", mode="passive")
        backup.initialize_replica()
        self.assertEqual(backup.replica_status, ReplicaStatus.UP)

    def test_checkpoint_synchronizes_backup_without_authorizing_client_work(
        self,
    ) -> None:
        server = self.make_server(replica_id="S2", mode="passive")
        server.handle_rm({"type": "recover", "source_id": "S1", "recovery_id": "checkpoint"})
        server.handle_checkpoint(
            {
                "type": "checkpoint",
                "replica_id": "S1",
                "my_state": 7,
                "checkpoint_count": 3,
            }
        )
        self.acknowledge(server)
        self.assertEqual(server.replica_status, ReplicaStatus.SYNCED)
        self.assertEqual(server.state, 7)
        self.assertEqual(server.checkpoint_count, 3)
        with patch.object(server, "_send_json") as send:
            server.handle_client(
                MagicMock(spec=socket.socket),
                {
                    "type": "request",
                    "client_id": "C1",
                    "request_num": 1,
                },
            )
        send.assert_not_called()
        self.assertEqual(server.state, 7)

    def test_readiness_requires_current_completion_and_ack(self):
        server = self.make_server()
        server.handle_rm({"type": "initialize", "recovery_id": "current"})
        self.assertEqual(server.replica_status, ReplicaStatus.UP)
        with patch.object(server, "_send_json") as send:
            server.handle_client(MagicMock(spec=socket.socket),
                                 {"type": "request", "client_id": "C1", "request_num": 1})
            send.assert_not_called()
        for kind in ("ready", "recovery_rejected", "recovery_cancelled"):
            server.handle_rm({"type": kind, "recovery_id": "old"})
            self.assertEqual(server._recovery_id, "current")
            self.assertEqual(server.replica_status, ReplicaStatus.UP)
        server.handle_rm({"type": "ready", "recovery_id": "current"})
        self.assertEqual(server.replica_status, ReplicaStatus.SYNCED)
        server.handle_rm({"type": "recovery_cancelled", "recovery_id": "current"})
        self.assertEqual(server.replica_status, ReplicaStatus.UP)
        self.assertIsNone(server._recovery_id)

    def test_completion_is_sent_to_rm_with_recovery_token(self):
        server = self.make_server()
        replica, manager = socket.socketpair()
        self.addCleanup(replica.close)
        self.addCleanup(manager.close)
        manager.settimeout(1)
        server._rm = replica
        server._register(replica, "rm")
        server.handle_rm({"type": "initialize", "recovery_id": "initial"})
        server._write_pending(replica)
        self.assertEqual(recv_json(manager), {"type": "sync_complete",
                                             "replica_id": "S1", "recovery_id": "initial"})
        self.assertEqual(server.replica_status, ReplicaStatus.UP)
