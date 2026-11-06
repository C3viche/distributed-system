"""Exercise the proposed RM control channel without a running manager."""

# These tests deliberately inspect selector state and mock connection attempts.
# pyright: reportPrivateUsage=false, reportAny=false

import socket
import unittest
from unittest.mock import MagicMock, patch

from distributed_system.common import recv_json, send_json
from distributed_system.config import ReplicaStatus
from distributed_system.server.server import Server


class ServerRMTests(unittest.TestCase):
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
            },
        )
        self.assertEqual(server.replica_status, ReplicaStatus.UP)
        send_json(manager, {"type": "initialize"})
        server._read_messages(replica, "rm")
        self.assertEqual(server.replica_status, ReplicaStatus.SYNCED)

        manager.close()
        with patch(
            "distributed_system.server.server.time.monotonic", return_value=10.0
        ):
            server._read_messages(replica, "rm")
        self.assertIsNone(server._rm)
        self.assertEqual(server._rm_retry_at, 11.0)
        self.assertEqual(server.replica_status, ReplicaStatus.SYNCED)

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
        server.handle_rm({"type": "recover", "source_id": "S2"})
        self.assertEqual(server.recovery_source_id, "S2")
        self.assertEqual(server.replica_status, ReplicaStatus.UP)
        server.handle_rm({"type": "wait"})
        self.assertIsNone(server.recovery_source_id)
        self.assertEqual(server.replica_status, ReplicaStatus.UP)

    def test_invalid_instruction_preserves_status(self) -> None:
        server = self.make_server()
        messages: list[dict[str, object]] = [
            {"type": "recover", "source_id": "S1"},
            {"type": "recover", "source_id": "unknown"},
            {"type": "recover"},
            {"type": "membership", "members": []},
        ]
        for message in messages:
            with self.subTest(message=message), self.assertRaises(ValueError):
                server.handle_rm(message)
            self.assertEqual(server.replica_status, ReplicaStatus.UP)
            self.assertIsNone(server.recovery_source_id)

    def test_unsynced_primary_does_not_publish_checkpoint(self) -> None:
        server = self.make_server(mode="passive")
        with patch.object(server, "_send_json") as send:
            server.send_checkpoint()
        send.assert_not_called()
        self.assertEqual(server.checkpoint_count, 1)

    def test_synced_backup_still_ignores_client_requests(self) -> None:
        server = self.make_server(replica_id="S2", mode="passive")
        server.handle_rm({"type": "initialize"})
        peer = MagicMock(spec=socket.socket)
        with patch.object(server, "_send_json") as send:
            server.handle_client(
                peer, {"type": "request", "client_id": "C1", "request_num": 1}
            )
        send.assert_not_called()
        self.assertEqual(server.state, 0)
