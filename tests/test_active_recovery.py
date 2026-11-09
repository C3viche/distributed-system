"""Active snapshot pulls through the existing server selector/socket handlers.

These tests use quiescent traffic during snapshot transfer. They do not prove
request-log catch-up or state consistency during concurrent client broadcasts.
"""

# Tests attach channels directly and control connection/deadline behavior.
# pyright: reportPrivateUsage=false, reportAny=false

import selectors
import socket
import unittest
from unittest.mock import MagicMock, patch

from distributed_system.common import send_json
from distributed_system.config import ReplicaStatus
from distributed_system.server.server import Server


class ActiveRecoveryTests(unittest.TestCase):
    def server(self, replica_id: str) -> Server:
        server = Server(replica_id, replication_mode="active")
        self.addCleanup(server.sel.close)
        return server

    def channel(self) -> tuple[socket.socket, socket.socket]:
        local, remote = socket.socketpair()
        self.addCleanup(local.close)
        self.addCleanup(remote.close)
        return local, remote

    def start_pull(self, target: Server) -> tuple[socket.socket, socket.socket]:
        local, donor = self.channel()
        target.handle_rm({"type": "recover", "source_id": "S2"})
        with patch(
            "distributed_system.server.server.socket.create_connection",
            return_value=local,
        ):
            target.try_connect_to_recovery_source()
        return local, donor

    def test_pull_routes_request_installs_snapshot_and_completes_recovery(self) -> None:
        target = self.server("S1")
        donor = self.server("S2")
        donor.initialize_replica()
        donor.state = 7
        local, remote = self.start_pull(target)
        self.assertEqual(target.replica_status, ReplicaStatus.UP)
        donor._register(remote, "client")
        target._write_pending(local)
        donor._read_messages(remote, "client")
        self.assertEqual(donor.sel.get_key(remote).data, "recovery_peer")
        donor._write_pending(remote)
        with patch.object(
            target, "complete_recovery", wraps=target.complete_recovery
        ) as complete:
            target._read_messages(local, "recovery")
        complete.assert_called_once_with()
        self.assertEqual(target.state, 7)
        self.assertEqual(target.replica_status, ReplicaStatus.SYNCED)
        self.assertIsNone(target._recovery)
        self.assertIsNone(target.recovery_source_id)
        with patch.object(target, "_send_json") as send:
            target.handle_client(
                MagicMock(spec=socket.socket),
                {
                    "type": "request",
                    "client_id": "C1",
                    "request_num": 1,
                },
            )
        self.assertEqual(target.state, 8)
        send.assert_called_once()
        self.assertEqual(donor.state, 7)

    def test_invalid_snapshot_preserves_state_and_retries(self) -> None:
        target = self.server("S1")
        local, remote = self.start_pull(target)
        target.state = 3
        send_json(
            remote,
            {
                "type": "recovery_snapshot",
                "replica_id": "S3",
                "target_id": "S1",
                "my_state": 50,
            },
        )
        target._handle_socket_event(local, "recovery", selectors.EVENT_READ)
        self.assertEqual(target.state, 3)
        self.assertEqual(target.replica_status, ReplicaStatus.UP)
        self.assertEqual(target.recovery_source_id, "S2")
        self.assertIsNone(target._recovery)
        self.assertGreater(target._recovery_retry_at, 0)

    def test_response_timeout_keeps_replica_unsynchronized(self) -> None:
        target = self.server("S1")
        local, _ = self.start_pull(target)
        target._recovery_deadline = 10.0
        with patch(
            "distributed_system.server.server.time.monotonic", return_value=10.0
        ):
            target.try_connect_to_recovery_source()
        self.assertNotIn(local, target._connections)
        self.assertEqual(target._recovery_retry_at, 11.0)
        self.assertEqual(target.replica_status, ReplicaStatus.UP)

    def test_unavailable_donor_uses_backoff(self) -> None:
        target = self.server("S1")
        target.prepare_recovery("S2")
        with (
            patch("distributed_system.server.server.time.monotonic") as now,
            patch(
                "distributed_system.server.server.socket.create_connection",
                side_effect=OSError("offline"),
            ) as connect,
        ):
            now.return_value = 10.0
            target.try_connect_to_recovery_source()
            now.return_value = 10.5
            target.try_connect_to_recovery_source()
            self.assertEqual(connect.call_count, 1)
            now.return_value = 11.0
            target.try_connect_to_recovery_source()
            self.assertEqual(connect.call_count, 2)
        self.assertEqual(target.replica_status, ReplicaStatus.UP)

    def test_wait_cancels_pull_and_stale_response_is_rejected(self) -> None:
        target = self.server("S1")
        local, _ = self.start_pull(target)
        target.handle_rm({"type": "wait"})
        with self.assertRaises(ValueError):
            target.handle_recovery_snapshot(
                local,
                {
                    "type": "recovery_snapshot",
                    "replica_id": "S2",
                    "target_id": "S1",
                    "my_state": 7,
                },
            )
        self.assertEqual(target.state, 0)
        self.assertEqual(target.replica_status, ReplicaStatus.UP)

    def test_unsynchronized_donor_cannot_send_snapshot(self) -> None:
        donor = self.server("S2")
        local, _ = self.channel()
        donor._register(local, "client")
        with self.assertRaises(ValueError):
            donor.handle_recovery_request(
                local,
                {
                    "type": "recovery_request",
                    "replica_id": "S1",
                    "source_id": "S2",
                },
            )
        self.assertFalse(donor._connections[local].has_pending_output)
