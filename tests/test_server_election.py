"""Backup-to-primary failover driven by GFD/RM membership snapshots."""

# These tests inspect role/channel bookkeeping without opening the serve loop.
# pyright: reportPrivateUsage=false, reportAny=false

import unittest
from unittest.mock import MagicMock, patch

from distributed_system.config import ReplicaStatus
from distributed_system.server.server import Server


class ServerElectionTests(unittest.TestCase):
    def make_server(self, replica_id: str, *, primary: str = "S1") -> Server:
        server = Server(
            replica_id, replication_mode="passive", primary_id=primary
        )
        self.addCleanup(server.sel.close)
        return server

    def test_event_for_socket_closed_earlier_in_batch_is_skipped(self) -> None:
        # promotion can close a socket that still has an event queued in the same select batch
        import selectors
        import socket

        server = self.make_server("S2")
        sock, peer = socket.socketpair()
        self.addCleanup(peer.close)
        server._register(sock, "primary")
        peer.sendall(b'{"type": "checkpoint"}\n')
        with patch("distributed_system.server.server.log"):
            server._close_sock(sock, "promotion closed it")
            server._handle_socket_event(sock, "primary", selectors.EVENT_READ)

    def test_backup_promotes_when_it_becomes_first_member(self) -> None:
        server = self.make_server("S2")
        self.assertEqual(server.role, "backup")
        server.handle_rm({"type": "recover", "source_id": "S1", "recovery_id": "checkpoint"})
        server.handle_checkpoint(
            {
                "type": "checkpoint",
                "request_history": {},
                "replica_id": "S1",
                "my_state": 7,
                "checkpoint_count": 3,
            }
        )
        server.handle_rm({"type": "ready", "recovery_id": "checkpoint"})
        self.assertEqual(server.replica_status, ReplicaStatus.SYNCED)

        server.handle_rm(
            {
                "type": "promote_primary",
                "replica_id": "S2",
                "members": ["S2", "S3"],
                "member_count": 2,
            }
        )
        self.assertEqual(server.role, "primary")
        self.assertEqual(server.primary_id, "S2")
        self.assertEqual(server.membership, ["S2", "S3"])
        self.assertEqual(server._pending_backups, ["S3"])
        self.assertEqual(server.replica_status, ReplicaStatus.SYNCED)
        self.assertEqual(server.state, 7)

        peer = MagicMock()
        server._connections[peer] = MagicMock()
        with patch.object(server, "_send_json") as send:
            server.handle_client(
                peer,
                {"type": "request", "client_id": "C1", "request_num": 1},
            )
        send.assert_called_once()
        self.assertEqual(server.state, 8)

    def test_promoted_primary_adds_later_members_as_backups(self) -> None:
        server = self.make_server("S2")
        server.replica_status = ReplicaStatus.SYNCED
        server.apply_membership(["S2"])
        self.assertEqual(server.role, "primary")
        self.assertEqual(server._pending_backups, [])
        server.apply_membership(["S2", "S3", "S1"])
        self.assertEqual(server.role, "primary")
        self.assertEqual(server.primary_id, "S2")
        self.assertEqual(server._pending_backups, ["S3", "S1"])

    def test_recovered_configured_primary_comes_back_as_backup(self) -> None:
        server = self.make_server("S1")
        self.assertEqual(server.role, "backup")
        server.apply_membership(["S2", "S3", "S1"])
        self.assertEqual(server.role, "backup")
        self.assertEqual(server.primary_id, "S2")
        self.assertEqual(server._pending_backups, [])

    def test_remaining_backup_retargets_to_the_new_primary(self) -> None:
        server = self.make_server("S3")
        server.apply_membership(["S2", "S3"])
        self.assertEqual(server.role, "backup")
        self.assertEqual(server.primary_id, "S2")

    def test_passive_servers_print_who_is_primary(self) -> None:
        backup = self.make_server("S2")
        primary = Server("S1", replication_mode="passive", primary_id="S1")
        primary.role = "primary"
        self.addCleanup(primary.sel.close)
        primary._backups = {MagicMock(): "S3", MagicMock(): "S2"}
        with patch("distributed_system.server.server.log") as log:
            backup._print_role_if_time()
            primary._print_role_if_time()
            primary._print_role_if_time()
        lines = [call.args[0] for call in log.call_args_list]
        self.assertEqual(lines, ["S2 (backup): primary is S1", "S1 (primary): backups S2, S3"])

    def test_active_replicas_do_not_change_role(self) -> None:
        server = Server("S2", replication_mode="active")
        self.addCleanup(server.sel.close)
        server.apply_membership(["S2", "S3"])
        self.assertEqual(server.role, "active")
        self.assertEqual(server.membership, ["S2", "S3"])

    def test_synced_backup_still_accepts_election_from_rm(self) -> None:
        server = self.make_server("S2")
        server.replica_status = ReplicaStatus.SYNCED
        server.handle_rm(
            {
                "type": "membership",
                "members": ["S2", "S3"],
                "member_count": 2,
                "primary_id": "S2",
            }
        )
        self.assertEqual(server.role, "primary")
        self.assertEqual(server.primary_id, "S2")

    def test_malformed_election_is_ignored(self) -> None:
        server = self.make_server("S2")
        server.handle_rm({"type": "promote_primary", "replica_id": "S2"})
        server.handle_rm({"type": "membership", "members": ["S9"]})
        self.assertEqual(server.role, "backup")
        self.assertEqual(server.primary_id, "S1")

    def test_unsynchronized_backup_cannot_be_promoted(self):
        server = self.make_server("S2")
        server.handle_rm({"type": "membership", "members": ["S2", "S3"],
                          "member_count": 2, "primary_id": "S2"})
        self.assertEqual(server.role, "backup")
        self.assertEqual(server.replica_status, ReplicaStatus.UP)

    def test_rm_skips_unsynchronized_first_member(self):
        from distributed_system.rm.rm import Recovery, ReplicationManager
        manager = ReplicationManager()
        self.addCleanup(manager._selector.close)
        manager.membership = ["S2", "S3"]
        manager.replicas = {"S2": Recovery(MagicMock()), "S3": Recovery(MagicMock(), ready=True)}
        manager._bootstrapped = True
        manager._snapshot_received = True
        with patch("distributed_system.rm.rm.REPLICATION_MODE", "passive"), patch.object(manager, "_send"):
            manager._coordinate()
        self.assertEqual(manager.primary_id, "S3")
        server = self.make_server("S3")
        server.replica_status = ReplicaStatus.SYNCED
        server.handle_rm({"type": "membership", "members": ["S2", "S3"],
                          "member_count": 2, "primary_id": manager.primary_id})
        self.assertEqual(server.role, "primary")

    def test_rm_keeps_ready_primary_when_earlier_members_finish_recovery(self):
        from distributed_system.rm.rm import Recovery, ReplicationManager
        manager = ReplicationManager()
        self.addCleanup(manager._selector.close)
        manager.membership = ["S2", "S3", "S1"]
        manager.primary_id = "S1"
        manager.replicas = {name: Recovery(MagicMock(), ready=True)
                            for name in manager.membership}
        manager._bootstrapped = True
        manager._snapshot_received = True
        with patch("distributed_system.rm.rm.REPLICATION_MODE", "passive"), patch.object(manager, "_send"):
            manager._coordinate()
            self.assertEqual(manager.primary_id, "S1")
            # Losing readiness still triggers election among eligible replicas.
            manager.replicas["S1"].ready = False
            manager._coordinate()
            self.assertEqual(manager.primary_id, "S2")
            # Recovery of S1 must not displace the new primary either.
            manager.replicas["S1"].ready = True
            manager.membership = ["S1", "S3", "S2"]
            manager._coordinate()
            self.assertEqual(manager.primary_id, "S2")
