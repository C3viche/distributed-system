"""Check the transport boundary using real buffered socket traffic."""

# Tests attach socket pairs directly to avoid depending on configured addresses.
# pyright: reportPrivateUsage=false

import socket
import unittest
from unittest.mock import patch

from distributed_system.common import recv_json, send_json
from distributed_system.server.server import Server


class ServerTransportTests(unittest.TestCase):
    def connect_peer(self, server: Server) -> socket.socket:
        self.addCleanup(server.close)
        local, peer = socket.socketpair()
        self.addCleanup(peer.close)
        peer.settimeout(1.0)
        server.transport.io.register(local, "client")
        return peer

    def test_registration_and_checkpoint_in_same_read_change_routing(self) -> None:
        server = Server("S2", replication_mode="passive", primary_id="S1")
        peer = self.connect_peer(server)
        peer.sendall(
            b'{"type":"register_primary","replica_id":"S1"}\n'
            + b'{"type":"checkpoint","replica_id":"S1","my_state":7,'
            + b'"checkpoint_count":3}\n'
        )
        server.transport.poll(1.0)
        self.assertEqual(server.state, 7)
        # The label must survive beyond the initial read, too.
        send_json(
            peer,
            {
                "type": "checkpoint",
                "replica_id": "S1",
                "my_state": 8,
                "checkpoint_count": 4,
            },
        )
        server.transport.poll(1.0)
        self.assertEqual(server.state, 8)
        self.assertEqual(server.checkpoint_count, 4)

    def test_partial_request_waits_for_complete_frame_and_reply_is_flushed(
        self,
    ) -> None:
        server = Server("S1", replication_mode="active")
        peer = self.connect_peer(server)
        peer.sendall(b'{"type":"request","client_id":"C1",')
        server.transport.poll(1.0)
        self.assertEqual(server.state, 0)
        peer.sendall(b'"request_num":1}\n')
        server.transport.poll(1.0)
        server.transport.poll(1.0)
        self.assertEqual(
            recv_json(peer),
            {
                "type": "reply",
                "client_id": "C1",
                "replica_id": "S1",
                "request_num": 1,
                "state": 1,
            },
        )

    def test_lfd_registration_heartbeat_and_disconnect(self) -> None:
        server = Server("S1", replication_mode="active")
        self.addCleanup(server.close)
        replica, lfd = socket.socketpair()
        self.addCleanup(lfd.close)
        lfd.settimeout(1.0)
        with patch(
            "distributed_system.server.transport.socket.create_connection",
            return_value=replica,
        ):
            server.try_connect_to_lfd()
        server.transport.poll(1.0)
        self.assertEqual(
            recv_json(lfd),
            {
                "type": "registration",
                "replica_id": "S1",
            },
        )
        send_json(lfd, {"type": "heartbeat", "count": 1})
        server.transport.poll(1.0)
        server.transport.poll(1.0)
        self.assertEqual(
            recv_json(lfd),
            {
                "type": "heartbeat_ack",
                "replica_id": "S1",
                "count": 1,
            },
        )
        lfd.close()
        server.transport.poll(1.0)
        self.assertIsNone(server._lfd)
        self.assertEqual(server.state, 0)

    def test_passive_backup_ignores_client_request(self) -> None:
        server = Server("S2", replication_mode="passive", primary_id="S1")
        peer = self.connect_peer(server)
        send_json(peer, {"type": "request", "client_id": "C1", "request_num": 1})
        server.transport.poll(1.0)
        self.assertEqual(server.state, 0)
        peer.setblocking(False)
        with self.assertRaises(BlockingIOError):
            _ = peer.recv(1)
