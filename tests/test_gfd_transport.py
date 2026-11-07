"""GFD membership and failure handling over the shared socket interface."""

# Session setup and deterministic deadline checks inspect service internals.
# pyright: reportPrivateUsage=false

import socket
import unittest
from unittest.mock import patch

from distributed_system.common import recv_json, send_json
from distributed_system.gfd.gfd import GlobalFaultDetector
from distributed_system.gfd.sessions import PeerSession


class GFDTransportTests(unittest.TestCase):
    def connect_peer(
        self, gfd: GlobalFaultDetector
    ) -> tuple[socket.socket, socket.socket]:
        local, peer = socket.socketpair()
        peer.settimeout(1.0)
        self.addCleanup(peer.close)
        gfd.transport.register(local, "peer")
        gfd._sessions[local] = PeerSession(local)
        return local, peer

    def test_membership_publication_and_lfd_timeout(self) -> None:
        gfd = GlobalFaultDetector()
        self.addCleanup(gfd.close)
        _, client = self.connect_peer(gfd)
        send_json(client, {"type": "register_client", "client_id": "C1"})
        gfd.poll(1.0)
        gfd.poll(1.0)
        self.assertEqual(
            recv_json(client), {"type": "membership", "members": [], "member_count": 0}
        )
        local_lfd, lfd = self.connect_peer(gfd)
        send_json(lfd, {"type": "register_lfd", "lfd_id": "LFD1"})
        send_json(lfd, {"type": "add_replica", "replica_id": "S1"})
        gfd.poll(1.0)
        gfd.poll(1.0)
        self.assertEqual(
            recv_json(client),
            {"type": "membership", "members": ["S1"], "member_count": 1},
        )
        gfd._sessions[local_lfd].next_due = 0.0
        with patch("distributed_system.gfd.gfd.time.monotonic", return_value=10.0):
            gfd._tick_heartbeats()
        gfd.poll(1.0)
        heartbeat = recv_json(lfd)
        self.assertIsNotNone(heartbeat)
        assert heartbeat is not None
        self.assertEqual(heartbeat["type"], "heartbeat")
        with patch("distributed_system.gfd.gfd.time.monotonic", return_value=13.0):
            gfd._tick_heartbeats()
        self.assertNotIn(local_lfd, gfd._sessions)
        gfd.poll(1.0)
        self.assertEqual(
            recv_json(client), {"type": "membership", "members": [], "member_count": 0}
        )
