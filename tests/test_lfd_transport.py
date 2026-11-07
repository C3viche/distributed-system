"""LFD registration, heartbeat, and membership traffic over shared sockets."""

# Attach socket pairs directly to exercise protocol state without fixed ports.
# pyright: reportPrivateUsage=false

import socket
import time
import unittest

from distributed_system.common import recv_json, send_json
from distributed_system.lfd.lfd import LocalFaultDetector
from distributed_system.lfd.sessions import ServerSession


class LFDTransportTests(unittest.TestCase):
    def test_registration_health_and_crash_publication(self) -> None:
        lfd = LocalFaultDetector("LFD1")
        self.addCleanup(lfd.close)
        local_gfd, gfd = socket.socketpair()
        self.addCleanup(gfd.close)
        gfd.settimeout(1.0)
        lfd._gfd.sock = local_gfd
        lfd._gfd.connecting = True
        lfd._gfd.connect_deadline = time.monotonic() + lfd.timeout
        lfd._sessions[local_gfd] = lfd._gfd
        lfd.transport.watch_connect(local_gfd, "gfd")
        lfd.poll(0)
        lfd.poll(0)
        self.assertEqual(recv_json(gfd), {"type": "register_lfd", "lfd_id": "LFD1"})
        send_json(gfd, {"type": "heartbeat", "from": "GFD", "to": "LFD1", "count": 1})
        lfd.poll(0)
        lfd.poll(0)
        self.assertEqual(
            recv_json(gfd),
            {"type": "heartbeat_ack", "from": "LFD1", "to": "GFD", "count": 1},
        )

        local_server, server = socket.socketpair()
        self.addCleanup(server.close)
        server.settimeout(1.0)
        lfd.transport.register(local_server, "server")
        lfd._sessions[local_server] = ServerSession(
            local_server, time.monotonic() + lfd.timeout
        )
        send_json(server, {"type": "registration", "replica_id": "S1"})
        for _ in range(3):
            lfd.poll(0)
        request = recv_json(server)
        assert request is not None
        self.assertEqual(request["type"], "heartbeat")
        send_json(
            server,
            {"type": "heartbeat_ack", "replica_id": "S1", "count": request["count"]},
        )
        lfd.poll(0)
        lfd.poll(0)
        self.assertEqual(
            recv_json(gfd),
            {"type": "add_replica", "lfd_id": "LFD1", "replica_id": "S1"},
        )
        server.close()
        lfd.poll(0)
        lfd.poll(0)
        self.assertEqual(
            recv_json(gfd),
            {"type": "delete_replica", "lfd_id": "LFD1", "replica_id": "S1"},
        )
