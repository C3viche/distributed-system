"""Verify the same socket interface on both ends of a TCP connection."""

import selectors
import unittest
from typing import cast

from distributed_system.transport import SocketTransport


class SocketTransportTests(unittest.TestCase):
    def test_listen_connect_and_buffered_send_share_one_interface(self) -> None:
        server = SocketTransport()
        client = SocketTransport()
        self.addCleanup(server.close)
        self.addCleanup(client.close)
        listener = server.listen(0, host="127.0.0.1")
        address = cast(tuple[str, int], listener.getsockname())
        outgoing = client.connect(address, "server", timeout=1.0)
        self.assertEqual(
            server.ready(1.0), [(listener, "listener", selectors.EVENT_READ)]
        )
        incoming, _ = server.accept(listener, "client")
        client.send(outgoing, {"type": "request", "request_num": 1})
        self.assertTrue(client.has_pending_output(outgoing))
        self.assertTrue(client.ready(1.0)[0][2] & selectors.EVENT_WRITE)
        client.flush(outgoing)
        self.assertFalse(client.has_pending_output(outgoing))
        server.relabel(incoming, "registered_client")
        self.assertEqual(
            server.ready(1.0), [(incoming, "registered_client", selectors.EVENT_READ)]
        )
        self.assertEqual(
            server.receive(incoming), [{"type": "request", "request_num": 1}]
        )
        client.disconnect(outgoing)
        self.assertTrue(server.ready(1.0))
        self.assertIsNone(server.receive(incoming))
