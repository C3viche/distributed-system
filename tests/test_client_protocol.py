"""Ensure the thin client advances only after a successful replica reply."""
import unittest
from unittest.mock import patch
from distributed_system.client.client import Client

class ClientProtocolTests(unittest.TestCase):
    def test_request_number_advances_only_after_reply(self):
        client = Client('C1', 1)
        self.addCleanup(client.conns.close)
        with patch.object(client.conns, 'send_and_collect', side_effect=[None, {'state': 1}]) as send:
            self.assertFalse(client._send_and_await_reply())
            self.assertEqual(client.request_num, 1)
            self.assertTrue(client._send_and_await_reply())
            self.assertEqual(client.request_num, 2)
            self.assertEqual([call.args[0] for call in send.call_args_list], [1, 1])
