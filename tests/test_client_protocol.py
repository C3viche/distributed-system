"""Client reply identity validation."""
import unittest
from distributed_system.client.client import Client

class ClientProtocolTests(unittest.TestCase):
    def test_reply_requires_matching_identity_and_request(self):
        client = Client('C1', 1)
        reply = dict(type='reply', client_id='C1', replica_id='S1', request_num=1)
        self.assertTrue(client._reply_matches(reply, 'S1'))
        for field, wrong in [('client_id', 'C2'), ('replica_id', 'S2'), ('request_num', 2)]:
            self.assertFalse(client._reply_matches({**reply, field: wrong}, 'S1'))
