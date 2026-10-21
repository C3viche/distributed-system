"""Routing and fault-continuity checks using local TCP replicas."""

# unittest initializes fixtures in setUp; these tests intentionally inspect
# private state and dynamic mock calls. Keep those exceptions local to tests.
# pyright: reportUninitializedInstanceVariable=false, reportPrivateUsage=false, reportAny=false

import json
import socket
import threading
import time
import unittest
from typing import cast, override
from unittest.mock import AsyncMock, MagicMock, patch

from distributed_system.client import _parse_args
from distributed_system.client.client import Client
from distributed_system.client.replica_conns import ReplicaConnections
from distributed_system.common import BufferedJsonConnection, send_json


class Replica:
    """A TCP replica that records requests and optionally answers them."""

    def __init__(self, replica_id: str) -> None:
        self.replica_id: str = replica_id
        self.requests: list[dict[str, object]] = []
        self.answer: bool = True
        self.peer: socket.socket | None = None
        self.stopped: threading.Event = threading.Event()
        self.listener: socket.socket = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen()
        self.listener.settimeout(0.05)
        self.address: tuple[str, int] = cast(tuple[str, int], self.listener.getsockname())
        self.thread: threading.Thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def run(self) -> None:
        while not self.stopped.is_set():
            try:
                peer = self.listener.accept()[0]
            except TimeoutError:
                continue
            except OSError:
                return
            self.peer = peer
            peer.settimeout(0.05)
            buffer = bytearray()
            try:
                while not self.stopped.is_set():
                    try:
                        data = peer.recv(4096)
                    except TimeoutError:
                        continue
                    if not data:
                        break
                    buffer.extend(data)
                    while b"\n" in buffer:
                        line, _, remainder = buffer.partition(b"\n")
                        buffer[:] = remainder
                        request = cast(dict[str, object], json.loads(line))
                        self.requests.append(request)
                        if self.answer:
                            number = request["request_num"]
                            assert isinstance(number, int)
                            self.reply(number)
            except OSError:
                pass
            finally:
                peer.close()
                self.peer = None

    def reply(self, number: int) -> None:
        peer = self.peer
        assert peer is not None
        send_json(peer, {"type": "reply", "client_id": "C1",
                             "replica_id": self.replica_id,
                             "request_num": number, "state": number})

    def close(self) -> None:
        self.stopped.set()
        self.listener.close()
        if self.peer is not None:
            try:
                self.peer.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        self.thread.join(1)


class RoutingTests(unittest.TestCase):
    @override
    def setUp(self) -> None:
        quiet = patch("distributed_system.client.replica_conns.log")
        self.logs: MagicMock | AsyncMock = quiet.start()
        self.addCleanup(quiet.stop)
        self.replicas: dict[str, Replica] = {name: Replica(name) for name in ("S1", "S2", "S3")}
        for replica in self.replicas.values():
            self.addCleanup(replica.close)
        self.conns: ReplicaConnections = ReplicaConnections("C1", {
            name: replica.address for name, replica in self.replicas.items()
        }, timeout=0.2)
        self.addCleanup(self.conns.close)
        self.conns.connect_all()
        deadline = time.monotonic() + 1
        while len(self.conns.alive) < 3 and time.monotonic() < deadline:
            self.conns.receive_pending(0.01)
        self.assertEqual(set(self.conns.alive), {"S1", "S2", "S3"})

    def require_reply(self, reply: dict[str, object] | None) -> dict[str, object]:
        if reply is None:
            self.fail("Expected a replica reply")
        return reply

    def test_active_broadcast_and_duplicates(self) -> None:
        reply = self.conns.send_active(1, "hello")
        assert reply is not None
        self.assertEqual(reply["request_num"], 1)
        self.conns.receive_pending(0.05)
        self.assertEqual([len(replica.requests) for replica in self.replicas.values()], [1, 1, 1])
        self.assertTrue(any("Discarded duplicate" in call.args[0]
                            for call in self.logs.call_args_list))
        self.assertIsNotNone(self.conns.send_and_collect(2, "legacy"))

    def test_passive_backup_failure_and_reassignment(self) -> None:
        self.assertEqual(self.require_reply(self.conns.send_passive(1, "hello", "S1"))["replica_id"], "S1")
        self.assertEqual(len(self.conns.alive), 3)
        self.assertEqual([len(self.replicas[name].requests) for name in ("S2", "S3")], [0, 0])
        self.replicas["S3"].close()
        self.conns.sync_membership(["S1", "S2"])
        self.assertEqual(self.require_reply(self.conns.send_passive(2, "fault", "S1"))["replica_id"], "S1")
        self.assertEqual(self.require_reply(self.conns.send_passive(3, "new primary", "S2"))["replica_id"], "S2")
        self.assertEqual(len(self.replicas["S1"].requests), 2)

    def test_timeout_late_reply_and_next_request(self) -> None:
        self.replicas["S1"].answer = False
        self.assertIsNone(self.conns.send_passive(1, "timeout", "S1"))
        self.replicas["S1"].reply(1)
        self.replicas["S1"].answer = True
        reply = self.conns.send_passive(2, "next", "S1")
        assert reply is not None
        self.assertEqual(reply["request_num"], 2)
        self.assertTrue(any("Discarded late reply" in call.args[0]
                            for call in self.logs.call_args_list))

    def test_wrong_replica_reply_cannot_complete_passive_request(self) -> None:
        self.replicas["S1"].answer = False
        deadline = time.monotonic() + 1
        while self.replicas["S2"].peer is None and time.monotonic() < deadline:
            time.sleep(0.001)
        self.replicas["S2"].reply(1)
        self.assertIsNone(self.conns.send_passive(1, "primary only", "S1"))

    def test_unreachable_backup_does_not_delay_primary(self) -> None:
        self.replicas["S3"].close()
        self.conns.mark_dead("S3")
        self.conns._retry_at["S3"] = 0
        started = time.monotonic()
        self.conns.sync_membership(["S1", "S2", "S3"])
        self.assertIsNotNone(self.conns.send_passive(1, "still running", "S1"))
        self.assertLess(time.monotonic() - started, 0.2)
        self.assertNotIn("S3", self.conns.alive)
        self.assertGreater(self.conns._retry_at["S3"], time.monotonic())

    def test_reconnect_readmitted_member(self) -> None:
        self.conns.sync_membership(["S1", "S2"])
        self.conns._retry_at["S3"] = 0
        self.conns.sync_membership(["S1", "S2", "S3"])
        deadline = time.monotonic() + 1
        while "S3" not in self.conns.alive and time.monotonic() < deadline:
            self.conns.receive_pending(0.01)
        self.assertIn("S3", self.conns.alive)
        self.assertIsNotNone(self.conns.send_passive(1, "reconnected", "S3"))


class ClientTests(unittest.TestCase):
    @override
    def setUp(self) -> None:
        quiet = patch("distributed_system.client.client.log")
        _ = quiet.start()
        self.addCleanup(quiet.stop)
        self.client: Client = Client("C1", replication_mode="passive", count=2, interval=0.001)
        self.addCleanup(self.client.conns.close)
        receiver, publisher = socket.socketpair()
        self.publisher: socket.socket = publisher
        self.client._gfd = BufferedJsonConnection(receiver)
        self.addCleanup(receiver.close)
        self.addCleanup(self.publisher.close)

    def snapshot(self, members: list[str], **extra: object) -> None:
        send_json(self.publisher, {"type": "membership", "members": members,
                                  "member_count": len(members), **extra})
        self.client._read_gfd()

    def test_primary_assignment_omission_and_clear(self) -> None:
        self.snapshot(["S1", "S2", "S3"])
        self.assertEqual(self.client.primary_id, "S1")
        self.snapshot(["S1", "S2"], primary_id="S2")
        self.assertEqual(self.client.primary_id, "S2")
        self.snapshot(["S2"])
        self.assertEqual(self.client.primary_id, "S2")
        self.snapshot(["S2"], primary_id=None)
        self.assertIsNone(self.client.primary_id)
        self.assertFalse(self.client._has_request_target())

    def test_invalid_assignment_preserves_entire_snapshot(self) -> None:
        self.snapshot(["S1", "S2"], primary_id="S2")
        self.snapshot(["S1"], primary_id="S3")
        self.assertEqual(self.client.membership, ["S1", "S2"])
        self.assertEqual(self.client.primary_id, "S2")
        self.assertIsNone(self.client._gfd)

    def test_missing_primary_does_not_fall_back(self) -> None:
        with patch.object(self.client.conns, "alive", {"S2": object()}):
            self.assertFalse(self.client._has_request_target())
            self.snapshot(["S2"], primary_id="S2")
            self.assertTrue(self.client._has_request_target())

    def test_loop_continues_after_timeout_and_counts_issued_requests(self) -> None:
        with (patch.object(self.client, "_has_request_target", return_value=True),
              patch.object(self.client.conns, "send_passive", side_effect=[None, {"request_num": 2}]) as send):
            self.client._loop()
        self.assertEqual([call.args[0] for call in send.call_args_list], [1, 2])
        self.assertEqual(self.client.request_num, 3)

    def test_cli_defaults_and_passive_flag(self) -> None:
        with patch("sys.argv", ["client", "--id", "C1"]):
            self.assertEqual(_parse_args().replication_mode, "active")
        with patch("sys.argv", ["client", "--id", "C1", "--replication-mode", "passive"]):
            self.assertEqual(_parse_args().replication_mode, "passive")


if __name__ == "__main__":
    _ = unittest.main()
