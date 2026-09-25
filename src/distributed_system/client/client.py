"""Milestone 1 client for 18-749.

Runs as an independent process (C1, C2, or C3). Each client opens
long-lived TCP connections to its configured replicas and sends tagged requests in a continuous
loop, incrementing its own request_num after each successful reply.

Console format matches server/LFD conventions in ``common.log``:
    Sent     <client_id, S1, N, payload>            (bold yellow)
    Received <client_id, S1, N, reply, state=X>     (yellow)

No coordination between C1/C2/C3. Each holds its own request_num.
"""

import selectors
import socket
import time
from typing import cast

from distributed_system.common import BufferedJsonConnection, log
from distributed_system.config import get_server_addresses


class Client:
    """One independent client process (C1, C2, or C3)."""

    def __init__(
        self,
        client_id: str,
        num_replicas: int | None,
        interval: float = 1.0,
        count: int | None = None,
        payload_template: str = "hello from {client_id} #{request_num}",
    ) -> None:
        """Initialize the client configuration and state."""
        self.client_id: str = client_id
        self.num_replicas: int | None = num_replicas
        self.replicas: dict[str, tuple[str, int]] = get_server_addresses(num_replicas)
        self.interval: float = interval
        self.count: int | None = count  # None -> loop until Ctrl-C / server closes
        self.payload_template: str = payload_template
        self.request_num: int = 1

    def run(self) -> None:
        """Connect to available replicas and keep receiving between requests."""
        log(f"{self.client_id} starting", kind="info")
        socks: dict[str, socket.socket] = {}
        self._selector = selectors.DefaultSelector()
        self._connections: dict[socket.socket, BufferedJsonConnection] = {}
        try:
            for replica_id, (host, port) in self.replicas.items():
                try:
                    sock = socket.create_connection((host, port), timeout=3.0)
                except OSError as exc:
                    log(f"{self.client_id} could not connect to {replica_id}: {exc}", kind="failure")
                    continue
                socks[replica_id] = sock
                self._connections[sock] = BufferedJsonConnection(sock)
                self._selector.register(sock, selectors.EVENT_READ, replica_id)
                log(f"{self.client_id} connected to {replica_id} at {host}:{port}", kind="registration")
            if not socks:
                log(f"{self.client_id} could not connect to any server replicas", kind="failure")
                return
            self._loop(socks)
        except KeyboardInterrupt:
            log(f"{self.client_id} shutting down", kind="info")
        finally:
            for sock in socks.values():
                sock.close()
            self._selector.close()
            self._connections.clear()

    def _drop(self, socks: dict[str, socket.socket], replica_id: str) -> None:
        sock = socks.pop(replica_id)
        self._selector.unregister(sock)
        self._connections.pop(sock)
        sock.close()
        log(f"{self.client_id} disconnected from {replica_id}", kind="failure")

    def _loop(self, socks: dict[str, socket.socket]) -> None:
        sent = 0
        while self.count is None or sent < self.count:
            if not self._send_and_await_reply(socks):
                return
            sent += 1
            if self.count is None or sent < self.count:
                # Receive late duplicates during the normal inter-request delay.
                self._receive_until(socks, time.monotonic() + self.interval, None)

    def _send_and_await_reply(self, socks: dict[str, socket.socket]) -> bool:
        request = {
            "type": "request", "client_id": self.client_id,
            "request_num": self.request_num,
            "payload": self.payload_template.format(
                client_id=self.client_id, request_num=self.request_num),
        }
        for replica_id, sock in socks.items():
            request["replica_id"] = replica_id
            self._connections[sock].queue(request)
            self._selector.modify(sock, selectors.EVENT_READ | selectors.EVENT_WRITE, replica_id)
            log(f"Sent <{self.client_id}, {replica_id}, {self.request_num}, {request['payload']}>", kind="send")
        return self._receive_until(socks, time.monotonic() + 3.0, self.request_num)

    def _receive_until(
        self, socks: dict[str, socket.socket], deadline: float, request_num: int | None,
    ) -> bool:
        """Wait for the first current reply; retain partial and late replies."""
        success = False
        while socks and (remaining := deadline - time.monotonic()) > 0:
            for key, events in self._selector.select(remaining):
                sock = cast(socket.socket, key.fileobj)
                replica_id = cast(str, key.data)
                # kqueue can return separate read/write events for a closed peer.
                if sock not in self._connections:
                    continue
                connection = self._connections[sock]
                try:
                    if events & selectors.EVENT_WRITE:
                        connection.flush()
                        if not connection.has_pending_output:
                            self._selector.modify(sock, selectors.EVENT_READ, replica_id)
                    if not events & selectors.EVENT_READ:
                        continue
                    messages = connection.receive()
                    if messages is None:
                        self._drop(socks, replica_id)
                        continue
                    for reply in messages:
                        if not isinstance(reply, dict):
                            continue
                        number = reply.get("request_num")
                        if (reply.get("type") != "reply"
                            or reply.get("client_id") != self.client_id
                            or reply.get("replica_id") != replica_id
                            or type(number) is not int):
                            continue
                        if 1 <= number < self.request_num:
                            log(f"request_num {number}: Discarded duplicate reply from {replica_id}", kind="info")
                        elif number == request_num and not success and self._reply_matches(reply, replica_id):
                            log(f"Received <{self.client_id}, {replica_id}, {self.request_num}, reply, state={reply.get('state')}>", kind="receive")
                            self.request_num += 1
                            success = True
                except BlockingIOError:
                    continue
                except (OSError, ValueError):
                    self._drop(socks, replica_id)
            if success:
                return True
        return False

    def _reply_matches(self, reply: dict[str, object], expected_replica_id: str) -> bool:
        """Validate that the response header matches the expected request metadata."""
        return (
            reply.get("type") == "reply"
            and reply.get("client_id") == self.client_id
            and reply.get("replica_id") == expected_replica_id
            and reply.get("request_num") == self.request_num
        )
