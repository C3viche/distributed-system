"""Persistent, nonblocking client connections to the configured replicas.

The first matching reply completes a request. Partial messages survive across
requests, and late replies are logged as duplicates while other peers progress.
"""

import selectors
import socket
import time
from typing import cast

from distributed_system.common import BufferedJsonConnection, log
from distributed_system.config import get_server_addresses


class ReplicaConnections:
    """Own replica sockets and their buffers for the client's lifetime."""

    def __init__(self, client_id: str,
                 replicas: dict[str, tuple[str, int]] | None = None,
                 timeout: float = 1.0) -> None:
        self.client_id = client_id
        self.replicas = replicas if replicas is not None else get_server_addresses()
        self.timeout = timeout
        self.alive: dict[str, socket.socket] = {}
        self.dead: set[str] = set()
        self._selector = selectors.DefaultSelector()
        self._connections: dict[socket.socket, BufferedJsonConnection] = {}
        self._last_completed = 0

    def connect_all(self) -> None:
        """Connect to available replicas, retaining main's bounded connect timeout."""
        for replica_id, (host, port) in self.replicas.items():
            if replica_id in self.dead or replica_id in self.alive:
                continue
            try:
                sock = socket.create_connection((host, port), timeout=self.timeout)
            except OSError as exc:
                log(f"{self.client_id} could not connect to {replica_id}: {exc}", kind="failure")
                self.dead.add(replica_id)
                continue
            self.alive[replica_id] = sock
            self._connections[sock] = BufferedJsonConnection(sock)
            self._selector.register(sock, selectors.EVENT_READ, replica_id)
            log(f"{self.client_id} connected to {replica_id} at {host}:{port}", kind="registration")

    def mark_dead(self, replica_id: str) -> None:
        """Remove a failed peer from both future broadcasts and readiness checks."""
        self.dead.add(replica_id)
        sock = self.alive.pop(replica_id, None)
        if sock is not None:
            self._selector.unregister(sock)
            self._connections.pop(sock)
            sock.close()
            log(f"{self.client_id} disconnected from {replica_id}", kind="failure")

    def send_and_collect(self, request_num: int, payload: str) -> dict[str, object] | None:
        """Queue the request for every peer and return the first matching reply."""
        for replica_id, sock in self.alive.items():
            self._connections[sock].queue({
                "type": "request", "client_id": self.client_id,
                "replica_id": replica_id, "request_num": request_num, "payload": payload,
            })
            self._selector.modify(sock, selectors.EVENT_READ | selectors.EVENT_WRITE, replica_id)
            log(f"Sent <{self.client_id}, {replica_id}, {request_num}, {payload}>", kind="send")
        return self._receive_until(time.monotonic() + self.timeout, request_num)

    def receive_pending(self, duration: float) -> None:
        """Service delayed replies and queued writes during the request interval."""
        self._receive_until(time.monotonic() + duration, None)

    def _receive_until(self, deadline: float, request_num: int | None) -> dict[str, object] | None:
        delivered = None
        while self.alive and (remaining := deadline - time.monotonic()) > 0:
            for key, events in self._selector.select(remaining):
                sock = cast(socket.socket, key.fileobj)
                replica_id = cast(str, key.data)
                # A closed socket can still have another event in this batch.
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
                        self.mark_dead(replica_id)
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
                        if 1 <= number <= self._last_completed:
                            log(f"request_num {number}: Discarded duplicate reply from {replica_id}", kind="info")
                        elif number == request_num and delivered is None:
                            log(f"Received <{self.client_id}, {replica_id}, {number}, reply, state={reply.get('state')}>", kind="receive")
                            self._last_completed = number
                            delivered = reply
                except BlockingIOError:
                    continue
                except (OSError, ValueError):
                    self.mark_dead(replica_id)
            if delivered is not None:
                return delivered
        return None

    def close(self) -> None:
        """Release all sockets and the selector when the client exits."""
        for replica_id in list(self.alive):
            self.mark_dead(replica_id)
        self._selector.close()
