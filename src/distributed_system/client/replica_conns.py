"""Persistent client sockets with active broadcast and passive primary routing."""

import errno
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
        self._connecting: dict[str, tuple[socket.socket, float]] = {}
        self._retry_at: dict[str, float] = {}
        self._last_finished = 0
        self._last_completed = 0

    def connect_all(self, members: list[str] | None = None) -> None:
        """Start connections without waiting for unavailable replicas."""
        selected = self.replicas if members is None else {name: self.replicas[name] for name in members}
        self._expire_connections()
        for replica_id, address in selected.items():
            if (replica_id in self.alive or replica_id in self._connecting
                or time.monotonic() < self._retry_at.get(replica_id, 0)):
                continue
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setblocking(False)
            try:
                result = sock.connect_ex(address)
                if result not in (0, errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY):
                    raise OSError(result, errno.errorcode.get(result, "connect failed"))
                self._selector.register(sock, selectors.EVENT_WRITE, replica_id)
                self._connecting[replica_id] = (sock, time.monotonic() + self.timeout)
                if result == 0:
                    self._finish_connect(replica_id, sock)
            except OSError as exc:
                if replica_id not in self._connecting:
                    sock.close()
                self.mark_dead(replica_id)
                log(f"{self.client_id} could not connect to {replica_id}: {exc}", kind="failure")
        # Collect handshakes already ready without waiting for slower peers.
        self.receive_pending(0)

    def _finish_connect(self, replica_id: str, sock: socket.socket) -> None:
        error = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        if error:
            raise OSError(error, errno.errorcode.get(error, "connect failed"))
        self._connecting.pop(replica_id)
        self.alive[replica_id] = sock
        self.dead.discard(replica_id)
        self._retry_at.pop(replica_id, None)
        self._connections[sock] = BufferedJsonConnection(sock)
        self._selector.modify(sock, selectors.EVENT_READ, replica_id)
        host, port = self.replicas[replica_id]
        log(f"{self.client_id} connected to {replica_id} at {host}:{port}", kind="registration")

    def _expire_connections(self) -> None:
        for replica_id, (_, deadline) in list(self._connecting.items()):
            if time.monotonic() >= deadline:
                log(f"{self.client_id} connection to {replica_id} timed out", kind="failure")
                self.mark_dead(replica_id)

    def sync_membership(self, members: list[str]) -> None:
        """Close removed peers and retry missing members with bounded backoff."""
        for replica_id in set(self.alive) | set(self._connecting):
            if replica_id not in members:
                self.mark_dead(replica_id)
        self.connect_all(members)

    def mark_dead(self, replica_id: str) -> None:
        """Close a failed or removed peer, allowing a later reconnect."""
        self.dead.add(replica_id)
        self._retry_at[replica_id] = time.monotonic() + 1.0
        pending = self._connecting.pop(replica_id, None)
        sock = self.alive.pop(replica_id, None)
        if sock is None and pending is not None:
            sock = pending[0]
        if sock is not None:
            self._selector.unregister(sock)
            self._connections.pop(sock, None)
            sock.close()
            log(f"{self.client_id} disconnected from {replica_id}", kind="failure")

    def send_and_collect(self, request_num: int, payload: str) -> dict[str, object] | None:
        """Compatibility entry point for active replication."""
        return self.send_active(request_num, payload)

    def send_active(self, request_num: int, payload: str) -> dict[str, object] | None:
        """Broadcast to connected replicas and deliver the first valid reply."""
        return self._send_to(list(self.alive), request_num, payload)

    def send_passive(self, request_num: int, payload: str,
                     primary_id: str) -> dict[str, object] | None:
        """Send only to the assigned primary and accept only its reply."""
        targets = [primary_id] if primary_id in self.alive else []
        return self._send_to(targets, request_num, payload)

    def _send_to(self, targets: list[str], request_num: int,
                 payload: str) -> dict[str, object] | None:
        if not targets:
            return None
        for replica_id in targets:
            sock = self.alive[replica_id]
            self._connections[sock].queue({
                "type": "request", "client_id": self.client_id,
                "replica_id": replica_id, "request_num": request_num, "payload": payload,
            })
            self._selector.modify(sock, selectors.EVENT_READ | selectors.EVENT_WRITE, replica_id)
            log(f"Sent <{self.client_id}, {replica_id}, {request_num}, {payload}>", kind="send")
        reply = self._receive_until(time.monotonic() + self.timeout, request_num, set(targets))
        self._last_finished = request_num
        if reply is None:
            log(f"request_num {request_num}: no reply from {', '.join(targets)}; continuing",
                kind="failure")
            # A timed-out queued write must not accumulate or execute much later.
            # Drop only connections that still have unsent bytes.
            for replica_id in targets:
                sock = self.alive.get(replica_id)
                if sock is not None and self._connections[sock].has_pending_output:
                    self.mark_dead(replica_id)
        return reply

    def receive_pending(self, duration: float) -> None:
        """Service delayed replies, writes, and pending connection attempts."""
        if not self.alive and not self._connecting:
            time.sleep(max(0, duration))
            return
        self._receive_until(time.monotonic() + duration, None)

    def _receive_until(self, deadline: float, request_num: int | None,
                       targets: set[str] | None = None) -> dict[str, object] | None:
        delivered = None
        first_poll = True
        while self.alive or self._connecting:
            self._expire_connections()
            remaining = deadline - time.monotonic()
            if remaining <= 0 and not first_poll:
                break
            first_poll = False
            wait = max(0, remaining)
            if self._connecting:
                wait = min(wait, max(0, min(end for _, end in self._connecting.values()) - time.monotonic()))
            for key, events in self._selector.select(wait):
                sock = cast(socket.socket, key.fileobj)
                replica_id = cast(str, key.data)
                try:
                    if replica_id in self._connecting and self._connecting[replica_id][0] is sock:
                        self._finish_connect(replica_id, sock)
                        continue
                    if sock not in self._connections:
                        continue
                    connection = self._connections[sock]
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
                        if 1 <= number <= self._last_finished or number == self._last_completed:
                            label = "duplicate" if number == self._last_completed else "late"
                            log(f"request_num {number}: Discarded {label} reply from {replica_id}", kind="info")
                        elif number == request_num and targets and replica_id in targets and delivered is None:
                            log(f"Received <{self.client_id}, {replica_id}, {number}, reply, state={reply.get('state')}>", kind="receive")
                            self._last_completed = number
                            delivered = reply
                except BlockingIOError:
                    continue
                except (OSError, ValueError) as exc:
                    log(f"{self.client_id} connection to {replica_id} failed: {exc}", kind="failure")
                    self.mark_dead(replica_id)
            if delivered is not None:
                return delivered
            if targets is not None and not targets.intersection(self.alive):
                break
        return None

    def close(self) -> None:
        """Release all sockets and the selector when the client exits."""
        for replica_id in set(self.alive) | set(self._connecting):
            self.mark_dead(replica_id)
        self._selector.close()
