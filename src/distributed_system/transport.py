"""Shared buffered JSON socket interface for clients and servers.

This layer owns listening, connection buffers, and selector subscriptions.
Callers own protocol labels, connection deadlines, retries, and message policy.
Read/write operations may raise BlockingIOError; callers retry on readiness.
"""

import selectors
import socket
from typing import cast

from distributed_system.common import BufferedJsonConnection


class SocketTransport:
    """Manage socket I/O without knowing client or server replication roles."""

    def __init__(self) -> None:
        self._selector: selectors.DefaultSelector = selectors.DefaultSelector()
        self._connections: dict[socket.socket, BufferedJsonConnection] = {}

    def listen(self, port: int, host: str = "0.0.0.0") -> socket.socket:
        """Open a listener; ready() reports it with the label 'listener'."""
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((host, port))
            listener.listen()
            _ = self._selector.register(listener, selectors.EVENT_READ, "listener")
        except OSError:
            listener.close()
            raise
        return listener

    def accept(
        self, listener: socket.socket, label: str
    ) -> tuple[socket.socket, tuple[str, int]]:
        """Accept a peer and attach its JSON buffers under the supplied label."""
        peer, address = cast(tuple[socket.socket, tuple[str, int]], listener.accept())
        self.register(peer, label)
        return peer, address

    def connect(
        self, address: tuple[str, int], label: str, timeout: float
    ) -> socket.socket:
        """Open a bounded connection and attach nonblocking JSON buffers."""
        sock = socket.create_connection(address, timeout=timeout)
        self.register(sock, label)
        return sock

    def watch_connect(self, sock: socket.socket, label: str) -> None:
        """Watch a caller-started nonblocking connect for completion.

        The caller checks SO_ERROR and calls register() after success, or
        disconnect() after failure or expiry. Pending sockets have no buffers.
        """
        _ = self._selector.register(sock, selectors.EVENT_WRITE, label)

    def register(self, sock: socket.socket, label: str) -> None:
        """Attach a connected socket, also completing a watched connection."""
        self._connections[sock] = BufferedJsonConnection(sock)
        if sock in self._selector.get_map():
            _ = self._selector.modify(sock, selectors.EVENT_READ, label)
        else:
            _ = self._selector.register(sock, selectors.EVENT_READ, label)

    def ready(self, timeout: float | None) -> list[tuple[socket.socket, str, int]]:
        """Wait for socket readiness; return socket, protocol label, event mask."""
        return [
            (key.fileobj, cast(str, key.data), events)
            for key, events in self._selector.select(timeout)
            if isinstance(key.fileobj, socket.socket)
        ]

    def relabel(self, sock: socket.socket, label: str) -> None:
        """Change protocol routing while preserving read/write subscriptions."""
        key = self._selector.get_key(sock)
        _ = self._selector.modify(sock, key.events, label)

    def send(self, sock: socket.socket, message: dict[str, object]) -> None:
        """Queue a JSON message and watch the socket for write readiness."""
        self._connections[sock].queue(message)
        key = self._selector.get_key(sock)
        _ = self._selector.modify(
            sock, selectors.EVENT_READ | selectors.EVENT_WRITE, cast(str, key.data)
        )

    def flush(self, sock: socket.socket) -> None:
        """Attempt one write, retaining pending bytes and the current label."""
        connection = self._connections[sock]
        connection.flush()
        if not connection.has_pending_output:
            key = self._selector.get_key(sock)
            _ = self._selector.modify(sock, selectors.EVENT_READ, cast(str, key.data))

    def receive(self, sock: socket.socket) -> list[object] | None:
        """Decode complete lines, preserving partial data; None means EOF."""
        return self._connections[sock].receive()

    def has_connection(self, sock: socket.socket) -> bool:
        """Whether this socket has a connected JSON channel."""
        return sock in self._connections

    def has_pending_output(self, sock: socket.socket) -> bool:
        """Whether a queued message still has unsent bytes."""
        return self._connections[sock].has_pending_output

    def disconnect(self, sock: socket.socket) -> None:
        """Release a listener, pending connect, or connected channel."""
        _ = self._selector.unregister(sock)
        _ = self._connections.pop(sock, None)
        sock.close()

    def close(self) -> None:
        """Release every owned socket and the selector, without protocol actions."""
        for key in list(self._selector.get_map().values()):
            if isinstance(key.fileobj, socket.socket):
                self.disconnect(key.fileobj)
        self._selector.close()
