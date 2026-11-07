"""Socket mechanics for a server replica, independent of replication policy.

The server supplies message and disconnect callbacks. This module owns socket
buffers, selector events, and channel labels. Message handlers in server.py
decide replication behavior and change application state.
"""

import selectors
import socket
from collections.abc import Callable
from typing import cast

from distributed_system.common import BufferedJsonConnection, log


class ServerTransport:
    """Run buffered JSON channels in the server's single event loop.

    A message callback returns the channel's next label. This lets a primary
    identify an accepted connection before checkpoints arrive, even when both
    messages arrive in the same read.
    """

    def __init__(
        self,
        on_message: Callable[[socket.socket, str, dict[str, object]], str],
        on_disconnect: Callable[[socket.socket, str], None],
    ) -> None:
        self._on_message: Callable[[socket.socket, str, dict[str, object]], str] = (
            on_message
        )
        self._on_disconnect: Callable[[socket.socket, str], None] = on_disconnect
        self._selector: selectors.DefaultSelector = selectors.DefaultSelector()
        self._connections: dict[socket.socket, BufferedJsonConnection] = {}
        self._client_ids: dict[socket.socket, str] = {}
        self._listener: socket.socket | None = None

    def listen(self, port: int) -> None:
        """Bind the replica listener on all interfaces."""
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("0.0.0.0", port))
            listener.listen()
            _ = self._selector.register(listener, selectors.EVENT_READ, "listener")
        except OSError:
            listener.close()
            raise
        self._listener = listener

    def connect(
        self, address: tuple[str, int], kind: str, timeout: float
    ) -> socket.socket:
        """Make a bounded connection and attach nonblocking JSON buffers."""
        sock = socket.create_connection(address, timeout=timeout)
        self._register(sock, kind)
        return sock

    def send(self, sock: socket.socket, message: dict[str, object]) -> None:
        """Queue output and request write readiness without blocking."""
        self._connections[sock].queue(message)
        key = self._selector.get_key(sock)
        _ = self._selector.modify(
            sock, selectors.EVENT_READ | selectors.EVENT_WRITE, cast(str, key.data)
        )

    def remember_client(self, sock: socket.socket, client_id: str) -> None:
        """Retain a client's identity for disconnect logging."""
        self._client_ids[sock] = client_id

    def poll(self, timeout: float | None) -> None:
        """Dispatch one batch of events, waiting at most until the next deadline."""
        for key, events in self._selector.select(timeout):
            if not isinstance(key.fileobj, socket.socket):
                continue
            sock = key.fileobj
            if sock is self._listener:
                self._accept(sock)
                continue
            try:
                if events & selectors.EVENT_READ:
                    self._read_messages(sock, cast(str, key.data))
                if sock in self._connections and events & selectors.EVENT_WRITE:
                    self._write_pending(sock)
            except BlockingIOError:
                pass
            except (OSError, ValueError) as exc:
                self._close_peer(sock, f"Connection lost: {exc}")

    def close(self) -> None:
        """Release owned sockets and the selector at shutdown, without retries."""
        for sock in self._connections:
            sock.close()
        self._connections.clear()
        self._client_ids.clear()
        if self._listener is not None:
            self._listener.close()
            self._listener = None
        self._selector.close()

    def _register(self, sock: socket.socket, kind: str) -> None:
        self._connections[sock] = BufferedJsonConnection(sock)
        _ = self._selector.register(sock, selectors.EVENT_READ, kind)

    def _accept(self, listener: socket.socket) -> None:
        conn, address = cast(tuple[socket.socket, tuple[str, int]], listener.accept())
        self._register(conn, "client")
        log(
            f"new client connection from {address[0]}:{address[1]}", kind="registration"
        )

    def _write_pending(self, sock: socket.socket) -> None:
        connection = self._connections[sock]
        connection.flush()
        if not connection.has_pending_output:
            # Reading may have changed the channel label during this event batch.
            _ = self._selector.modify(
                sock, selectors.EVENT_READ, cast(str, self._selector.get_key(sock).data)
            )

    def _read_messages(self, sock: socket.socket, kind: str) -> None:
        messages = self._connections[sock].receive()
        if messages is None:
            reason = {
                "lfd": "lost connection to LFD",
                "primary": "lost connection to primary",
                "backup": "lost checkpoint channel to backup",
            }.get(kind, "client disconnected")
            self._close_peer(sock, reason)
            return
        for message in messages:
            if not isinstance(message, dict):
                raise ValueError("Expected a JSON object")  # noqa: TRY004 - close malformed peer
            next_kind = self._on_message(sock, kind, cast(dict[str, object], message))
            if next_kind != kind:
                key = self._selector.get_key(sock)
                _ = self._selector.modify(sock, key.events, next_kind)
                kind = next_kind

    def _close_peer(self, sock: socket.socket, reason: str) -> None:
        _ = self._selector.unregister(sock)
        _ = self._connections.pop(sock)
        client_id = self._client_ids.pop(sock, None)
        sock.close()
        if client_id and "client disconnected" in reason:
            reason = f"{client_id} disconnected"
        elif client_id and reason.startswith("Connection lost"):
            reason = f"{client_id} disconnected ({reason})"
        self._on_disconnect(sock, reason)
