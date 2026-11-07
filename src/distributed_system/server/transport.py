"""Socket mechanics for a server replica, independent of replication policy.

The server supplies message and disconnect callbacks. This module owns socket
buffers, selector events, and channel labels. Message handlers in server.py
decide replication behavior and change application state.
"""

import selectors
import socket
from collections.abc import Callable
from typing import cast

from distributed_system.common import log
from distributed_system.transport import SocketTransport


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
        self.io: SocketTransport = SocketTransport()
        self._client_ids: dict[socket.socket, str] = {}
        self._listener: socket.socket | None = None

    def listen(self, port: int) -> None:
        """Bind the replica listener on all interfaces."""
        self._listener = self.io.listen(port)

    def connect(
        self, address: tuple[str, int], kind: str, timeout: float
    ) -> socket.socket:
        """Open a service channel through the shared socket interface."""
        return self.io.connect(address, kind, timeout)

    def send(self, sock: socket.socket, message: dict[str, object]) -> None:
        """Queue a server protocol message without blocking."""
        self.io.send(sock, message)

    def remember_client(self, sock: socket.socket, client_id: str) -> None:
        """Retain a client's identity for disconnect logging."""
        self._client_ids[sock] = client_id

    def poll(self, timeout: float | None) -> None:
        """Dispatch one batch of events, waiting at most until the next deadline."""
        for sock, kind, events in self.io.ready(timeout):
            if sock is self._listener:
                self._accept(sock)
                continue
            try:
                if events & selectors.EVENT_READ:
                    self._read_messages(sock, kind)
                if self.io.has_connection(sock) and events & selectors.EVENT_WRITE:
                    self.io.flush(sock)
            except BlockingIOError:
                pass
            except (OSError, ValueError) as exc:
                self._close_peer(sock, f"Connection lost: {exc}")

    def close(self) -> None:
        """Release owned sockets at shutdown, without service retries."""
        self.io.close()
        self._client_ids.clear()
        self._listener = None

    def _accept(self, listener: socket.socket) -> None:
        _, address = self.io.accept(listener, "client")
        log(
            f"new client connection from {address[0]}:{address[1]}", kind="registration"
        )

    def _read_messages(self, sock: socket.socket, kind: str) -> None:
        messages = self.io.receive(sock)
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
                self.io.relabel(sock, next_kind)
                kind = next_kind

    def _close_peer(self, sock: socket.socket, reason: str) -> None:
        self.io.disconnect(sock)
        client_id = self._client_ids.pop(sock, None)
        if client_id and "client disconnected" in reason:
            reason = f"{client_id} disconnected"
        elif client_id and reason.startswith("Connection lost"):
            reason = f"{client_id} disconnected ({reason})"
        self._on_disconnect(sock, reason)
