"""Fault-detector session identities and heartbeat/retry deadlines."""

import socket

from distributed_system.common import BufferedJsonConnection
from distributed_system.config import REPLICA_LFDS

REPLICA_ASSIGNMENTS = {lfd: replica for replica, lfd in REPLICA_LFDS.items()}


class ServerSession:
    """One incoming server connection, from registration through heartbeats."""

    def __init__(self, sock: socket.socket, registration_deadline: float) -> None:
        self.sock: socket.socket = sock
        self.connection: BufferedJsonConnection = BufferedJsonConnection(sock)
        self.registration_deadline: float | None = registration_deadline
        self.next_heartbeat: float | None = None
        self.ack_deadline: float | None = None
        self.awaiting_count: int | None = None
        self.healthy: bool = False

    def next_deadline(self) -> float | None:
        """Return the next registration, ACK, or heartbeat deadline."""
        if self.registration_deadline is not None:
            return self.registration_deadline
        return (
            self.ack_deadline if self.ack_deadline is not None else self.next_heartbeat
        )


class GFDSession:
    """Outbound GFD connection state, retained between reconnect attempts."""

    def __init__(self, address: tuple[str, int]) -> None:
        self.address: tuple[str, int] = address
        self.sock: socket.socket | None = None
        self.connection: BufferedJsonConnection | None = None
        self.connecting: bool = False
        self.connect_deadline: float | None = None
        self.retry_at: float = 0.0

    def disconnected(self, now: float) -> None:
        """Discard connection state and schedule the next attempt."""
        self.sock = None
        self.connection = None
        self.connecting = False
        self.connect_deadline = None
        self.retry_at = now + 1.0

    def next_deadline(self) -> float | None:
        """Wake for reconnection or expiry of an in-progress connection."""
        return self.retry_at if self.sock is None else self.connect_deadline
