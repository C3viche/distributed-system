"""Fault-detector session identities and heartbeat/retry deadlines."""

import itertools
import socket


class PeerSession:
    """One connection, identified by its first registration message."""

    def __init__(self, sock: socket.socket) -> None:
        self.sock: socket.socket = sock
        # Exactly one identity is set: an LFD owns a replica, a client observes it.
        self.lfd_id: str | None = None
        self.client_id: str | None = None
        self.replica_id: str | None = None
        self.counts: itertools.count[int] = itertools.count(1)
        self.awaiting_ack: bool = False
        self.next_due: float = 0.0
        self.ack_deadline: float = 0.0
        self.last_count: int | None = None
