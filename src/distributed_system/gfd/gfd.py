"""Global Fault Detector for Milestone 2 18-749.

Runs on the sacred (client) laptop. LFDs report replica health; registered
clients receive the current membership and every subsequent change.

Run from the repository root:
    uv run gfd
    uv run gfd --freq 2 --timeout 2
"""

from __future__ import annotations

import argparse
import itertools
import math
import selectors
import socket
import time
from typing import cast

from distributed_system.common import BufferedJsonConnection, heartbeat, log, set_process_id
from distributed_system.config import REPLICA_LFDS, resolve_address


def positive_number(value: str) -> float:
    try:
        num_value = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"'{value}' is not a valid number")

    if not math.isfinite(num_value) or num_value <= 0:
        raise argparse.ArgumentTypeError("must be a positive finite number")
    return num_value


def replica_id_for_lfd(lfd_id: str) -> str:
    """Use the same assignment as the server and LFD."""
    for replica, assigned_lfd in REPLICA_LFDS.items():
        if assigned_lfd == lfd_id:
            return replica
    raise ValueError(f"No replica mapping for LFD id: {lfd_id}")


class PeerSession:
    """One connection, identified by its first registration message."""

    def __init__(self, sock: socket.socket) -> None:
        self.sock: socket.socket = sock
        self.conn: BufferedJsonConnection = BufferedJsonConnection(sock)
        # Exactly one identity is set: an LFD owns a replica, a client observes it.
        self.lfd_id: str | None = None
        self.client_id: str | None = None
        self.replica_id: str | None = None
        self.counts: itertools.count[int] = itertools.count(1)
        self.awaiting_ack: bool = False
        self.next_due: float = 0.0
        self.ack_deadline: float = 0.0
        self.last_count: int | None = None


class GlobalFaultDetector:
    """Heartbeat LFDs and publish replica membership to clients."""

    def __init__(
        self,
        frequency: float = 1.0,
        timeout: float = 2.0,
        host_override: str | None = None,
        port_override: int | None = None,
    ) -> None:
        self.interval: float = 1.0 / frequency
        self.timeout: float = timeout
        self.host: str
        self.port: int
        self.host, self.port = resolve_address("GFD", host_override, port_override)
        self.membership: list[str] = []
        self.member_count: int = 0
        self._sessions: dict[socket.socket, PeerSession] = {}
        self.sel: selectors.DefaultSelector = selectors.DefaultSelector()

    def membership_text(self) -> str:
        n = self.member_count
        word = "member" if n == 1 else "members"
        if n == 0:
            return f"GFD: 0 {word}"
        return f"GFD: {n} {word}: {', '.join(self.membership)}"

    def _print_membership(self) -> None:
        # A membership transition is one publish point for the console and clients.
        log(self.membership_text(), kind="membership")
        self._publish_membership()

    def _publish_membership(self, recipient: PeerSession | None = None) -> None:
        """Send a full snapshot so late clients and updates use the same format."""
        message: dict[str, object] = {
            "type": "membership", "members": list(self.membership),
            "member_count": self.member_count,
        }
        clients = ([recipient] if recipient is not None else
                   [s for s in self._sessions.values() if s.client_id is not None])
        # Queue on each nonblocking connection; one slow client cannot hold up LFD heartbeats.
        for session in clients:
            try:
                self._send_json(session, message)
                log(f"GFD sends membership to {session.client_id}: {self.membership_text()}",
                    kind="membership")
            except (OSError, ValueError, KeyError) as exc:
                self._close_session(session.sock, f"{session.client_id} membership send failed: {exc}")

    def _add_replica(self, replica_id: str) -> bool:
        if replica_id in self.membership:
            return False
        self.membership.append(replica_id)
        self.member_count = len(self.membership)
        return True

    def _delete_replica(self, replica_id: str) -> bool:
        if replica_id not in self.membership:
            return False
        self.membership.remove(replica_id)
        self.member_count = len(self.membership)
        return True

    def _send_json(self, session: PeerSession, message: dict[str, object]) -> None:
        session.conn.queue(message)
        key = self.sel.get_key(session.sock)
        _ = self.sel.modify(session.sock, selectors.EVENT_READ | selectors.EVENT_WRITE,
                            cast(str, key.data))

    def _close_session(self, sock: socket.socket, why: str) -> None:
        session = self._sessions.pop(sock, None)
        try:
            _ = self.sel.unregister(sock)
        except KeyError:
            pass
        sock.close()
        log(why, kind="failure")
        if session is None or session.replica_id is None:
            return
        # An LFD disconnect invalidates its last reported healthy replica.
        replica_id = session.replica_id
        lfd_id = session.lfd_id or "LFD?"
        if self._delete_replica(replica_id):
            log(f"{lfd_id}: delete replica {replica_id}", kind="membership")
            self._print_membership()

    def _drop_existing(self, lfd_id: str, incoming: socket.socket) -> None:
        for sock, session in list(self._sessions.items()):
            if sock is incoming or session.lfd_id != lfd_id:
                continue
            self._close_session(sock, f"{lfd_id} replaced by a new connection")

    def _handle_register_client(self, session: PeerSession, msg: dict[str, object]) -> None:
        client_id = msg.get("client_id")
        if session.lfd_id is not None or session.client_id is not None:
            raise ValueError("Peer already registered")
        if not isinstance(client_id, str) or not client_id:
            raise ValueError("Invalid client ID")
        # Replacing a stale client socket avoids publishing the same update twice.
        for sock, existing in list(self._sessions.items()):
            if sock is not session.sock and existing.client_id == client_id:
                self._close_session(sock, f"{client_id} replaced by a new connection")
        session.client_id = client_id
        log(f"{client_id} registered with GFD", kind="registration")
        # Late joiners need the current state even if no further replica changes occur.
        self._publish_membership(session)

    def _handle_register(self, session: PeerSession, msg: dict[str, object]) -> None:
        lfd_id = msg.get("lfd_id")
        if session.client_id is not None:
            raise ValueError("Client cannot register as an LFD")
        if not isinstance(lfd_id, str) or not lfd_id:
            log("Invalid register_lfd (missing lfd_id)", kind="failure")
            return
        _ = replica_id_for_lfd(lfd_id)  # Reject unconfigured LFD identities.
        if session.lfd_id is not None and session.lfd_id != lfd_id:
            raise ValueError("Cannot change the identity of a registered LFD")
        self._drop_existing(lfd_id, session.sock)
        session.lfd_id = lfd_id
        session.awaiting_ack = False
        session.next_due = time.monotonic()
        log(f"{lfd_id} registered with GFD", kind="registration")

    def _owned_replica(self, session: PeerSession, msg: dict[str, object]) -> str:
        if session.lfd_id is None:
            raise ValueError("LFD must register before reporting membership")
        replica_id = replica_id_for_lfd(session.lfd_id)
        if (msg.get("lfd_id", session.lfd_id) != session.lfd_id
            or msg.get("replica_id", replica_id) != replica_id):
            raise ValueError("Membership message does not match the registered LFD")
        return replica_id

    def _handle_add(self, session: PeerSession, msg: dict[str, object]) -> None:
        replica_id = self._owned_replica(session, msg)
        session.replica_id = replica_id
        # Repeated add reports do not produce duplicate client notifications.
        if self._add_replica(replica_id):
            log(f"{session.lfd_id}: add replica {replica_id}", kind="membership")
            self._print_membership()

    def _handle_delete(self, session: PeerSession, msg: dict[str, object]) -> None:
        replica_id = self._owned_replica(session, msg)
        session.replica_id = None
        if self._delete_replica(replica_id):
            log(f"{session.lfd_id}: delete replica {replica_id}", kind="membership")
            self._print_membership()

    def _handle_ack(self, session: PeerSession, msg: dict[str, object]) -> None:
        if session.lfd_id is None or not session.awaiting_ack:
            return
        if (msg.get("from") != session.lfd_id or msg.get("to") != "GFD"
            or type(msg.get("count")) is not int or msg.get("count") != session.last_count):
            log(
                f"Invalid heartbeat ACK from {session.lfd_id} (count {msg.get('count')})",
                kind="failure",
            )
            return
        session.awaiting_ack = False
        log(
            f"[{session.last_count}] GFD receives heartbeat from {session.lfd_id}",
            kind="heartbeat",
        )

    def _handle_message(self, session: PeerSession, msg: dict[str, object]) -> None:
        raw_message: object = msg
        if not isinstance(raw_message, dict):
            raise ValueError("Expected a JSON object")  # noqa: TRY004 - peer validation closes this socket
        msg = cast(dict[str, object], raw_message)
        kind = msg.get("type")
        if kind == "register_client":
            self._handle_register_client(session, msg)
        elif session.client_id is not None:
            raise ValueError("Unexpected message from client")
        elif kind == "register_lfd":
            self._handle_register(session, msg)
        elif kind == "add_replica":
            self._handle_add(session, msg)
        elif kind == "delete_replica":
            self._handle_delete(session, msg)
        elif kind == "heartbeat_ack":
            self._handle_ack(session, msg)
        else:
            log(f"GFD ignored unexpected message type: {kind}", kind="info")

    def _send_heartbeat(self, session: PeerSession, now: float) -> None:
        if session.lfd_id is None:
            return
        count = next(session.counts)
        session.last_count = count
        session.awaiting_ack = True
        session.ack_deadline = now + self.timeout
        session.next_due = now + self.interval
        log(f"[{count}] GFD sending heartbeat to {session.lfd_id}", kind="heartbeat")
        self._send_json(session, heartbeat("GFD", session.lfd_id, count))

    def _tick_heartbeats(self) -> None:
        now = time.monotonic()
        for sock, session in list(self._sessions.items()):
            # Clients are subscribers; only registered LFDs are heartbeat targets.
            if session.lfd_id is None:
                continue
            if session.awaiting_ack:
                if now >= session.ack_deadline:
                    self._close_session(sock, f"{session.lfd_id} heartbeat timeout")
                continue
            if now >= session.next_due:
                try:
                    self._send_heartbeat(session, now)
                except (OSError, ValueError) as exc:
                    self._close_session(sock, f"{session.lfd_id} send failed: {exc}")

    def _select_timeout(self) -> float | None:
        now = time.monotonic()
        deadlines: list[float] = []
        for session in self._sessions.values():
            if session.lfd_id is None:
                continue
            if session.awaiting_ack:
                deadlines.append(max(0.0, session.ack_deadline - now))
            else:
                deadlines.append(max(0.0, session.next_due - now))
        if not deadlines:
            return None
        return min(deadlines)

    def _read_messages(self, sock: socket.socket) -> None:
        session = self._sessions.get(sock)
        if session is None:
            return
        messages = session.conn.receive()
        if messages is None:
            label = session.lfd_id or session.client_id or "Peer"
            self._close_session(sock, f"{label} disconnected")
            return
        for msg in messages:
            self._handle_message(session, msg)

    def _write_pending(self, sock: socket.socket) -> None:
        session = self._sessions.get(sock)
        if session is None:
            return
        session.conn.flush()
        if not session.conn.has_pending_output:
            _ = self.sel.modify(sock, selectors.EVENT_READ, "lfd")

    def _accept(self, listener: socket.socket) -> None:
        conn, addr = cast(tuple[socket.socket, tuple[str, int]], listener.accept())
        session = PeerSession(conn)
        self._sessions[conn] = session
        _ = self.sel.register(conn, selectors.EVENT_READ, "lfd")
        log(f"Connection from {addr[0]}:{addr[1]}", kind="info")

    def serve(self) -> None:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((self.host, self.port))
        listener.listen()
        listener.setblocking(False)
        log(f"GFD listening on {self.host}:{self.port}", kind="info")
        self._print_membership()
        _ = self.sel.register(listener, selectors.EVENT_READ, "listener")

        while True:
            self._tick_heartbeats()
            timeout = self._select_timeout()
            for key, events in self.sel.select(timeout=timeout):
                if not isinstance(key.fileobj, socket.socket):
                    continue
                sock = key.fileobj
                data = cast(str, key.data)
                if data == "listener":
                    self._accept(sock)
                    continue
                try:
                    if events & selectors.EVENT_READ:
                        self._read_messages(sock)
                    if sock in self._sessions and events & selectors.EVENT_WRITE:
                        self._write_pending(sock)
                except BlockingIOError:
                    pass
                except (OSError, ValueError) as exc:
                    self._close_session(sock, f"Connection lost: {exc}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument(
        "--freq",
        "--heartbeat_freq",
        dest="frequency",
        type=positive_number,
        default=1.0,
        help="GFD→LFD heartbeats per second (Hz); 2 means one every 0.5 seconds",
    )
    _ = parser.add_argument(
        "--timeout",
        type=positive_number,
        default=2.0,
        help="seconds to wait for an LFD heartbeat ACK (default: 2)",
    )
    _ = parser.add_argument("--host", default=None, help="override GFD_HOST")
    _ = parser.add_argument("--port", type=int, default=None, help="override GFD_PORT")
    args = parser.parse_args()
    set_process_id("GFD")

    detector = GlobalFaultDetector(
        frequency=cast(float, args.frequency),
        timeout=cast(float, args.timeout),
        host_override=cast(str | None, args.host),
        port_override=cast(int | None, args.port),
    )
    try:
        detector.serve()
    except KeyboardInterrupt:
        log("GFD shutting down", kind="info")


if __name__ == "__main__":
    main()
