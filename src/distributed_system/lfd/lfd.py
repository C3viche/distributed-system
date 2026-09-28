"""M2 LFD: monitor one replica and report membership to the GFD.

Run: uv run lfd --id LFD2 --heartbeat_freq 2
Frequency is in Hz. Server and GFD traffic share one nonblocking event loop.
"""

import argparse
import errno
import itertools
import math
import selectors
import socket
import time
from typing import cast

from distributed_system.common import BufferedJsonConnection, heartbeat, log, set_process_id
from distributed_system.config import REPLICA_LFDS, get_address

REPLICA_ASSIGNMENTS = {lfd: replica for replica, lfd in REPLICA_LFDS.items()}


def positive_number(value: str) -> float:
    try:
        number = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"'{value}' is not a valid number")
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("must be a positive finite number")
    return number


class ServerSession:
    """One incoming server connection, from registration through heartbeats."""

    def __init__(self, sock: socket.socket, registration_deadline: float) -> None:
        self.sock = sock
        self.connection = BufferedJsonConnection(sock)
        self.registration_deadline: float | None = registration_deadline
        self.next_heartbeat: float | None = None
        self.ack_deadline: float | None = None
        self.awaiting_count: int | None = None
        self.healthy = False

    def next_deadline(self) -> float | None:
        """Return the next registration, ACK, or heartbeat deadline."""
        if self.registration_deadline is not None:
            return self.registration_deadline
        return self.ack_deadline if self.ack_deadline is not None else self.next_heartbeat


class GFDSession:
    """Outbound GFD connection state, retained between reconnect attempts."""

    def __init__(self, address: tuple[str, int]) -> None:
        self.address = address
        self.sock: socket.socket | None = None
        self.connection: BufferedJsonConnection | None = None
        self.connecting = False
        self.connect_deadline: float | None = None
        self.retry_at = 0.0

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


class LocalFaultDetector:
    """One event loop owns registration, heartbeat scheduling, and socket I/O."""

    def __init__(
        self,
        lfd_id: str,
        replica_id: str | None = None,
        frequency: float = 1.0,
        timeout: float = 2.0,
    ) -> None:
        self.lfd_id = lfd_id
        self.replica_id = REPLICA_ASSIGNMENTS[lfd_id] if replica_id is None else replica_id
        if self.replica_id != REPLICA_ASSIGNMENTS[lfd_id]:
            raise ValueError(f"{lfd_id} must monitor {REPLICA_ASSIGNMENTS[lfd_id]}")
        self.interval = 1.0 / frequency
        self.timeout = timeout
        self.host, self.port = get_address(lfd_id)
        # Counts belong to the LFD lifetime and must survive server reconnects.
        self._counts = itertools.count(1)
        self._selector = selectors.DefaultSelector()
        self._connections: dict[socket.socket, ServerSession | GFDSession] = {}
        self._server: ServerSession | None = None
        self._gfd = GFDSession(get_address("GFD"))

    def _membership_changed(self, added: bool) -> None:
        """Queue a membership transition if the GFD connection is ready."""
        # A new GFD connection receives current health, not stale queued events.
        if self._gfd.sock is None or self._gfd.connecting:
            return
        kind = "add_replica" if added else "delete_replica"
        self._queue(self._gfd.sock, {
            "type": kind, "lfd_id": self.lfd_id, "replica_id": self.replica_id,
        })
        action = "add" if added else "delete"
        log(f"{self.lfd_id}: {action} replica {self.replica_id}", kind="membership")

    def _connect_gfd(self, now: float) -> None:
        """Start a nonblocking connection; completion is handled on write readiness."""
        host, port = self._gfd.address
        log(f"{self.lfd_id} trying to connect to GFD at {host}:{port}", kind="info")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(False)
        try:
            result = sock.connect_ex(self._gfd.address)
        except OSError as exc:
            sock.close()
            self._gfd.retry_at = now + 1.0
            log(f"{self.lfd_id} could not reach GFD ({exc}); retrying in 1s", kind="failure")
            return
        if result not in (0, errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY):
            sock.close()
            self._gfd.retry_at = now + 1.0
            log(f"{self.lfd_id} could not reach GFD ({errno.errorcode.get(result, result)}); retrying in 1s", kind="failure")
            return
        self._gfd.sock = sock
        self._gfd.connecting = True
        self._gfd.connect_deadline = now + self.timeout
        self._gfd.connection = BufferedJsonConnection(sock)
        self._connections[sock] = self._gfd
        self._selector.register(sock, selectors.EVENT_WRITE, "gfd")

    def _finish_gfd_connect(self) -> None:
        """Register with GFD and reannounce the currently healthy replica."""
        assert self._gfd.sock is not None
        error = self._gfd.sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        if error:
            self._drop(self._gfd.sock)
            return
        self._gfd.connecting = False
        self._gfd.connect_deadline = None
        self._queue(self._gfd.sock, {"type": "register_lfd", "lfd_id": self.lfd_id})
        log(f"{self.lfd_id} connected to GFD; registration queued", kind="registration")
        if self._server is not None and self._server.healthy:
            self._membership_changed(added=True)

    def _handle_gfd(self, message: dict[str, object]) -> None:
        """Validate and answer a heartbeat on the GFD channel."""
        count = message.get("count")
        if (message.get("type") != "heartbeat" or message.get("from") != "GFD"
            or message.get("to") != self.lfd_id or type(count) is not int or count < 1):
            raise ValueError("Invalid GFD heartbeat")
        assert self._gfd.sock is not None
        log(f"[{count}] {self.lfd_id} receives heartbeat from GFD", kind="heartbeat")
        self._queue(self._gfd.sock, heartbeat(self.lfd_id, "GFD", count, ack=True))
        log(f"[{count}] {self.lfd_id} sending heartbeat ACK to GFD", kind="heartbeat")

    def _accept(self, listener: socket.socket) -> None:
        """Track an incoming server until it registers or times out."""
        sock, _ = listener.accept()
        self._connections[sock] = ServerSession(sock, time.monotonic() + self.timeout)
        self._selector.register(sock, selectors.EVENT_READ, "server")

    def _queue(self, sock: socket.socket, message: dict[str, object]) -> None:
        """Buffer a message and request write readiness from the shared selector."""
        connection = self._connections[sock].connection
        assert connection is not None
        connection.queue(message)
        self._selector.modify(sock, selectors.EVENT_READ | selectors.EVENT_WRITE,
                              self._selector.get_key(sock).data)

    def _drop(self, sock: socket.socket) -> None:
        """Close one connection and coordinate retry or membership removal."""
        self._selector.unregister(sock)
        self._connections.pop(sock)
        sock.close()
        if sock is self._gfd.sock:
            was_connecting = self._gfd.connecting
            self._gfd.disconnected(time.monotonic())
            if was_connecting:
                # The attempt never completed (refused, timed out, host down).
                log(f"{self.lfd_id} could not reach GFD; retrying in 1s", kind="failure")
            else:
                log(f"{self.lfd_id} lost GFD connection; retrying in 1s", kind="failure")
            return
        if self._server is not None and sock is self._server.sock:
            log(f"{self.replica_id} has died", kind="failure")
            was_healthy = self._server.healthy
            self._server = None
            if was_healthy:
                self._membership_changed(added=False)

    def _message(self, sock: socket.socket, message: dict[str, object]) -> None:
        """Handle server registration or a heartbeat ACK for that session."""
        session = self._connections[sock]
        assert isinstance(session, ServerSession)
        if session.registration_deadline is not None:
            if (
                message.get("type") != "registration"
                or message.get("replica_id") != self.replica_id
                or self._server is not None
            ):
                log("Invalid or duplicate server registration", kind="failure")
                self._drop(sock)
                return
            session.registration_deadline = None
            self._server = session
            session.next_heartbeat = time.monotonic()
            log(f"{self.replica_id} registered with {self.lfd_id}", kind="registration")
            return

        if (
            session.awaiting_count is None
            or message.get("type") != "heartbeat_ack"
            or message.get("replica_id") != self.replica_id
            or message.get("count") != session.awaiting_count
        ):
            log("Invalid heartbeat ACK; closing connection", kind="failure")
            self._drop(sock)
            return
        log(
            f"[{session.awaiting_count}] {self.lfd_id} receives heartbeat from {self.replica_id}",
            kind="heartbeat",
        )
        session.awaiting_count = None
        session.ack_deadline = None
        if not session.healthy:
            session.healthy = True
            self._membership_changed(added=True)

    def _tick(self, now: float) -> None:
        """Service connection deadlines and schedule server heartbeats."""
        if self._gfd.sock is None and now >= self._gfd.retry_at:
            self._connect_gfd(now)
        elif (self._gfd.sock is not None and self._gfd.connect_deadline is not None
              and now >= self._gfd.connect_deadline):
            self._drop(self._gfd.sock)
        for sock, session in list(self._connections.items()):
            if (isinstance(session, ServerSession)
                and session.registration_deadline is not None
                and now >= session.registration_deadline):
                log("Server registration timed out", kind="failure")
                self._drop(sock)
        if self._server is None:
            return
        if self._server.ack_deadline is not None and now >= self._server.ack_deadline:
            self._drop(self._server.sock)
        elif (
            self._server.awaiting_count is None
            and self._server.next_heartbeat is not None
            and now >= self._server.next_heartbeat
        ):
            count = next(self._counts)
            self._server.awaiting_count = count
            self._server.ack_deadline = now + self.timeout
            self._server.next_heartbeat = now + self.interval
            log(f"[{count}] {self.lfd_id} sending heartbeat to {self.replica_id}", kind="heartbeat")
            self._queue(self._server.sock, {"type": "heartbeat", "lfd_id": self.lfd_id, "count": count})

    def _wait_timeout(self) -> float | None:
        """Wait only until the earliest deadline owned by either session type."""
        deadlines = [session.next_deadline() for session in self._connections.values()
                     if isinstance(session, ServerSession)]
        deadlines.append(self._gfd.next_deadline())
        pending = [deadline for deadline in deadlines if deadline is not None]
        return max(0.0, min(pending) - time.monotonic()) if pending else None

    def run(self) -> None:
        """Dispatch server and GFD socket events in one nonblocking loop."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((self.host, self.port))
            listener.listen()
            listener.setblocking(False)
            self._selector.register(listener, selectors.EVENT_READ, "listener")
            log(f"{self.lfd_id} listening on {self.host}:{self.port}", kind="info")
            try:
                while True:
                    self._tick(time.monotonic())
                    events = self._selector.select(self._wait_timeout())
                    # Enforce deadlines even if a peer continuously trickles bytes.
                    self._tick(time.monotonic())
                    for key, mask in events:
                        sock = cast(socket.socket, key.fileobj)
                        try:
                            if key.data == "listener":
                                self._accept(listener)
                                continue
                            if sock not in self._connections:
                                continue
                            if key.data == "gfd" and self._gfd.connecting:
                                self._finish_gfd_connect()
                                continue
                            connection = self._connections[sock].connection
                            assert connection is not None
                            if mask & selectors.EVENT_READ:
                                messages = connection.receive()
                                if messages is None:
                                    self._drop(sock)
                                    continue
                                for message in messages:
                                    if not isinstance(message, dict):
                                        raise ValueError("Expected a JSON object")  # noqa: TRY004 - peer validation
                                    if key.data == "gfd":
                                        self._handle_gfd(message)
                                    else:
                                        self._message(sock, message)
                                    if sock not in self._connections:
                                        break
                            if sock in self._connections and mask & selectors.EVENT_WRITE:
                                connection.flush()
                                if not connection.has_pending_output:
                                    self._selector.modify(sock, selectors.EVENT_READ, key.data)
                        except BlockingIOError:
                            pass
                        except (OSError, ValueError) as exc:
                            log(f"{self.lfd_id} connection error: {exc}", kind="failure")
                            if sock in self._connections:
                                self._drop(sock)
            finally:
                for sock in self._connections:
                    sock.close()
                self._connections.clear()
                self._selector.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--id", choices=list(REPLICA_ASSIGNMENTS), required=True)
    parser.add_argument("--replica-id", choices=list(REPLICA_LFDS), default=None, help="assigned replica (default: matching S1/S2/S3)")
    parser.add_argument(
        "--freq", "--heartbeat_freq", dest="frequency", type=positive_number,
        default=1.0, help="heartbeats per second (Hz)",
    )
    parser.add_argument("--timeout", type=positive_number, default=2.0, help="ACK/registration timeout in seconds")
    args = parser.parse_args()
    if args.replica_id is not None and args.replica_id != REPLICA_ASSIGNMENTS[args.id]:
        parser.error(f"{args.id} must monitor {REPLICA_ASSIGNMENTS[args.id]}")
    set_process_id(cast(str, args.id))
    detector = LocalFaultDetector(
        lfd_id=cast(str, args.id), replica_id=cast(str | None, args.replica_id),
        frequency=cast(float, args.frequency), timeout=cast(float, args.timeout),
    )
    try:
        detector.run()
    except KeyboardInterrupt:
        log(f"{detector.lfd_id} shutting down", kind="info")


if __name__ == "__main__":
    main()
