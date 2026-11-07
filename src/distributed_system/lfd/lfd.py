"""M2 LFD: monitor one replica and report membership to the GFD.

Run: uv run lfd --id LFD2 --heartbeat_freq 2
Frequency is in Hz. Server and GFD traffic share one nonblocking event loop.
"""

import errno
import itertools
import selectors
import socket
import time
from typing import cast

from distributed_system.common import (
    heartbeat,
    log,
    set_process_id,
)
from distributed_system.config import get_address
from distributed_system.lfd.cli import parse_args
from distributed_system.lfd.sessions import (
    REPLICA_ASSIGNMENTS,
    GFDSession,
    ServerSession,
)
from distributed_system.transport import SocketTransport


class LocalFaultDetector:
    """One event loop owns registration, heartbeat scheduling, and socket I/O."""

    def __init__(
        self,
        lfd_id: str,
        replica_id: str | None = None,
        frequency: float = 1.0,
        timeout: float = 2.0,
    ) -> None:
        self.lfd_id: str = lfd_id
        self.replica_id: str = (
            REPLICA_ASSIGNMENTS[lfd_id] if replica_id is None else replica_id
        )
        if self.replica_id != REPLICA_ASSIGNMENTS[lfd_id]:
            raise ValueError(f"{lfd_id} must monitor {REPLICA_ASSIGNMENTS[lfd_id]}")
        self.interval: float = 1.0 / frequency
        self.timeout: float = timeout
        self.host: str
        self.port: int
        self.host, self.port = get_address(lfd_id)
        # Counts belong to the LFD lifetime and must survive server reconnects.
        self._counts: itertools.count[int] = itertools.count(1)
        self.transport: SocketTransport = SocketTransport()
        self._sessions: dict[socket.socket, ServerSession | GFDSession] = {}
        self._server: ServerSession | None = None
        self._absent_notice_at: float | None = None
        self._gfd: GFDSession = GFDSession(get_address("GFD"))

    def run(self) -> None:
        """Listen, maintain the GFD channel, and monitor the assigned replica."""
        _ = self.transport.listen(self.port, self.host)
        log(f"{self.lfd_id} listening on {self.host}:{self.port}", kind="info")
        try:
            while True:
                self._tick(time.monotonic())
                self.poll(self._wait_timeout())
        finally:
            self.close()

    def poll(self, timeout: float | None) -> None:
        """Dispatch socket events while enforcing registration and ACK deadlines."""
        events = self.transport.ready(timeout)
        # Enforce deadlines even if a peer continuously trickles bytes.
        self._tick(time.monotonic())
        for sock, kind, mask in events:
            try:
                if kind == "listener":
                    self._accept(sock)
                    continue
                if sock not in self._sessions:
                    continue
                if kind == "gfd" and self._gfd.connecting:
                    self._finish_gfd_connect()
                    continue
                if mask & selectors.EVENT_READ:
                    messages = self.transport.receive(sock)
                    if messages is None:
                        self._drop(sock)
                        continue
                    for raw_message in messages:
                        if not isinstance(raw_message, dict):
                            raise ValueError("Expected a JSON object")  # noqa: TRY004 - peer validation
                        message = cast(dict[str, object], raw_message)
                        if kind == "gfd":
                            self._handle_gfd(message)
                        else:
                            self._message(sock, message)
                        if sock not in self._sessions:
                            break
                if sock in self._sessions and mask & selectors.EVENT_WRITE:
                    self.transport.flush(sock)
            except BlockingIOError:
                pass
            except (OSError, ValueError) as exc:
                log(f"{self.lfd_id} connection error: {exc}", kind="failure")
                if sock in self._sessions:
                    self._drop(sock)

    def close(self) -> None:
        """Release service channels and session records without publishing failures."""
        self.transport.close()
        self._sessions.clear()

    def _membership_changed(self, added: bool) -> None:
        """Queue a membership transition if the GFD connection is ready."""
        # A new GFD connection receives current health, not stale queued events.
        if self._gfd.sock is None or self._gfd.connecting:
            return
        kind = "add_replica" if added else "delete_replica"
        self.transport.send(
            self._gfd.sock,
            {
                "type": kind,
                "lfd_id": self.lfd_id,
                "replica_id": self.replica_id,
            },
        )
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
            log(
                f"{self.lfd_id} could not reach GFD ({exc}); retrying in 1s",
                kind="failure",
            )
            return
        if result not in (0, errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY):
            sock.close()
            self._gfd.retry_at = now + 1.0
            log(
                f"{self.lfd_id} could not reach GFD ({errno.errorcode.get(result, result)}); retrying in 1s",
                kind="failure",
            )
            return
        self._gfd.sock = sock
        self._gfd.connecting = True
        self._gfd.connect_deadline = now + self.timeout
        self._sessions[sock] = self._gfd
        self.transport.watch_connect(sock, "gfd")

    def _finish_gfd_connect(self) -> None:
        """Register with GFD and reannounce the currently healthy replica."""
        assert self._gfd.sock is not None
        error = self._gfd.sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        if error:
            self._drop(self._gfd.sock)
            return
        self.transport.register(self._gfd.sock, "gfd")
        self._gfd.connecting = False
        self._gfd.connect_deadline = None
        self.transport.send(
            self._gfd.sock, {"type": "register_lfd", "lfd_id": self.lfd_id}
        )
        log(f"{self.lfd_id} connected to GFD; registration queued", kind="registration")
        if self._server is not None and self._server.healthy:
            self._membership_changed(added=True)

    def _handle_gfd(self, message: dict[str, object]) -> None:
        """Validate and answer a heartbeat on the GFD channel."""
        count = message.get("count")
        if (
            message.get("type") != "heartbeat"
            or message.get("from") != "GFD"
            or message.get("to") != self.lfd_id
            or type(count) is not int
            or count < 1
        ):
            raise ValueError("Invalid GFD heartbeat")
        assert self._gfd.sock is not None
        log(f"[{count}] {self.lfd_id} receives heartbeat from GFD", kind="heartbeat")
        self.transport.send(
            self._gfd.sock, heartbeat(self.lfd_id, "GFD", count, ack=True)
        )
        log(f"[{count}] {self.lfd_id} sending heartbeat ACK to GFD", kind="heartbeat")

    def _accept(self, listener: socket.socket) -> None:
        """Track an incoming server until it registers or times out."""
        sock, _ = self.transport.accept(listener, "server")
        self._sessions[sock] = ServerSession(sock, time.monotonic() + self.timeout)

    def _drop(self, sock: socket.socket) -> None:
        """Close one connection and coordinate retry or membership removal."""
        self.transport.disconnect(sock)
        _ = self._sessions.pop(sock)
        if sock is self._gfd.sock:
            was_connecting = self._gfd.connecting
            self._gfd.disconnected(time.monotonic())
            if was_connecting:
                # The attempt never completed (refused, timed out, host down).
                log(
                    f"{self.lfd_id} could not reach GFD; retrying in 1s", kind="failure"
                )
            else:
                log(
                    f"{self.lfd_id} lost GFD connection; retrying in 1s", kind="failure"
                )
            return
        if self._server is not None and sock is self._server.sock:
            log(f"{self.replica_id} has died", kind="failure")
            was_healthy = self._server.healthy
            self._server = None
            if was_healthy:
                self._membership_changed(added=False)

    def _message(self, sock: socket.socket, message: dict[str, object]) -> None:
        """Handle server registration or a heartbeat ACK for that session."""
        session = self._sessions[sock]
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
        elif (
            self._gfd.sock is not None
            and self._gfd.connect_deadline is not None
            and now >= self._gfd.connect_deadline
        ):
            self._drop(self._gfd.sock)
        for sock, session in list(self._sessions.items()):
            if (
                isinstance(session, ServerSession)
                and session.registration_deadline is not None
                and now >= session.registration_deadline
            ):
                log("Server registration timed out", kind="failure")
                self._drop(sock)
        if self._server is None:
            # Keep saying so once per heartbeat interval, so the console shows
            # the LFD is still waiting for its replica after a crash.
            if self._absent_notice_at is None or now >= self._absent_notice_at:
                log(
                    f"{self.lfd_id} heartbeat to {self.replica_id}: no connection, waiting for {self.replica_id} to register",
                    kind="failure",
                )
                self._absent_notice_at = now + self.interval
            return
        self._absent_notice_at = None
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
            log(
                f"[{count}] {self.lfd_id} sending heartbeat to {self.replica_id}",
                kind="heartbeat",
            )
            self.transport.send(
                self._server.sock,
                {"type": "heartbeat", "lfd_id": self.lfd_id, "count": count},
            )

    def _wait_timeout(self) -> float | None:
        """Wait only until the earliest deadline owned by either session type."""
        deadlines = [
            session.next_deadline()
            for session in self._sessions.values()
            if isinstance(session, ServerSession)
        ]
        deadlines.append(self._gfd.next_deadline())
        if self._server is None:
            deadlines.append(self._absent_notice_at)
        pending = [deadline for deadline in deadlines if deadline is not None]
        return max(0.0, min(pending) - time.monotonic()) if pending else None


def main() -> None:
    args = parse_args()
    set_process_id(cast(str, args.id))
    detector = LocalFaultDetector(
        lfd_id=cast(str, args.id),
        replica_id=cast(str | None, args.replica_id),
        frequency=cast(float, args.frequency),
        timeout=cast(float, args.timeout),
    )
    try:
        detector.run()
    except KeyboardInterrupt:
        log(f"{detector.lfd_id} shutting down", kind="info")


if __name__ == "__main__":
    main()
