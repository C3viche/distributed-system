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

from distributed_system.common import BufferedJsonConnection, heartbeat, log
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
        self._counts = itertools.count(1)
        self._selector = selectors.DefaultSelector()
        self._connections: dict[socket.socket, BufferedJsonConnection] = {}
        self._registration_deadlines: dict[socket.socket, float] = {}
        self._server: socket.socket | None = None
        self._next_heartbeat: float | None = None
        self._ack_deadline: float | None = None
        self._awaiting_count: int | None = None
        self._healthy = False
        self._gfd: socket.socket | None = None
        self._gfd_connecting = False
        self._gfd_connect_deadline: float | None = None
        self._gfd_retry_at = 0.0
        self._gfd_address = get_address("GFD")

    def _membership_changed(self, added: bool) -> None:
        # A new GFD connection receives current health, not stale queued events.
        if self._gfd is None or self._gfd_connecting:
            return
        kind = "add_replica" if added else "delete_replica"
        self._queue(self._gfd, {
            "type": kind, "lfd_id": self.lfd_id, "replica_id": self.replica_id,
        })
        action = "add" if added else "delete"
        log(f"{self.lfd_id}: {action} replica {self.replica_id}", kind="membership")

    def _connect_gfd(self, now: float) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(False)
        try:
            result = sock.connect_ex(self._gfd_address)
        except OSError:
            sock.close()
            self._gfd_retry_at = now + 1.0
            return
        if result not in (0, errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY):
            sock.close()
            self._gfd_retry_at = now + 1.0
            return
        self._gfd = sock
        self._gfd_connecting = True
        self._gfd_connect_deadline = now + self.timeout
        self._connections[sock] = BufferedJsonConnection(sock)
        self._selector.register(sock, selectors.EVENT_WRITE, "gfd")

    def _finish_gfd_connect(self) -> None:
        assert self._gfd is not None
        error = self._gfd.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        if error:
            self._drop(self._gfd)
            return
        self._gfd_connecting = False
        self._gfd_connect_deadline = None
        self._queue(self._gfd, {"type": "register_lfd", "lfd_id": self.lfd_id})
        log(f"{self.lfd_id} connected to GFD; registration queued", kind="registration")
        if self._healthy:
            self._membership_changed(added=True)

    def _handle_gfd(self, message: dict[str, object]) -> None:
        count = message.get("count")
        if (message.get("type") != "heartbeat" or message.get("from") != "GFD"
            or message.get("to") != self.lfd_id or type(count) is not int or count < 1):
            raise ValueError("Invalid GFD heartbeat")
        assert self._gfd is not None
        log(f"[{count}] {self.lfd_id} receives heartbeat from GFD", kind="heartbeat")
        self._queue(self._gfd, heartbeat(self.lfd_id, "GFD", count, ack=True))
        log(f"[{count}] {self.lfd_id} sending heartbeat ACK to GFD", kind="heartbeat")

    def _accept(self, listener: socket.socket) -> None:
        sock, _ = listener.accept()
        self._connections[sock] = BufferedJsonConnection(sock)
        self._registration_deadlines[sock] = time.monotonic() + self.timeout
        self._selector.register(sock, selectors.EVENT_READ, "server")

    def _queue(self, sock: socket.socket, message: dict[str, object]) -> None:
        self._connections[sock].queue(message)
        self._selector.modify(sock, selectors.EVENT_READ | selectors.EVENT_WRITE,
                              self._selector.get_key(sock).data)

    def _drop(self, sock: socket.socket) -> None:
        self._selector.unregister(sock)
        self._connections.pop(sock)
        self._registration_deadlines.pop(sock, None)
        sock.close()
        if sock is self._gfd:
            self._gfd = None
            self._gfd_connecting = False
            self._gfd_connect_deadline = None
            self._gfd_retry_at = time.monotonic() + 1.0
            log(f"{self.lfd_id} lost GFD connection; retrying", kind="failure")
            return
        if sock is self._server:
            log(f"{self.replica_id} has died", kind="failure")
            self._server = None
            self._next_heartbeat = None
            self._ack_deadline = None
            self._awaiting_count = None
            was_healthy = self._healthy
            self._healthy = False
            if was_healthy:
                self._membership_changed(added=False)

    def _message(self, sock: socket.socket, message: dict[str, object]) -> None:
        if sock in self._registration_deadlines:
            if (
                message.get("type") != "registration"
                or message.get("replica_id") != self.replica_id
                or self._server is not None
            ):
                log("Invalid or duplicate server registration", kind="failure")
                self._drop(sock)
                return
            del self._registration_deadlines[sock]
            self._server = sock
            self._next_heartbeat = time.monotonic()
            log(f"{self.replica_id} registered with {self.lfd_id}", kind="registration")
            return

        if (
            self._awaiting_count is None
            or message.get("type") != "heartbeat_ack"
            or message.get("replica_id") != self.replica_id
            or message.get("count") != self._awaiting_count
        ):
            log("Invalid heartbeat ACK; closing connection", kind="failure")
            self._drop(sock)
            return
        log(
            f"[{self._awaiting_count}] {self.lfd_id} receives heartbeat from {self.replica_id}",
            kind="heartbeat",
        )
        self._awaiting_count = None
        self._ack_deadline = None
        if not self._healthy:
            self._healthy = True
            self._membership_changed(added=True)

    def _tick(self, now: float) -> None:
        if self._gfd is None and now >= self._gfd_retry_at:
            self._connect_gfd(now)
        elif (self._gfd is not None and self._gfd_connect_deadline is not None
              and now >= self._gfd_connect_deadline):
            self._drop(self._gfd)
        for sock, deadline in list(self._registration_deadlines.items()):
            if now >= deadline:
                log("Server registration timed out", kind="failure")
                self._drop(sock)
        if self._server is None:
            return
        if self._ack_deadline is not None and now >= self._ack_deadline:
            self._drop(self._server)
        elif (
            self._awaiting_count is None
            and self._next_heartbeat is not None
            and now >= self._next_heartbeat
        ):
            count = next(self._counts)
            self._awaiting_count = count
            self._ack_deadline = now + self.timeout
            self._next_heartbeat = now + self.interval
            log(f"[{count}] {self.lfd_id} sending heartbeat to {self.replica_id}", kind="heartbeat")
            self._queue(self._server, {"type": "heartbeat", "lfd_id": self.lfd_id, "count": count})

    def _wait_timeout(self) -> float | None:
        deadlines = list(self._registration_deadlines.values())
        if self._gfd is None:
            deadlines.append(self._gfd_retry_at)
        elif self._gfd_connect_deadline is not None:
            deadlines.append(self._gfd_connect_deadline)
        if self._ack_deadline is not None:
            deadlines.append(self._ack_deadline)
        elif self._next_heartbeat is not None:
            deadlines.append(self._next_heartbeat)
        return max(0.0, min(deadlines) - time.monotonic()) if deadlines else None

    def run(self) -> None:
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
                            if key.data == "gfd" and self._gfd_connecting:
                                self._finish_gfd_connect()
                                continue
                            connection = self._connections[sock]
                            if mask & selectors.EVENT_READ:
                                messages = connection.receive()
                                if messages is None:
                                    self._drop(sock)
                                    continue
                                for message in messages:
                                    if not isinstance(message, dict):
                                        raise ValueError("Expected a JSON object")
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
