"""Client for Milestone 2 active replication with GFD membership.

Runs as an independent process (C1, C2, or C3). Each client opens one
long-lived TCP connection per replica and sends the same tagged request to
every live replica before the next request. The first reply is delivered;
later replies for that request_num are discarded.

No coordination between C1/C2/C3. Each holds its own request_num.
"""

import selectors
import socket
import time
from typing import cast

from distributed_system.common import BufferedJsonConnection, log, send_json
from distributed_system.config import get_address, get_server_addresses
from distributed_system.server.replica_conns import ReplicaConnections


class Client:
    """One independent client process (C1, C2, or C3)."""

    def __init__(
        self,
        client_id: str,
        num_replicas: int | None = None,
        interval: float = 1.0,
        count: int | None = None,
        payload_template: str = "hello from {client_id} #{request_num}",
    ) -> None:
        """Initialize the client configuration and state."""
        self.client_id: str = client_id
        # num_replicas is retained for callers using the old CLI; GFD is authoritative.
        self.replicas: dict[str, tuple[str, int]] = get_server_addresses()
        self.membership: list[str] = []
        self._gfd: BufferedJsonConnection | None = None
        self._received_membership: bool = False
        self._gfd_retry_at: float = 0.0
        self.interval: float = interval
        self.count: int | None = count  # None -> loop until Ctrl-C / server closes
        self.payload_template: str = payload_template
        self.request_num: int = 1
        self.conns: ReplicaConnections = ReplicaConnections(client_id, replicas=self.replicas)

    def run(self) -> None:
        """Wait for GFD membership before connecting to any replicas."""
        log(f"{self.client_id} starting", kind="info")
        try:
            self._loop()
        except KeyboardInterrupt:
            log(f"{self.client_id} shutting down", kind="info")
        finally:
            if self._gfd is not None:
                self._gfd.sock.close()
            self.conns.close()

    def _connect_gfd(self) -> bool:
        """Open the membership channel and ask GFD for a fresh snapshot.

        This is used at startup and after a lost GFD connection. The short
        connection timeout keeps an unavailable GFD from blocking forever.
        """
        sock: socket.socket | None = None
        try:
            sock = socket.create_connection(get_address("GFD"), timeout=2.0)
            send_json(sock, {"type": "register_client", "client_id": self.client_id})
            # Future snapshots arrive asynchronously, so subsequent reads use
            # the same nonblocking JSON-line buffer as the other components.
            self._gfd = BufferedJsonConnection(sock)
            log(f"{self.client_id} registered with GFD", kind="registration")
            return True
        except OSError as exc:
            if sock is not None:
                sock.close()
            log(f"{self.client_id} could not connect to GFD: {exc}", kind="failure")
            self._gfd_retry_at = time.monotonic() + 1.0
            return False

    def _read_gfd(self) -> None:
        """Accept complete membership snapshots from GFD.

        The buffer retains partial lines for the next read. Validate the full
        snapshot before replacing our view so a malformed update cannot leave
        the client with a partly changed member list.
        """
        assert self._gfd is not None
        try:
            messages = self._gfd.receive()
            if messages is None:
                raise ConnectionError("GFD disconnected")
            for payload in messages:
                # The JSON decoder can return a non-object despite its type
                # hint, so check the wire data before reading its fields.
                raw_message: object = payload
                if not isinstance(raw_message, dict):
                    raise ValueError("Invalid GFD membership")  # noqa: TRY004 - close bad peer
                message = cast(dict[str, object], raw_message)
                raw_members = message.get("members")
                if not isinstance(raw_members, list):
                    raise ValueError("Invalid GFD membership")  # noqa: TRY004 - close bad peer
                members = cast(list[object], raw_members)
                count = message.get("member_count")
                if (message.get("type") != "membership"
                    or any(not isinstance(member, str) or member not in self.replicas for member in members)
                    or len(members) != len(set(cast(list[str], members)))
                    or type(count) is not int or count != len(members)):
                    raise ValueError("Invalid GFD membership")
                # This only updates the desired member list. Replica sockets
                # are reconciled at the next request boundary.
                self.membership = cast(list[str], members)
                self._received_membership = True
                label = "member" if count == 1 else "members"
                member_list = f": {', '.join(self.membership)}" if members else ""
                log(f"{self.client_id} receives GFD membership: {count} {label}{member_list}",
                    kind="membership")
        except BlockingIOError:
            return
        except (OSError, ValueError) as exc:
            # Keep the last valid snapshot while the request loop reconnects.
            log(f"{self.client_id} lost GFD membership channel: {exc}", kind="failure")
            self._gfd.sock.close()
            self._gfd = None
            self._gfd_retry_at = time.monotonic() + 1.0

    def _wait_for_gfd(self, timeout: float) -> None:
        """Wait for one GFD read opportunity, or for the timeout to expire.

        This also acts as a timed sleep when GFD is disconnected. One read can
        contain a partial line, a full snapshot, or several snapshots.
        """
        if self._gfd is None:
            time.sleep(timeout)
            return
        with selectors.DefaultSelector() as sel:
            _ = sel.register(self._gfd.sock, selectors.EVENT_READ)
            if sel.select(timeout):
                self._read_gfd()

    def _wait_between_requests(self) -> None:
        """Receive both membership updates and late replies during the interval."""
        deadline = time.monotonic() + self.interval
        while (remaining := deadline - time.monotonic()) > 0:
            # Small bounded slices retain ReplicaConnections' ownership of its selector.
            duration = min(remaining, 0.05)
            if self.conns.alive:
                self.conns.receive_pending(duration)
                self._wait_for_gfd(0)
            else:
                self._wait_for_gfd(duration)

    def _loop(self) -> None:
        """Apply GFD snapshots at request boundaries, including empty snapshots."""
        sent = 0
        while self.count is None or sent < self.count:
            if self._gfd is None and time.monotonic() >= self._gfd_retry_at:
                self._connect_gfd()
            self._wait_for_gfd(0)
            if self._received_membership:
                self.conns.sync_membership(self.membership)
            if not self.conns.alive:
                self._wait_between_requests()
                continue
            if not self._send_and_await_reply():
                if not self.conns.alive:
                    self._wait_between_requests()
                    continue
                return
            sent += 1
            if self.count is None or sent < self.count:
                self._wait_between_requests()

    def _send_and_await_reply(self) -> bool:
        """Send one request through ReplicaConnections. True when a reply is delivered."""
        payload = self.payload_template.format(
            client_id=self.client_id,
            request_num=self.request_num,
        )
        reply = self.conns.send_and_collect(self.request_num, payload)
        if reply is None:
            return False
        self.request_num += 1
        return True
