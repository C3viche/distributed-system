"""Independent active/passive clients using GFD membership and primary assignments."""

import time

from distributed_system.client.membership import MembershipChannel, parse_membership
from distributed_system.client.replica_conns import ReplicaConnections
from distributed_system.common import log
from distributed_system.config import (
    PRIMARY_ID,
    REPLICATION_MODE,
    get_server_addresses,
)


class Client:
    """One independent client process (C1, C2, or C3)."""

    def __init__(
        self,
        client_id: str,
        num_replicas: int | None = None,
        interval: float = 1.0,
        count: int | None = None,
        payload_template: str = "hello from {client_id} #{request_num}",
        replication_mode: str = REPLICATION_MODE,
    ) -> None:
        """Initialize the client configuration and state."""
        if replication_mode not in {"active", "passive"}:
            raise ValueError("replication_mode must be active or passive")
        self.replication_mode: str = replication_mode
        self.primary_id: str | None = PRIMARY_ID
        self.client_id: str = client_id
        # num_replicas is retained for callers using the old CLI; GFD is authoritative.
        self.replicas: dict[str, tuple[str, int]] = get_server_addresses()
        self.membership: list[str] = []
        self.gfd: MembershipChannel = MembershipChannel(
            client_id, self._apply_membership
        )
        self._received_membership: bool = False
        self.interval: float = interval
        self.count: int | None = count  # None -> loop until Ctrl-C
        self.payload_template: str = payload_template
        self.request_num: int = 1
        self.conns: ReplicaConnections = ReplicaConnections(
            client_id, replicas=self.replicas
        )

    def run(self) -> None:
        """Wait for GFD membership before connecting to any replicas."""
        log(
            f"{self.client_id} starting in {self.replication_mode} mode"
            + (
                f", primary={self.primary_id}"
                if self.replication_mode == "passive"
                else ""
            ),
            kind="info",
        )
        try:
            self._loop()
        except KeyboardInterrupt:
            log(f"{self.client_id} shutting down", kind="info")
        finally:
            self.gfd.close()
            self.conns.close()

    def _apply_membership(self, payload: object) -> None:
        """Apply one complete snapshot; replica sockets change at request boundaries."""
        members, primary = parse_membership(payload, self.replicas, self.primary_id)
        if primary != self.primary_id:
            log(f"{self.client_id} assigned primary: {primary}", kind="membership")
        self.primary_id = primary
        self.membership = members
        self._received_membership = True
        count = len(members)
        label = "member" if count == 1 else "members"
        member_list = f": {', '.join(members)}" if members else ""
        log(
            f"{self.client_id} receives GFD membership: {count} {label}{member_list}",
            kind="membership",
        )

    def _wait_between_requests(self) -> None:
        """Receive both membership updates and late replies during the interval."""
        deadline = time.monotonic() + self.interval
        while (remaining := deadline - time.monotonic()) > 0:
            # Small bounded slices retain ReplicaConnections' ownership of its selector.
            duration = min(remaining, 0.05)
            if self.conns.alive or self._received_membership:
                self.conns.receive_pending(duration)
                self.gfd.poll(0)
            else:
                self.gfd.poll(duration)

    def _loop(self) -> None:
        """Apply GFD snapshots at request boundaries, including empty snapshots."""
        sent = 0
        while self.count is None or sent < self.count:
            if self.gfd.socket is None and time.monotonic() >= self.gfd.retry_at:
                _ = self.gfd.connect()
            self.gfd.poll(0)
            if self._received_membership:
                self.conns.sync_membership(self.membership)
            if not self._has_request_target():
                self._wait_between_requests()
                continue
            _ = self._send_and_await_reply()
            sent += 1
            if self.count is None or sent < self.count:
                self._wait_between_requests()

    def _has_request_target(self) -> bool:
        """Return whether routing has a connected replica eligible for the next request."""
        if self.replication_mode == "active":
            return bool(self.conns.alive)
        return self.primary_id is not None and self.primary_id in self.conns.alive

    def _send_and_await_reply(self) -> bool:
        """Send one request through ReplicaConnections. True when a reply is delivered."""
        payload = self.payload_template.format(
            client_id=self.client_id,
            request_num=self.request_num,
        )
        if self.replication_mode == "active":
            reply = self.conns.send_active(self.request_num, payload)
        else:
            assert self.primary_id is not None
            reply = self.conns.send_passive(self.request_num, payload, self.primary_id)
        self.request_num += 1
        return reply is not None
