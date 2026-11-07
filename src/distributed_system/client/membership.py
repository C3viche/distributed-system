"""GFD membership channel and snapshot validation for a client.

The channel owns connection/retry mechanics. Client applies each validated
snapshot and reconciles replica connections at the next request boundary.
"""

import selectors
import socket
import time
from collections.abc import Callable, Collection
from typing import cast

from distributed_system.common import log
from distributed_system.config import get_address
from distributed_system.transport import SocketTransport


def parse_membership(
    payload: object, replicas: Collection[str], previous_primary: str | None
) -> tuple[list[str], str | None]:
    """Validate an entire snapshot before changing membership or assignment."""
    if not isinstance(payload, dict):
        raise ValueError("Invalid GFD membership")  # noqa: TRY004 - close bad peer
    message = cast(dict[str, object], payload)
    raw_members = message.get("members")
    if not isinstance(raw_members, list):
        raise ValueError("Invalid GFD membership")  # noqa: TRY004 - close bad peer
    members = cast(list[object], raw_members)
    count = message.get("member_count")
    if (
        message.get("type") != "membership"
        or any(
            not isinstance(member, str) or member not in replicas for member in members
        )
        or len(members) != len(set(cast(list[str], members)))
        or type(count) is not int
        or count != len(members)
    ):
        raise ValueError("Invalid GFD membership")
    primary = message.get("primary_id", previous_primary)
    if (
        "primary_id" in message
        and primary is not None
        and (not isinstance(primary, str) or primary not in members)
    ):
        raise ValueError("Invalid GFD primary assignment")
    return cast(list[str], members), cast(str | None, primary)


class MembershipChannel:
    """Maintain the GFD subscription using the shared socket interface."""

    def __init__(self, client_id: str, on_snapshot: Callable[[object], None]) -> None:
        self.client_id: str = client_id
        self._on_snapshot: Callable[[object], None] = on_snapshot
        self.transport: SocketTransport = SocketTransport()
        self.socket: socket.socket | None = None
        self.retry_at: float = 0.0

    def connect(self) -> bool:
        """Open the GFD channel and queue registration; bound each attempt to 2s."""
        try:
            self.socket = self.transport.connect(get_address("GFD"), "gfd", timeout=2.0)
            self.transport.send(
                self.socket, {"type": "register_client", "client_id": self.client_id}
            )
            log(f"{self.client_id} registered with GFD", kind="registration")
            return True
        except OSError as exc:
            self._disconnect(f"{self.client_id} could not connect to GFD: {exc}")
            return False

    def poll(self, timeout: float) -> None:
        """Receive snapshots and flush registration, or wait while disconnected."""
        if self.socket is None:
            time.sleep(timeout)
            return
        try:
            for sock, _, events in self.transport.ready(timeout):
                if events & selectors.EVENT_WRITE:
                    self.transport.flush(sock)
                if events & selectors.EVENT_READ:
                    messages = self.transport.receive(sock)
                    if messages is None:
                        raise ConnectionError("GFD disconnected")
                    for message in messages:
                        self._on_snapshot(message)
        except BlockingIOError:
            pass
        except (OSError, ValueError) as exc:
            self._disconnect(f"{self.client_id} lost GFD membership channel: {exc}")

    def _disconnect(self, reason: str) -> None:
        """Retain the last applied membership while scheduling reconnection."""
        if self.socket is not None:
            self.transport.disconnect(self.socket)
            self.socket = None
        self.retry_at = time.monotonic() + 1.0
        log(reason, kind="failure")

    def close(self) -> None:
        """Release the GFD channel and its selector on client shutdown."""
        self.transport.close()
        self.socket = None
