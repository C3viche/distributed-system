"""Active-replication client that receives membership from the GFD.

Each client connects to the current members and sends the same tagged request
to each before advancing its request number.

Console format matches server/LFD conventions in ``common.log``:
    Sent     <client_id, S1, N, payload>            (bold yellow)
    Received <client_id, S1, N, reply, state=X>     (yellow)

No coordination between C1/C2/C3. Each holds its own request_num.
"""

import selectors
import socket
import time
from typing import cast

from distributed_system.common import BufferedJsonConnection, log, recv_json, send_json
from distributed_system.config import get_address, get_server_addresses


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
        self._received_membership = False
        self._gfd_retry_at = 0.0
        self.interval: float = interval
        self.count: int | None = count  # None -> loop until Ctrl-C / server closes
        self.payload_template: str = payload_template
        self.request_num: int = 1

    # Public entry point
    def run(self) -> None:
        """Receive initial membership, then run the request/reply loop.

        Handles network exceptions and graceful shutdown on KeyboardInterrupt.
        """
        log(f"{self.client_id} starting", kind="info")

        socks: dict[str, socket.socket] = {}
        try:
            if not self._connect_gfd():
                return
            if not self._wait_for_initial_membership():
                log(f"{self.client_id} did not receive initial GFD membership", kind="failure")
                return
            self._loop(socks)
        except KeyboardInterrupt:
            log(f"{self.client_id} shutting down", kind="info")
        finally:
            if self._gfd is not None:
                self._gfd.sock.close()
            for s in socks.values():
                try:
                    s.close()
                except OSError:
                    pass

    def _connect_gfd(self) -> bool:
        """Register for snapshots before choosing any replica sockets."""
        sock: socket.socket | None = None
        try:
            sock = socket.create_connection(get_address("GFD"), timeout=2.0)
            send_json(sock, {"type": "register_client", "client_id": self.client_id})
            self._gfd = BufferedJsonConnection(sock)
            self._received_membership = False
            log(f"{self.client_id} registered with GFD", kind="registration")
            return True
        except OSError as exc:
            if sock is not None:
                sock.close()
            log(f"{self.client_id} could not connect to GFD: {exc}", kind="failure")
            self._gfd_retry_at = time.monotonic() + 1.0
            return False

    def _read_gfd(self) -> None:
        """Consume complete JSON lines; a partial network read must not block requests."""
        assert self._gfd is not None
        try:
            messages = self._gfd.receive()
            if messages is None:
                raise ConnectionError("GFD disconnected")
            for message in messages:
                members = message.get("members") if isinstance(message, dict) else None
                count = message.get("member_count") if isinstance(message, dict) else None
                if (not isinstance(message, dict) or message.get("type") != "membership"
                    or not isinstance(members, list)
                    or any(not isinstance(member, str) or member not in self.replicas for member in members)
                    or len(members) != len(set(members))
                    or type(count) is not int or count != len(members)):
                    raise ValueError("Invalid GFD membership")
                # Full snapshots replace the prior view, including removals.
                self.membership = members
                self._received_membership = True
                log(f"{self.client_id} receives GFD membership: {count} "
                    f"{'member' if count == 1 else 'members'}"
                    f"{': ' + ', '.join(members) if members else ''}", kind="membership")
        except (OSError, ValueError, ConnectionError) as exc:
            log(f"{self.client_id} lost GFD membership channel: {exc}", kind="failure")
            self._gfd.sock.close()
            self._gfd = None
            self._gfd_retry_at = time.monotonic() + 1.0

    def _wait_for_gfd(self, timeout: float) -> None:
        """Poll once for a membership message without blocking on a partial line."""
        if self._gfd is None:
            time.sleep(timeout)
            return
        with selectors.DefaultSelector() as sel:
            sel.register(self._gfd.sock, selectors.EVENT_READ)
            if sel.select(timeout):
                self._read_gfd()

    def _wait_for_initial_membership(self) -> bool:
        deadline = time.monotonic() + 3.0
        while self._gfd is not None and not self._received_membership:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            self._wait_for_gfd(remaining)
        return self._received_membership

    def _wait_between_requests(self) -> None:
        deadline = time.monotonic() + self.interval
        while (remaining := deadline - time.monotonic()) > 0:
            self._wait_for_gfd(remaining)

    def _sync_replicas(self, socks: dict[str, socket.socket]) -> None:
        """Apply the latest snapshot at a request boundary."""
        # Never remove a socket while the reply selector is using it.
        for replica_id in list(socks):
            if replica_id not in self.membership:
                socks.pop(replica_id).close()
                log(f"{self.client_id} removed {replica_id} from membership", kind="membership")
        for replica_id in self.membership:
            if replica_id in socks:
                continue
            host, port = self.replicas[replica_id]
            try:
                socks[replica_id] = socket.create_connection((host, port), timeout=2.0)
                socks[replica_id].settimeout(None)
                log(f"{self.client_id} connected to {replica_id} at {host}:{port}", kind="registration")
            except OSError as exc:
                log(f"{self.client_id} could not connect to {replica_id}: {exc}", kind="failure")

    def _loop(self, socks: dict[str, socket.socket]) -> None:
        """Execute the continuous request loop over the active socket connection."""
        sent = 0
        while self.count is None or sent < self.count:
            if self._gfd is None and time.monotonic() >= self._gfd_retry_at:
                if self._connect_gfd():
                    self._wait_for_initial_membership()
            self._sync_replicas(socks)
            if not socks:
                # A zero-member snapshot is valid; wait for GFD instead of exiting.
                self._wait_between_requests()
                continue
            if not self._send_and_await_reply(socks):
                return
            sent += 1
            # Skip the sleep after the final send so --count exits promptly.
            if self.count is None or sent < self.count:
                self._wait_between_requests()

    def _send_and_await_reply(self, socks: dict[str, socket.socket]) -> bool:
        """Construct, send a JSON request payload, and wait for the matching reply.

        Returns:
            bool: True if a valid matching reply was received, False on error or disconnect.
        """
        TTL = 3.0

        payload = self.payload_template.format(
            client_id=self.client_id,
            request_num=self.request_num,
        )
        request = {
            "type": "request",
            "client_id": self.client_id,
            "request_num": self.request_num,
            "payload": payload,
        }

        # Broadcast request to all replica sockets (Using list() allows safely popping failed sockets in-place)
        for replica_id, sock in list(socks.items()):
            request["replica_id"] = replica_id
            log(f"Sent <{self.client_id}, {replica_id}, {self.request_num}, {payload}>", kind="send")
            try:
                send_json(sock, request)
            except OSError as exc:
                log(f"{self.client_id} send to {replica_id} failed: {exc}", kind="failure")

        if not socks:
            return False

        # Register sockets with selector to collect replies non-blockingly across open sockets
        sel = selectors.DefaultSelector()
        for replica_id, sock in socks.items():
            _ = sel.register(sock, selectors.EVENT_READ, data=replica_id)
        if self._gfd is not None:
            # Membership must be consumed even while replies are arriving.
            sel.register(self._gfd.sock, selectors.EVENT_READ, data="GFD")

        success = False
        deadline = time.time() + TTL

        try:
            # Loop while we still have sockets registered AND time remaining on the clock
            while any(key.data != "GFD" for key in sel.get_map().values()) and (timeout := deadline - time.time() > 0):
                for key, _ in sel.select(timeout=timeout):
                    sock = cast(socket.socket, key.fileobj)
                    replica_id = cast(str, key.data)

                    if replica_id == "GFD":
                        self._read_gfd()
                        if self._gfd is None:
                            sel.unregister(sock)
                        # Stop waiting for replies from members the GFD just removed.
                        for active_id in list(socks):
                            if active_id not in self.membership:
                                try:
                                    sel.unregister(socks[active_id])
                                except KeyError:
                                    pass
                        continue

                    _ = sel.unregister(sock) # Unregister immediately so we only read once per cycle

                    # Attempt collecting the reply
                    try:
                        reply = recv_json(sock)
                    except (OSError, ConnectionError, ValueError):
                        continue

                    # Filter out malformed, disconnected, or stale replies
                    if reply is None or not self._reply_matches(reply, replica_id):
                        continue

                    # Process first valid reply

                    if not success:
                        state = reply.get("state")
                        log(f"Received <{self.client_id}, {replica_id}, {self.request_num}, reply, state={state}>", kind="receive")
                        success = True
                    # else:
                        # log(f"request_num {self.request_num}: Discarded duplicate reply from {replica_id}", kind="info")
        finally:
            sel.close()

        if success:
            self.request_num += 1
            return True

        return False

    def _reply_matches(self, reply: dict[str, object], expected_replica_id: str) -> bool:
        """Validate that the response header matches the expected request metadata."""
        return (
            reply.get("type") == "reply"
            and reply.get("client_id") == self.client_id
            and reply.get("replica_id") == expected_replica_id
            and reply.get("request_num") == self.request_num
        )
