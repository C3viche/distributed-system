"""Milestone 1 client for 18-749.

Runs as an independent process (C1, C2, or C3). Each client opens a single
long-lived TCP connection to S1 and sends tagged requests in a continuous
loop, incrementing its own request_num after each successful reply.

Console format matches server/LFD conventions in ``common.log``:
    Sent     <client_id, S1, N, payload>            (bold yellow)
    Received <client_id, S1, N, reply, state=X>     (yellow)

No coordination between C1/C2/C3. Each holds its own request_num.
"""

import selectors
import socket
import time
from typing import cast

from distributed_system.common import log, recv_json, send_json
from distributed_system.config import get_server_addresses


class Client:
    """One independent client process (C1, C2, or C3)."""

    def __init__(
        self,
        client_id: str,
        interval: float = 1.0,
        count: int | None = None,
        payload_template: str = "hello from {client_id} #{request_num}",
    ) -> None:
        """Initialize the client configuration and state."""
        self.client_id: str = client_id
        self.replicas: dict[str, tuple[str, int]] = get_server_addresses()
        self.interval: float = interval
        self.count: int | None = count  # None -> loop until Ctrl-C / server closes
        self.payload_template: str = payload_template
        self.request_num: int = 1

    # Public entry point
    def run(self) -> None:
        """Connect to the server replica and start the request/reply loop.

        Handles network exceptions and graceful shutdown on KeyboardInterrupt.
        """
        log(f"{self.client_id} starting", kind="info")

        # Open sockets to all replicas
        socks: dict[str, socket.socket] = {}
        for replica_id, (host, port) in self.replicas.items():
            try:
                sock = socket.create_connection((host, port))
                log(f"{self.client_id} connected to {replica_id} at {host}:{port}", kind="registration")
                socks[replica_id] = sock
            except OSError as exc:
                log(f"{self.client_id} could not connect to {replica_id} at {host}:{port}: {exc}", kind="failure")
                return

        if not socks: # Check if any were connected to
            log(f"{self.client_id} could not connect to any server replicas", kind="failure")
            return

        try:
            self._loop(socks)
        except KeyboardInterrupt:
            log(f"{self.client_id} shutting down", kind="info")
        finally:
            for s in socks.values():
                try:
                    s.close()
                except OSError:
                    pass

    # Internal functions
    def _loop(self, socks: dict[str, socket.socket]) -> None:
        """Execute the continuous request loop over the active socket connection."""
        sent = 0
        while self.count is None or sent < self.count:
            if not self._send_and_await_reply(socks):
                return
            sent += 1
            # Skip the sleep after the final send so --count exits promptly.
            if self.count is None or sent < self.count:
                time.sleep(self.interval)

    def _send_and_await_reply(self, socks: dict[str, socket.socket]) -> bool:
        """Construct, send a JSON request payload, and wait for the matching reply.

        Returns:
            bool: True if a valid matching reply was received, False on error or disconnect.
        """
        payload = self.payload_template.format(
            client_id=self.client_id,
            request_num=self.request_num,
        )

        # Broadcast request to all replica sockets
        for replica_id, sock in socks.items():
            request = {
                "type": "request",
                "client_id": self.client_id,
                "replica_id": replica_id,
                "request_num": self.request_num,
                "payload": payload,
            }
            log(f"Sent <{self.client_id}, {replica_id}, {self.request_num}, {payload}>", kind="send")
            try:
                send_json(sock, request)
            except OSError as exc:
                log(f"{self.client_id} send to {replica_id} failed: {exc}", kind="failure")

        # Use Selector to collect replies non-blockingly across open sockets
        sel = selectors.DefaultSelector()
        for replica_id, sock in socks.items():
            _ = sel.register(sock, selectors.EVENT_READ, data=replica_id)

        received_first_reply = False
        try:
            # Wait up to 2.0 seconds for responses to arrive on registered sockets
            events = sel.select(timeout=3.0)
            for key, _ in events:
                sock = cast(socket.socket, key.fileobj)
                replica_id = cast(str, key.data)

                reply = recv_json(sock)
                if reply is None or not self._reply_matches(reply, replica_id):
                    continue

                state = reply.get("state")

                if not received_first_reply:
                    # First reply triggers success for this request_num!
                    state = reply.get("state")
                    log(f"Received <{self.client_id}, {replica_id}, {self.request_num}, reply, state={state}>", kind="receive")
                    received_first_reply = True
                else:
                    # Remaining responses are flagged and logged as duplicate replies
                    log(f"request_num {self.request_num}: Discarded duplicate reply from {replica_id}", kind="info")
        finally:
            sel.close()

        if received_first_reply:
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
