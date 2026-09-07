"""Milestone 1 client for 18-749.

Runs as an independent process (C1, C2, or C3). Each client opens a single
long-lived TCP connection to S1 and sends tagged requests in a continuous
loop, incrementing its own request_num after each successful reply.

Console format matches server/LFD conventions in ``common.log``:
    Sent     <client_id, S1, N, payload>            (bold yellow)
    Received <client_id, S1, N, reply, state=X>     (yellow)

No coordination between C1/C2/C3. Each holds its own request_num.
"""

import socket
import time

from distributed_system.common import log, recv_json, send_json


SERVER_ID = "S1"


class Client:
    """One independent client process (C1, C2, or C3)."""

    def __init__(
        self,
        client_id: str,
        server_host: str,
        server_port: int,
        interval: float = 1.0,
        count: int | None = None,
        payload_template: str = "hello from {client_id} #{request_num}",
    ):
        self.client_id = client_id
        self.server_host = server_host
        self.server_port = server_port
        self.interval = interval
        self.count = count  # None -> loop until Ctrl-C / server closes
        self.payload_template = payload_template
        self.request_num = 1

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------
    def run(self) -> None:
        log(f"{self.client_id} starting", kind="info")
        try:
            sock = socket.create_connection((self.server_host, self.server_port))
        except OSError as exc:
            log(
                f"{self.client_id} could not connect to {SERVER_ID} at "
                f"{self.server_host}:{self.server_port}: {exc}",
                kind="failure",
            )
            return

        log(
            f"{self.client_id} connected to {SERVER_ID} at "
            f"{self.server_host}:{self.server_port}",
            kind="registration",
        )

        try:
            self._loop(sock)
        except KeyboardInterrupt:
            log(f"{self.client_id} shutting down", kind="info")
        finally:
            try:
                sock.close()
            except OSError:
                pass

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------
    def _loop(self, sock: socket.socket) -> None:
        sent = 0
        while self.count is None or sent < self.count:
            if not self._send_and_await_reply(sock):
                return
            sent += 1
            # Skip the sleep after the final send so --count exits promptly.
            if self.count is None or sent < self.count:
                time.sleep(self.interval)

    def _send_and_await_reply(self, sock: socket.socket) -> bool:
        payload = self.payload_template.format(
            client_id=self.client_id,
            request_num=self.request_num,
        )
        request = {
            "type": "request",
            "client_id": self.client_id,
            "server_id": SERVER_ID,
            "request_num": self.request_num,
            "payload": payload,
        }

        # ---- send ----
        log(
            f"Sent <{self.client_id}, {SERVER_ID}, {self.request_num}, {payload}>",
            kind="send",
        )
        try:
            send_json(sock, request)
        except OSError as exc:
            log(f"{self.client_id} send failed: {exc}", kind="failure")
            return False

        # ---- receive ----
        try:
            reply = recv_json(sock)
        except (OSError, ConnectionError, ValueError) as exc:
            log(f"{self.client_id} recv failed: {exc}", kind="failure")
            return False

        if reply is None:
            log(
                f"{SERVER_ID} closed connection before replying to "
                f"request {self.request_num}",
                kind="failure",
            )
            return False

        if not self._reply_matches(reply):
            log(
                f"{self.client_id} got unexpected reply "
                f"(want request_num={self.request_num}): {reply}",
                kind="failure",
            )
            return False

        state = reply.get("state")
        log(
            f"Received <{self.client_id}, {SERVER_ID}, "
            f"{self.request_num}, reply, state={state}>",
            kind="receive",
        )
        self.request_num += 1
        return True

    def _reply_matches(self, reply: dict) -> bool:
        return (
            reply.get("type") == "reply"
            and reply.get("client_id") == self.client_id
            and reply.get("request_num") == self.request_num
        )
