"""Shared socket framing and console logging for Milestone 1.

Public API can be called:
    send_json(sock, message)
    recv_json(sock)
    log(message, kind="info")

Wire format is one JSON object per line:
    {"type": "request", ...}\\n
"""

import json
import socket
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import cast

from colored import attr, fg

# Color used for each kind of console message.
all_colors = {
    "send": "yellow",
    "receive": "yellow",
    "heartbeat": "magenta",
    "state": "green",
    "registration": "blue",
    "failure": "red",
    "info": "white",
}


def send_json(sock: socket.socket, message: Mapping[str, object]) -> None:
    """
    Send one JSON message over a TCP socket.

    Every message ends with '\\n', so the receiver knows
    where one message ends. sendall() writes the full payload.
    """
    data = json.dumps(message) + "\n"
    sock.sendall(data.encode("utf-8"))


def recv_json(sock: socket.socket) -> dict[str, object] | None:
    """
    Receive one newline-delimited JSON message.

    Reads one byte at a time until '\\n' so a single recv() cannot
    split a message or glue two messages together.

    Returns:
        dict: decoded JSON message
        None: if the other side closed the connection cleanly
    """
    data = bytearray()

    while True:
        chunk = sock.recv(1)
        if not chunk:
            if not data:
                return None
            raise ConnectionError(
                "Connection closed before a complete message was received"
            )

        if chunk == b"\n":
            break
        data.extend(chunk)

    return cast(dict[str, object], json.loads(data.decode("utf-8")))


class BufferedJsonConnection:
    """JSON-line buffers for one nonblocking socket; the caller owns scheduling.

    Call receive/flush on read/write readiness. BlockingIOError and parsing
    errors propagate to the caller. Existing blocking helpers remain separate.
    """

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.sock.setblocking(False)
        self._incoming = bytearray()
        self._outgoing = bytearray()

    def receive(self) -> list[dict[str, object]] | None:
        """Return complete messages, [] for a partial line, or None on EOF."""
        chunk = self.sock.recv(4096)
        if not chunk:
            return None
        self._incoming.extend(chunk)
        messages = []
        while b"\n" in self._incoming:
            line, _, remainder = self._incoming.partition(b"\n")
            self._incoming[:] = remainder
            messages.append(cast(dict[str, object], json.loads(line.decode("utf-8"))))
        return messages

    def queue(self, message: Mapping[str, object]) -> None:
        self._outgoing.extend((json.dumps(message) + "\n").encode("utf-8"))

    @property
    def has_pending_output(self) -> bool:
        return bool(self._outgoing)

    def flush(self) -> None:
        """Attempt one send, retaining any bytes the socket cannot accept yet."""
        if not self._outgoing:
            return
        sent = self.sock.send(self._outgoing)
        if sent == 0:
            raise ConnectionError("connection closed while sending")
        del self._outgoing[:sent]

def log(message: str, kind: str = "info") -> None:
    """
    Print a timestamped, color-coded console message.

    Example:
        log("Sent <C1,S1,1,request>", kind="send")
        -> [2026-09-03 20:45:30] Sent <C1,S1,1,request>
    """
    timestamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    color = all_colors.get(kind, all_colors["info"])

    style = attr("bold") if kind == "send" else ""

    print(f"{style}{fg(color)}[{timestamp}] {message}{attr('reset')}", flush=True)
