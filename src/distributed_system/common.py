"""Shared socket framing and console logging for Milestone 1.

Public API can be called:
    send_json(sock, message)
    recv_json(sock)
    log(message, kind="info")

Wire format is one JSON object per line:
    {"type": "request", ...}\\n
"""

import json
import re
import socket
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import cast

from colored import attr, fg

# Color used for each kind of console message.
all_colors = {
    "send": "yellow",
    "receive": "yellow",
    "heartbeat": "grey_50",
    "state": "green",
    "registration": "blue",
    "failure": "red",
    "info": "white",
}

# Each process gets its own color so you can tell who is talking at a glance.
process_colors = {
    "S1": "cyan",
    "LFD1": "light_magenta",
    "C1": "light_blue",
    "C2": "orange_1",
    "C3": "light_green",
}

# Kinds that print dim (background noise) or bold (must not be missed).
_DIM_KINDS = {"heartbeat"}
_BOLD_KINDS = {"failure"}

_PROCESS_RE = re.compile(r"\b(" + "|".join(process_colors) + r")\b")
_TUPLE_RE = re.compile(r"<[^<>\n]*>")
_STATE_RE = re.compile(r"(my_state\s*=\s*)(-?\d+)")


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

def _line_style(kind: str) -> str:
    """ANSI prefix for a whole line of the given kind."""
    style = fg(all_colors.get(kind, all_colors["info"]))
    if kind in _DIM_KINDS:
        style += attr("dim")
    if kind in _BOLD_KINDS:
        style += attr("bold")
    return style


def _decorate(message: str, kind: str) -> str:
    """Highlight the interesting tokens in a message without changing its text.

    - rubric tuples like <C1, S1, 3, request> are bolded
    - my_state values are bolded
    - process IDs (S1, LFD1, C1, ...) get their own color

    Heartbeats are left alone so they stay muted.
    """
    if kind in _DIM_KINDS:
        return message

    base = _line_style(kind)
    bold = attr("bold")

    restore = f"{attr('reset')}{base}"

    def color_process(m: re.Match[str]) -> str:
        pid = m.group(1)
        return f"{fg(process_colors[pid])}{bold}{pid}{restore}"

    def bold_state(m: re.Match[str]) -> str:
        return f"{m.group(1)}{bold}{m.group(2)}{restore}"

    def bold_tuple(m: re.Match[str]) -> str:
        # Any reset emitted inside the tuple (by a colored process ID) must
        # re-enable bold so the rest of the tuple stays bold.
        inner = m.group(0).replace(restore, restore + bold)
        return f"{bold}{inner}{restore}"

    message = _PROCESS_RE.sub(color_process, message)
    message = _STATE_RE.sub(bold_state, message)
    message = _TUPLE_RE.sub(bold_tuple, message)
    return message


def _timestamp() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _emit(body: str, kind: str, timestamp: str) -> None:
    ts = f"{fg('dark_gray')}{attr('dim')}[{timestamp}]{attr('reset')}"
    print(f"{ts} {_line_style(kind)}{body}{attr('reset')}", flush=True)


def log(message: str, kind: str = "info") -> None:
    """
    Print a timestamped, color-coded console message.

    The timestamp is dimmed, the line is colored by kind, and process IDs,
    rubric tuples, and my_state values inside the message are emphasized.
    The visible text is never altered, so rubric lines stay greppable.

    Example:
        log("Sent <C1,S1,1,request>", kind="send")
        -> [2026-09-03 20:45:30] Sent <C1,S1,1,request>
    """
    _emit(_decorate(message, kind), kind, _timestamp())


def log_block(title: str, lines: Sequence[str] = (), kind: str = "info") -> None:
    """
    Print a boxed multi-line console event. Every line keeps its timestamp.

    Use for rare, important events (connections, registration, failures),
    not for per-heartbeat or per-request output.

    Example:
        log_block("new client connection", ["from 10.0.0.5:63687", "my_state = 2"])
        -> [ts] ┌─ new client connection ──────────
           [ts] │ from 10.0.0.5:63687
           [ts] │ my_state = 2
           [ts] └──────────────────────────────────
    """
    timestamp = _timestamp()
    heavy = kind in _BOLD_KINDS
    h, v, tl, bl = ("━", "┃", "┏", "┗") if heavy else ("─", "│", "┌", "└")

    width = max([len(title) + 4, *(len(line) + 2 for line in lines), 40])
    top = f"{tl}{h} {title} " + h * (width - len(title) - 3)
    bottom = bl + h * width

    _emit(_decorate(top, kind), kind, timestamp)
    for line in lines:
        _emit(_decorate(f"{v} {line}", kind), kind, timestamp)
    _emit(bottom, kind, timestamp)
