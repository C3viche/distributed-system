"""Shared socket framing and console logging for Milestone 1.

Public API can be called:
    send_json(sock, message)
    recv_json(sock)
    log(message, kind="info")

Wire format is one JSON object per line:
    {"type": "request", ...}\\n
"""

import json
from datetime import datetime
from colored import fg, attr


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


def send_json(sock, message):
    """
    Send one JSON message over a TCP socket.

    Every message ends with '\\n', so the receiver knows
    where one message ends. sendall() writes the full payload.
    """
    data = json.dumps(message) + "\n"
    sock.sendall(data.encode("utf-8"))


def recv_json(sock):
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

    return json.loads(data.decode("utf-8"))


def log(message, kind="info"):
    """
    Print a timestamped, color-coded console message.

    Example:
        log("Sent <C1,S1,1,request>", kind="send")
        -> [2026-09-03 20:45:30] Sent <C1,S1,1,request>
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    color = all_colors.get(kind, all_colors["info"])

    style = attr("bold") if kind == "send" else ""

    print(f"{style}{fg(color)}[{timestamp}] {message}{attr('reset')}", flush=True)