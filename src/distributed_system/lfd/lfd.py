"""Milestone 1 local fault detector for S1.

Run from the repository root:
    uv run lfd --id LFD1 --freq 2
Frequency is measured in heartbeats per second (Hz).
"""

import argparse
import itertools
import math
import socket
import time
from collections.abc import Iterator
from typing import cast

from distributed_system.common import log, recv_json, send_json
from distributed_system.config import get_address


# Transforms stringed number input into float
def positive_number(value: str) -> float:
    try:
        num_value = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"'{value}' is not a valid number")

    if not math.isfinite(num_value) or num_value <= 0:
        raise argparse.ArgumentTypeError("must be a positive finite number")
    return num_value

# The main function that counts and sends hearbeats
def monitor(
    server: socket.socket, 
    interval: float, 
    timeout: float, 
    counts: Iterator[int]
    ) -> None:
    server.settimeout(timeout)

    for heartbeat_count in counts:
        started = time.monotonic()
        try:
            log(
                f"[{heartbeat_count}] LFD1 sending heartbeat to S1",
                kind="heartbeat",
            )
            send_json(server, { # The heartbeat message
                "type": "heartbeat",
                "lfd_id": "LFD1",
                "count": heartbeat_count,
            })
            ack = recv_json(server)
            if ack is None:
                raise ConnectionError("Server disconnected.")
            if ( # IMPORTANT: This is an invalid heartbeat ack, because each lfd is assigned to each server. Must be accurate
                ack.get("type") != "heartbeat_ack"
                or ack.get("replica_id") != "S1"
                or ack.get("count") != heartbeat_count
            ):
                log("Invalid heartbeat ACK; closing connection", kind="failure")
                return
            log(
                f"[{heartbeat_count}] LFD1 receives heartbeat from S1",
                kind="heartbeat",
            )
        except (OSError, ConnectionError):
            log("S1 has died", kind="failure")
            return

        elapsed = time.monotonic() - started
        time.sleep(max(0, interval - elapsed))


# Runs the lfd server
def run(frequency: float, timeout: float, lfd_id: str = "LFD1") -> None:
    interval = 1.0 / frequency
    host, port = get_address(lfd_id)
    # Keep numbering across registrations for the lifetime of this LFD.
    counts = itertools.count(1)

    # Opens a connection to listen for server connection
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((host, port))
        listener.listen()
        log(f"LFD1 listening on {host}:{port}", kind="info")

        # Receive heartbeat acks from server until it stops or fails
        while True:
            server, _ = cast(tuple[socket.socket, tuple[str, int]], listener.accept()) # connects with server
            with server:
                server.settimeout(timeout)
                try:
                    registration = recv_json(server)
                    if (
                        not isinstance(registration, dict)
                        or registration.get("type") != "registration"
                        or registration.get("replica_id") != "S1"
                    ):
                        log("Invalid server registration", kind="failure")
                        continue
                    log("S1 registered with LFD1", kind="registration")
                    monitor(server, interval, timeout, counts)
                except (OSError, ConnectionError, ValueError) as exc:
                    log(f"LFD1 connection error: {exc}", kind="failure")

# The arg parsing logic with default values but three options and runnable via uv
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument(
        "--id", choices=["LFD1"], default="LFD1",
        help="local fault detector ID (Milestone 1 supports LFD1)",
    )
    _ = parser.add_argument(
        "--freq", "--heartbeat_freq", dest="frequency",
        type=positive_number, default=1.0,
        help="heartbeats per second (Hz); 2 means one every 0.5 seconds",
    )
    _ = parser.add_argument(
        "--timeout", type=positive_number, default=2.0,
        help="socket timeout in seconds (default: 2)",
    )
    args = parser.parse_args()

    freq = cast(float, args.frequency)
    timeout = cast(float, args.timeout)
    lfd_id = cast(str, args.id)

    try:
        run(freq, timeout, lfd_id)
    except KeyboardInterrupt:
        log("LFD1 shutting down", kind="info")


if __name__ == "__main__":
    main()
