"""Milestone 1 local fault detector for S1.

Run: python lfd.py --heartbeat_freq 3 --timeout 2
heartbeat_freq is the interval between heartbeat starts, in seconds.
"""

import argparse
import itertools
import math
import socket
import time

from common import log, recv_json, send_json
from config import get_address


def positive_seconds(value):
    value = float(value)
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError("must be a positive finite number")
    return value


def monitor(server, interval, timeout, counts):
    server.settimeout(timeout)

    for heartbeat_count in counts:
        started = time.monotonic()
        try:
            log(
                f"[{heartbeat_count}] LFD1 sending heartbeat to S1",
                kind="heartbeat",
            )
            send_json(server, {
                "type": "heartbeat",
                "lfd_id": "LFD1",
                "count": heartbeat_count,
            })
            ack = recv_json(server)
            if ack is None:
                raise ConnectionError("server disconnected")
            if (
                not isinstance(ack, dict)
                or ack.get("type") != "heartbeat_ack"
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


def run(interval, timeout):
    host, port = get_address("LFD1")
    # Keep numbering across registrations for the lifetime of this LFD.
    counts = itertools.count(1)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((host, port))
        listener.listen()
        log(f"LFD1 listening on {host}:{port}", kind="info")

        while True:
            server, _ = listener.accept()
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--heartbeat_freq", type=positive_seconds, required=True,
        help="seconds between heartbeats",
    )
    parser.add_argument(
        "--timeout", type=positive_seconds, default=2.0,
        help="socket timeout in seconds (default: 2)",
    )
    args = parser.parse_args()
    try:
        run(args.heartbeat_freq, args.timeout)
    except KeyboardInterrupt:
        log("LFD1 shutting down", kind="info")
