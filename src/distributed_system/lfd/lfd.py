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

class LocalFaultDetector:
    """Monitor one assigned replica through its incoming registration socket."""

    def __init__(
        self,
        lfd_id: str = "LFD1",
        replica_id: str = "S1",
        frequency: float = 1.0,
        timeout: float = 2.0,
    ) -> None:
        self.lfd_id: str = lfd_id
        self.replica_id: str = replica_id
        self.interval: float = 1.0 / frequency
        self.timeout: float = timeout
        self.host, self.port = get_address(lfd_id)
        # Count from 1 for this LFD's lifetime, including server reconnects.
        self._counts: itertools.count[int] = itertools.count(1)

    def monitor(self, server: socket.socket) -> None:
        server.settimeout(self.timeout)

        for heartbeat_count in self._counts:
            started = time.monotonic()
            try:
                log(
                    f"[{heartbeat_count}] {self.lfd_id} sending heartbeat to {self.replica_id}",
                    kind="heartbeat",
                )
                send_json(server, { # The heartbeat message
                    "type": "heartbeat",
                    "lfd_id": self.lfd_id,
                    "count": heartbeat_count,
                })
                ack = recv_json(server)
                if ack is None:
                    raise ConnectionError("Server disconnected.")
                if ( # IMPORTANT: This is an invalid heartbeat ack, because each lfd is assigned to each server. Must be accurate
                    ack.get("type") != "heartbeat_ack"
                    or ack.get("replica_id") != self.replica_id
                    or ack.get("count") != heartbeat_count
                ):
                    log("Invalid heartbeat ACK; closing connection", kind="failure")
                    return
                log(
                    f"[{heartbeat_count}] {self.lfd_id} receives heartbeat from {self.replica_id}",
                    kind="heartbeat",
                )
            except (OSError, ConnectionError):
                log(f"{self.replica_id} has died", kind="failure")
                return

            elapsed = time.monotonic() - started
            time.sleep(max(0, self.interval - elapsed))

    def run(self) -> None:
        # Opens a connection to listen for server connection
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((self.host, self.port))
            listener.listen()
            log(f"{self.lfd_id} listening on {self.host}:{self.port}", kind="info")

            # Receive heartbeat acks from server until it stops or fails
            while True:
                server, _ = cast(tuple[socket.socket, tuple[str, int]], listener.accept()) # connects with server
                with server:
                    server.settimeout(self.timeout)
                    try:
                        registration = recv_json(server)
                        if (
                            not isinstance(registration, dict)
                            or registration.get("type") != "registration"
                            or registration.get("replica_id") != self.replica_id
                        ):
                            log("Invalid server registration", kind="failure")
                            continue
                        log(f"{self.replica_id} registered with {self.lfd_id}", kind="registration")
                        self.monitor(server)
                    except (OSError, ConnectionError, ValueError) as exc:
                        log(f"{self.lfd_id} connection error: {exc}", kind="failure")


# Parse CLI arguments and create one LFD process.
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument(
        "--id", choices=["LFD1"], default="LFD1",
        help="local fault detector ID (Milestone 1 supports LFD1)",
    )
    _ = parser.add_argument(
        "--replica-id", default="S1",
        help="ID of the server replica assigned to this LFD (default: S1)",
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

    detector = LocalFaultDetector(
        lfd_id=lfd_id, replica_id=cast(str, args.replica_id),
        frequency=freq, timeout=timeout,
    )
    try:
        detector.run()
    except KeyboardInterrupt:
        log(f"{lfd_id} shutting down", kind="info")


if __name__ == "__main__":
    main()
