"""Command-line configuration for LFD."""

import argparse
import math
from typing import cast

from distributed_system.config import REPLICA_LFDS
from distributed_system.lfd.sessions import REPLICA_ASSIGNMENTS


def positive_number(value: str) -> float:
    try:
        number = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"'{value}' is not a valid number")
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("must be a positive finite number")
    return number


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the LFD fault detector.")
    _ = parser.add_argument("--id", choices=list(REPLICA_ASSIGNMENTS), required=True)
    _ = parser.add_argument(
        "--replica-id",
        choices=list(REPLICA_LFDS),
        default=None,
        help="assigned replica (default: matching S1/S2/S3)",
    )
    _ = parser.add_argument(
        "--freq",
        "--heartbeat_freq",
        dest="frequency",
        type=positive_number,
        default=1.0,
        help="heartbeats per second (Hz)",
    )
    _ = parser.add_argument(
        "--timeout",
        type=positive_number,
        default=2.0,
        help="ACK/registration timeout in seconds",
    )
    args = parser.parse_args()
    lfd_id = cast(str, args.id)
    replica_id = cast(str | None, args.replica_id)
    if replica_id is not None and replica_id != REPLICA_ASSIGNMENTS[lfd_id]:
        parser.error(f"{lfd_id} must monitor {REPLICA_ASSIGNMENTS[lfd_id]}")
    return args
