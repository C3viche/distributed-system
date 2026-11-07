"""Command-line configuration for the server process."""

import argparse
import math

from distributed_system.config import (
    CHECKPOINT_FREQ,
    PRIMARY_ID,
    REPLICA_LFDS,
    REPLICATION_MODE,
)


def positive_float(text: str) -> float:
    value = float(text)
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError("must be finite and positive")
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    _ = parser.add_argument(
        "--id", choices=list(REPLICA_LFDS), required=True, help="Replica ID"
    )
    _ = parser.add_argument("--port", type=int, default=None, help="Override port")
    _ = parser.add_argument(
        "--replication-mode",
        choices=["active", "passive"],
        default=REPLICATION_MODE,
        help=f"Replication mode (default {REPLICATION_MODE})",
    )
    _ = parser.add_argument(
        "--primary",
        choices=list(REPLICA_LFDS),
        default=PRIMARY_ID,
        help=f"who the primary is in passive mode (default PRIMARY_ID={PRIMARY_ID})",
    )
    _ = parser.add_argument(
        "--checkpoint-freq",
        type=positive_float,
        default=CHECKPOINT_FREQ,
        help=f"checkpoints per second, primary only (default {CHECKPOINT_FREQ})",
    )

    return parser.parse_args()
