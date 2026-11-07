"""Command-line configuration for GFD."""

import argparse
import math


def positive_number(value: str) -> float:
    try:
        num_value = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"'{value}' is not a valid number")

    if not math.isfinite(num_value) or num_value <= 0:
        raise argparse.ArgumentTypeError("must be a positive finite number")
    return num_value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the GFD fault detector.")
    _ = parser.add_argument(
        "--freq",
        "--heartbeat_freq",
        dest="frequency",
        type=positive_number,
        default=1.0,
        help="GFD→LFD heartbeats per second (Hz); 2 means one every 0.5 seconds",
    )
    _ = parser.add_argument(
        "--timeout",
        type=positive_number,
        default=2.0,
        help="seconds to wait for an LFD heartbeat ACK (default: 2)",
    )
    _ = parser.add_argument("--host", default=None, help="override GFD_HOST")
    _ = parser.add_argument("--port", type=int, default=None, help="override GFD_PORT")
    args = parser.parse_args()
    return args
