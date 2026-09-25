"""Client entry point for Milestone 1.

Usage:
    uv run client --id C1 --num_replicas 1
    uv run client --id C2 --num_replicas 1 --interval 0.5
    uv run client --id C3 --num_replicas 1 --count 5

Address resolution precedence for replicas:
    -> S1_HOST / S1_PORT environment variables (also loaded from .env)
    -> config.py defaults (127.0.0.1:8080)
"""

import argparse
from typing import cast

from distributed_system.client.client import Client
from distributed_system.config import SERVERS


def _positive_float(value: str) -> float:
    f = float(value)
    if f <= 0:
        raise argparse.ArgumentTypeError("must be > 0")
    return f


def _positive_int(value: str) -> int:
    i = int(value)
    if i <= 0:
        raise argparse.ArgumentTypeError("must be > 0")
    return i


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument(
        "--id",
        dest="client_id",
        required=True,
        help="Client identifier (C1, C2, C3, etc).",
    )
    _ = parser.add_argument(
        "--num_replicas",
        dest="num_replicas",
        type=_positive_int,
        choices=range(1, len(SERVERS) + 1),
        required=True,
        help="Specify the number of replicas the client will broadcast to"
    )
    _ = parser.add_argument(
        "--interval",
        type=_positive_float,
        default=1.0,
        help="Seconds between requests (default: 1.0).",
    )
    _ = parser.add_argument(
        "--count",
        type=_positive_int,
        default=None,
        help="Send this many requests then exit (default: run until Ctrl-C).",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    client_id = cast(str, args.client_id)
    num_replicas = cast(int | None, args.num_replicas)

    interval = cast(float, args.interval)
    count = cast(int | None, args.count)

    Client(
        client_id = client_id,
        num_replicas = num_replicas,
        interval = interval,
        count = count,
    ).run()
