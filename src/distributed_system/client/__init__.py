"""Client entry point for active and passive replication.

Usage:
    uv run client --id C1 --num_replicas 1
    uv run client --id C2 --num_replicas 1 --interval 0.5
    uv run client --id C3 --num_replicas 1 --count 5

The GFD supplies live replica IDs; config.py resolves their host/port values.
"""

import argparse
from typing import cast

from distributed_system.client.client import Client
from distributed_system.common import set_process_id


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
        default=None,
        help="Legacy option; GFD membership determines the active replicas",
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
        help="Issue this many requests, including unanswered ones, then exit (default: until Ctrl-C).",
    )
    _ = parser.add_argument(
        "--replication-mode",
        choices=("active", "passive"),
        default="active",
        help="Broadcast to all replicas or send only to the assigned primary (default: active).",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    client_id = cast(str, args.client_id)
    set_process_id(client_id)
    num_replicas = cast(int | None, args.num_replicas)

    interval = cast(float, args.interval)
    count = cast(int | None, args.count)

    # Parse arguments for client id, num replicas, etc
    # Most importantly, now we support a "replication mode" which
    # gets passed along to the client object
    Client(
        client_id = client_id,
        num_replicas = num_replicas,
        interval = interval,
        count = count,
        replication_mode = cast(str, args.replication_mode),
    ).run()
