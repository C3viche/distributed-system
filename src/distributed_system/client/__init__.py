"""Client entry point for Milestone 1.

Usage:
    uv run client --id C1
    uv run client --id C2 --interval 0.5
    uv run client --id C3 --count 5
    uv run client --id C1 --server-host 10.0.1.42 --server-port 8080

Address resolution precedence for S1:
    --server-host / --server-port CLI args
    -> S1_HOST / S1_PORT environment variables (also loaded from .env)
    -> config.py defaults (127.0.0.1:8080)
"""

import argparse

from distributed_system.client.client import Client
from distributed_system.config import resolve_address


CLIENT_CHOICES = ("C1", "C2", "C3")


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
    parser.add_argument(
        "--id",
        dest="client_id",
        required=True,
        choices=CLIENT_CHOICES,
        help="Client identifier (C1, C2, or C3).",
    )
    parser.add_argument(
        "--server-host",
        default=None,
        help="Override S1 host (else S1_HOST env / .env / config default).",
    )
    parser.add_argument(
        "--server-port",
        default=None,
        type=int,
        help="Override S1 port (else S1_PORT env / .env / config default).",
    )
    parser.add_argument(
        "--interval",
        type=_positive_float,
        default=1.0,
        help="Seconds between requests (default: 1.0).",
    )
    parser.add_argument(
        "--count",
        type=_positive_int,
        default=None,
        help="Send this many requests then exit (default: run until Ctrl-C).",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    host, port = resolve_address("S1", args.server_host, args.server_port)
    Client(
        client_id=args.client_id,
        server_host=host,
        server_port=port,
        interval=args.interval,
        count=args.count,
    ).run()
