"""Host and port lookup for processes.

Public API:
    PRIMARY_ID: configured primary replica (default S1)
    CHECKPOINT_FREQ: checkpoints per second (default 1 Hz)
    get_address(process_id) -> (host, port)
    resolve_address(process_id, host_override, port_override) -> (host, port)
"""

# Network Settings
import math
import os
from typing import cast

from dotenv import load_dotenv

# Load variables from .env file if it exists locally
_ = load_dotenv()

# Dedicated mappings for each service component
SERVERS: dict[str, dict[str, str | int]] = {
    "S1": {
        "host": os.getenv("S1_HOST", "127.0.0.1"),
        "port": int(os.getenv("S1_PORT", "8080")),
    },
    "S2": {
        "host": os.getenv("S2_HOST", "127.0.0.1"),
        "port": int(os.getenv("S2_PORT", "8082")),
    },
    "S3": {
        "host": os.getenv("S3_HOST", "127.0.0.1"),
        "port": int(os.getenv("S3_PORT", "8083")),
    },
}

# Server/client CLI options can use these validated values as their defaults.
PRIMARY_ID = os.getenv("PRIMARY_ID", "S1")
if PRIMARY_ID not in SERVERS:
    raise ValueError(f"PRIMARY_ID must be one of {', '.join(SERVERS)}; got {PRIMARY_ID!r}")

try:
    CHECKPOINT_FREQ = float(os.getenv("CHECKPOINT_FREQ", "1"))
except ValueError as exc:
    raise ValueError("CHECKPOINT_FREQ must be a finite, positive number in Hz") from exc
if not math.isfinite(CHECKPOINT_FREQ) or CHECKPOINT_FREQ <= 0:
    raise ValueError("CHECKPOINT_FREQ must be a finite, positive number in Hz")

LFDS: dict[str, dict[str, str | int]] = {
    "LFD1": {
        "host": os.getenv("LFD1_HOST", "127.0.0.1"),
        "port": int(os.getenv("LFD1_PORT", "8081")),
    },
    "LFD2": {
        "host": os.getenv("LFD2_HOST", "127.0.0.1"),
        "port": int(os.getenv("LFD2_PORT", "8084")),
    },
    "LFD3": {
        "host": os.getenv("LFD3_HOST", "127.0.0.1"),
        "port": int(os.getenv("LFD3_PORT", "8085")),
    },
}

INFRASTRUCTURE: dict[str, dict[str, str | int]] = {
    "GFD": {
        "host": os.getenv("GFD_HOST", "127.0.0.1"),
        "port": int(os.getenv("GFD_PORT", "9001")),
    },
    "RM": {
        "host": os.getenv("RM_HOST", "127.0.0.1"),
        "port": int(os.getenv("RM_PORT", "9002")),
    },
}

REPLICA_LFDS = {"S1": "LFD1", "S2": "LFD2", "S3": "LFD3"}

# Unified single lookup dictionary
PROCESS_CONFIG = {**SERVERS, **LFDS, **INFRASTRUCTURE}

# Get all the server addresses from PROCESS_CONFIG and return as new dict
def get_server_addresses(num: int | None = None) -> dict[str, tuple[str, int]]:
    """Return a mapping of server replica IDs to their (host, port) tuples up to `num`."""
    # Convert SERVERS.items() into a list and slice the first `num` elements
    server_items = list(SERVERS.items())[:num] if num is not None else SERVERS.items()

    return {
        proc_id: (cast(str, cfg["host"]), cast(int, cfg["port"]))
        for proc_id, cfg in server_items
    }

# Helper function to get the address info. Will make dynamic later
def get_address(process_id: str) -> tuple[str, int]:
    """
    Return (host, port) for a process.
    """
    if process_id not in PROCESS_CONFIG:
        raise ValueError(f"Unknown process id: {process_id}")

    config = PROCESS_CONFIG[process_id]
    return str(config["host"]), int(config["port"])

# Resolves the address with CLI overrides
def resolve_address(
    process_id: str,
    host_override: str | None = None,
    port_override: int | None = None,
) -> tuple[str, int]:
    """Return (host, port) with CLI overrides taking precedence over environment values."""

    host, port = get_address(process_id)
    if host_override is not None:
        host = host_override
    if port_override is not None:
        port = int(port_override)
    return host, port
