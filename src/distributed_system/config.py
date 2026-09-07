"""Host and port lookup for Milestone 1 processes.

Public API:
    get_address(process_id) -> (host, port)

Clients C1/C2/C3 are not listed: they only connect out to S1.
Change the S1 host here when moving off localhost onto the
non-sacred machine; callers keep using get_address("S1").
"""

# Network Settings
import os

from dotenv import load_dotenv

# Load variables from .env file if it exists locally
_ = load_dotenv()

# Build the configuration using env vars, with sensible local fallbacks
PROCESS_CONFIG = {
    "S1": {
        "host": os.getenv("S1_HOST", "127.0.0.1"),
        "port": int(os.getenv("S1_PORT", "8080")),
    },
    "LFD1": {
        "host": os.getenv("LFD1_HOST", "127.0.0.1"),
        "port": int(os.getenv("LFD1_PORT", "8081")),
    },
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


def resolve_address(
    process_id: str,
    host_override: str | None = None,
    port_override: int | None = None,
) -> tuple[str, int]:
    """Return (host, port) with layered precedence.

    Order (highest wins):
        1. Explicit override args (typically from CLI flags)
        2. Environment variables (S1_HOST/S1_PORT, LFD1_HOST/LFD1_PORT, ...)
        3. Config defaults from ``PROCESS_CONFIG``

    Env vars are already folded into ``PROCESS_CONFIG`` at import time
    (including anything loaded from a local ``.env`` file), so callers
    only need to think about their explicit overrides.
    """
    host, port = get_address(process_id)
    if host_override is not None:
        host = host_override
    if port_override is not None:
        port = int(port_override)
    return host, port
