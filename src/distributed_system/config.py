"""Host and port lookup for processes.

Public API:
    get_address(process_id) -> (host, port)
    resolve_address(process_id, host_override, port_override) -> (host, port)
"""

# Network Settings
import os

from dotenv import load_dotenv

# Load variables from .env file if it exists locally
_ = load_dotenv()

# Build the configuration using env vars, with sensible local fallbacks
PROCESS_CONFIG = {
    # Server Replicas
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
    # Local Fault Detectors
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

    # Infrastructure
    "GFD": {
        "host": os.getenv("GFD_HOST", "127.0.0.1"),
        "port": int(os.getenv("GFD_PORT", "9001")),
    },
    "RM": {
        "host": os.getenv("RM_HOST", "127.0.0.1"),
        "port": int(os.getenv("RM_PORT", "9002")),
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
