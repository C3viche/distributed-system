"""Host and port lookup for Milestone 1 processes.

Public API:
    get_address(process_id) -> (host, port)

Clients C1/C2/C3 are not listed: they only connect out to S1.
Change the S1 host here when moving off localhost onto the
non-sacred machine; callers keep using get_address("S1").
"""

PROCESS_CONFIG = {
    "S1": {
        "host": "127.0.0.1",
        "port": 7001,
    },
    "LFD1": {
        "host": "127.0.0.1",
        "port": 7002,
    },
}


def get_address(process_id):
    """
    Return (host, port) for a process.
    """
    if process_id not in PROCESS_CONFIG:
        raise ValueError(f"Unknown process id: {process_id}")

    config = PROCESS_CONFIG[process_id]

    return config["host"], config["port"]