"""Primary election for passive replication.

RM chooses a synchronized primary in membership order and sends it explicitly.
The order-only helper also supports callers with a prefiltered eligible list.
Members are appended as they appear and removed on crash, so a recovered
replica is added at the end and comes back as a backup rather than reclaiming
the primary role.
"""

from distributed_system.config import REPLICA_LFDS


def elect_primary(members: list[str]) -> str | None:
    """Return the next primary, or None if the group is empty."""
    for replica_id in members:
        if replica_id in REPLICA_LFDS:
            return replica_id
    return None


def backups_for(primary_id: str, members: list[str]) -> list[str]:
    """Alive replicas that should receive checkpoints from the primary."""
    return [
        replica_id
        for replica_id in members
        if replica_id != primary_id and replica_id in REPLICA_LFDS
    ]
