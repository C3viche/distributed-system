"""Server lifecycle, service registration, and replication behavior.

transport.py owns socket buffers, selector events, and channel labels.
cli.py owns command-line parsing. This module decides how to handle client
requests, LFD heartbeats, and passive checkpoints.

Active replicas process client requests. In passive mode the configured primary
processes requests and sends periodic checkpoints; backups apply checkpoints
and ignore client requests. All channels share one selector loop.

Run with ``uv run server --id S1``; shared config supplies replication defaults.
"""

import math
import socket
import time
from typing import cast

from distributed_system.common import log, set_process_id
from distributed_system.config import (
    CHECKPOINT_FREQ,
    PRIMARY_ID,
    REPLICA_LFDS,
    REPLICATION_MODE,
    get_address,
    get_server_addresses,
)
from distributed_system.server.cli import parse_args as _parse_args
from distributed_system.server.transport import ServerTransport


def lfd_id_for(replica_id: str) -> str:
    """Return the LFD that heartbeats this replica: S1→LFD1, S2→LFD2, S3→LFD3."""
    try:
        return REPLICA_LFDS[replica_id]
    except KeyError as exc:
        raise ValueError(f"No LFD mapping for replica id: {replica_id}") from exc


class Server:
    """Coordinate application behavior through a dedicated socket transport."""

    # Lifecycle: construction and the running event loop

    def __init__(
        self,
        replica_id: str,
        port_override: int | None = None,
        replication_mode: str = REPLICATION_MODE,
        primary_id: str = PRIMARY_ID,
        checkpoint_freq: float = CHECKPOINT_FREQ,
    ):
        """Configure role, application state, and channels without opening sockets."""
        if replication_mode not in {"active", "passive"}:
            raise ValueError("replication_mode must be active or passive")
        if not math.isfinite(checkpoint_freq) or checkpoint_freq <= 0:
            raise ValueError("checkpoint_freq must be finite and positive")
        if primary_id not in REPLICA_LFDS:
            raise ValueError("primary_id must name a configured replica")

        self.replication_mode: str = replication_mode
        self.primary_id: str = primary_id
        self.replica_id: str = replica_id
        self.lfd_id: str = lfd_id_for(replica_id)
        self.state: int = 0
        self.transport: ServerTransport = ServerTransport(
            self.handle_message, self.connection_lost
        )

        # The configured mode and primary determine this replica's role.
        if replication_mode == "active":
            self.role: str = "active"  # M2: every replica handles every request
        elif replica_id == primary_id:
            self.role = "primary"  # M3: handles requests, sends checkpoints
        else:
            self.role = "backup"  # M3: ignores requests, receives checkpoints

        # Replication state and primary-only checkpoint scheduling.
        self.checkpoint_count: int = 1
        self._checkpoint_interval: float = (
            1.0 / checkpoint_freq
        )  # seconds between checkpoints
        self._next_checkpoint: float = 0.0  # set when serve() starts

        # other replicas: primary moves ids from _pending_backups to _backups (sock -> id)
        self._pending_backups: list[str] = []
        self._backups: dict[socket.socket, str] = {}
        self._next_backup_retry: float = 0.0  # don't redial more than once a second
        if self.role == "primary":
            self._pending_backups = [
                rid for rid in get_server_addresses() if rid != replica_id
            ]

        # Load host/port configuration. The listen socket still binds all interfaces.
        host, port = get_address(self.replica_id)

        self.host: str = host
        self.port = port

        if port_override:
            self.port: int = port_override

        # Outbound connection to the assigned LFD; None while we have no LFD.
        self._lfd: socket.socket | None = None

    def serve(self) -> None:
        """Listen, maintain service channels, dispatch messages, and checkpoint."""
        self._start_listener()
        while True:
            self._maintain_connections()
            self.transport.poll(self._seconds_until_next_wakeup())
            self._send_checkpoint_if_time()

    def _start_listener(self) -> None:
        """Expose the replica port and start the checkpoint clock."""
        self.transport.listen(self.port)
        log(
            f"{self.replica_id} starting in {self.replication_mode} mode"
            + (
                f", primary={self.primary_id}"
                if self.replication_mode == "passive"
                else ""
            )
            + f", role={self.role}; waiting for clients on {self.host}:{self.port}",
            kind="info",
        )

        # start the checkpoint clock now that the server is actually running
        self._next_checkpoint = time.monotonic() + self._checkpoint_interval

    def _maintain_connections(self) -> None:
        """Maintain LFD registration and the primary's backup channels."""
        if self._lfd is None:
            self.try_connect_to_lfd()
        if self._pending_backups:
            self.try_connect_to_backups()

    # State synchronization: passive checkpoint receive and publish

    def handle_checkpoint(self, msg: dict[str, object]) -> None:
        """Validate a checkpoint from the configured primary before applying it."""
        if msg.get("type") != "checkpoint" or msg.get("replica_id") != self.primary_id:
            return
        my_state = msg.get("my_state")
        checkpoint_count = msg.get("checkpoint_count")
        if type(my_state) is not int or type(checkpoint_count) is not int:
            return

        self.apply_checkpoint(my_state, checkpoint_count)

    def apply_checkpoint(self, my_state: int, checkpoint_count: int) -> None:
        """Install a validated passive checkpoint as the replica's local state."""
        self.state = my_state
        self.checkpoint_count = checkpoint_count
        log(
            f"{self.replica_id} received checkpoint from {self.primary_id}: my_state = {my_state}, checkpoint_count = {checkpoint_count}",
            kind="checkpoint",
        )

    def send_checkpoint(self) -> None:
        """Push my_state to every connected backup, print it, bump the count."""
        if not self._backups:
            # nobody to send to, does'nt bump the count
            log(
                f"{self.replica_id}: no backups connected, checkpoint [{self.checkpoint_count}] skipped",
                kind="checkpoint",
            )
            return
        message: dict[str, object] = {
            "type": "checkpoint",
            "replica_id": self.replica_id,
            "my_state": self.state,
            "checkpoint_count": self.checkpoint_count,
        }
        for sock, backup_id in list(self._backups.items()):
            self.transport.send(sock, message)
            log(
                f"{self.replica_id} sending checkpoint [{self.checkpoint_count}] to {backup_id}: my_state={self.state}",
                kind="checkpoint",
            )
        self.checkpoint_count += 1

    # Serving: client admission, application mutation, and health checks

    def handle_client(self, sock: socket.socket, msg: dict[str, object]) -> None:
        """Ignore backup requests; execute and reply on active replicas and the primary."""
        if msg.get("type") != "request":
            return

        client = msg.get("client_id")
        if isinstance(client, str) and client:
            self.transport.remember_client(sock, client)

        if self.role == "backup":
            log(
                f"{self.replica_id} (backup): ignoring request from {client}",
                kind="info",
            )
            return

        self.process_request(sock, msg)

    def process_request(self, sock: socket.socket, msg: dict[str, object]) -> None:
        """Increment application state and reply after handle_client admits a request.

        This is the application mutation point. Admission belongs to
        handle_client; repeated request numbers are not deduplicated.
        """
        client = msg.get("client_id")
        req = msg.get("request_num")
        payload = msg.get("payload", "")
        log(f"Received <{client}, {self.replica_id}, {req}, {payload}>", kind="receive")
        log(f"my_state = {self.state} before processing", kind="state")
        self.state += 1
        log(f"my_state = {self.state} after processing", kind="state")

        reply = {
            "type": "reply",
            "client_id": client,
            "replica_id": self.replica_id,
            "request_num": req,
            "state": self.state,
        }
        log(f"Sending <{client}, {self.replica_id}, {req}, reply>", kind="send")
        self.transport.send(sock, reply)

    def handle_lfd(self, sock: socket.socket, msg: dict[str, object]) -> None:
        """Answer heartbeat checks from the assigned LFD."""
        if msg.get("type") != "heartbeat":
            return

        count = msg.get("count")
        log(
            f"{self.replica_id} got heartbeat [{count}] from {self.lfd_id}",
            kind="heartbeat",
        )
        self.transport.send(
            sock,
            {"type": "heartbeat_ack", "replica_id": self.replica_id, "count": count},
        )
        log(
            f"{self.replica_id} sent ack [{count}] back to {self.lfd_id}",
            kind="heartbeat",
        )

    # Service connections: LFD registration and backup checkpoints

    def try_connect_to_lfd(self) -> None:
        """Attempt registration with the assigned LFD (up to one second).

        On failure the socket stays None and the serve loop retries on its
        next tick. On success the socket joins the selector so heartbeats
        are handled alongside client traffic.
        """
        lfd_host, lfd_port = get_address(self.lfd_id)
        log(
            f"{self.replica_id} trying to connect to {self.lfd_id} at {lfd_host}:{lfd_port}",
            kind="info",
        )
        try:
            # Short timeout bounds the TCP handshake if the LFD host is unreachable.
            s = self.transport.connect((lfd_host, lfd_port), "lfd", timeout=1.0)
        except OSError as exc:
            log(
                f"{self.replica_id} could not reach {self.lfd_id} ({exc}); retrying in 1s",
                kind="failure",
            )
            return

        self._lfd = s
        self.transport.send(s, {"type": "registration", "replica_id": self.replica_id})
        log(f"{self.replica_id} registered with {self.lfd_id}", kind="registration")

    def try_connect_to_backups(self) -> None:
        """Open the checkpoint channel to one backup. One try per second so a slow host
        can't stall the loop; a failed id goes to the back of the queue."""
        now = time.monotonic()
        if not self._pending_backups or now < self._next_backup_retry:
            return
        self._next_backup_retry = now + 1.0
        backup_id = self._pending_backups.pop(0)
        host, port = get_address(backup_id)
        log(
            f"{self.replica_id} trying to open checkpoint channel to {backup_id} at {host}:{port}",
            kind="info",
        )
        try:
            s = self.transport.connect((host, port), "backup", timeout=0.5)
        except OSError as exc:
            log(
                f"{self.replica_id} could not reach {backup_id} ({exc}); retrying",
                kind="failure",
            )
            self._pending_backups.append(backup_id)
            return

        self._backups[s] = backup_id
        # first message on the socket tells the backup this isn't a client
        self.transport.send(
            s, {"type": "register_primary", "replica_id": self.replica_id}
        )
        log(
            f"{self.replica_id} opened checkpoint channel to {backup_id}",
            kind="registration",
        )

    # Protocol routing and channel lifecycle callbacks

    def handle_message(
        self, sock: socket.socket, kind: str, msg: dict[str, object]
    ) -> str:
        """Dispatch a decoded message and return the channel's protocol label.

        Accepted peers begin as clients. A backup recognizes primary registration
        and routes subsequent messages on that channel to checkpoint handling.
        Socket framing and selector changes belong to ServerTransport.
        """
        if kind == "lfd":
            self.handle_lfd(sock, msg)
        elif kind == "backup":
            pass  # The primary's outbound checkpoint channel has no replies.
        elif self.role == "backup" and msg.get("type") == "register_primary":
            log(
                f"{self.replica_id} (backup): checkpoint channel from {msg.get('replica_id')}",
                kind="registration",
            )
            return "primary"
        elif kind == "primary":
            self.handle_checkpoint(msg)
        else:
            self.handle_client(sock, msg)
        return kind

    def connection_lost(self, sock: socket.socket, why: str) -> None:
        """Retry LFD registration or a backup channel after a disconnect."""
        backup_id = self._backups.pop(sock, None)
        if sock is self._lfd:
            self._lfd = None
            if why == "lost connection to LFD":
                why = f"lost connection to {self.lfd_id}"
        elif backup_id:
            self._pending_backups.append(backup_id)
            why = f"lost checkpoint channel to {backup_id}; will retry"
        log(why, kind="failure")

    def close(self) -> None:
        """Release network resources when this server process stops."""
        self.transport.close()

    # Deadlines for connection retries and checkpoint publication

    def _seconds_until_next_wakeup(self) -> float | None:
        """Wake for the earliest connection retry or checkpoint deadline."""
        waits: list[float] = []
        if self._lfd is None:
            waits.append(1.0)
        if self._pending_backups:
            waits.append(max(0.0, self._next_backup_retry - time.monotonic()))
        if self.role == "primary":
            waits.append(max(0.0, self._next_checkpoint - time.monotonic()))
        return min(waits) if waits else None

    def _send_checkpoint_if_time(self) -> None:
        """Run the primary's checkpoint timer without drifting its deadline."""
        now = time.monotonic()
        if self.role != "primary" or now < self._next_checkpoint:
            return
        self.send_checkpoint()
        # step from the old deadline not from now, otherwise the timer drifts
        while self._next_checkpoint <= now:
            self._next_checkpoint += self._checkpoint_interval


def main() -> None:
    args = _parse_args()
    replica_id = cast(str, args.id)
    set_process_id(replica_id)
    port_override = cast(int | None, args.port)

    server = Server(
        replica_id=replica_id,
        port_override=port_override,
        replication_mode=cast(str, args.replication_mode),
        primary_id=cast(str, args.primary),
        checkpoint_freq=cast(float, args.checkpoint_freq),
    )
    try:
        server.serve()
    except KeyboardInterrupt:
        log(f"{replica_id} shutting down", kind="failure")
    finally:
        server.close()


if __name__ == "__main__":
    main()
