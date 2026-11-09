"""Server replica for 18-749 (S1, S2, or S3).

RM admits replicas through join requests and confirms synchronization.
Active recovery pulls a point-in-time snapshot from the assigned donor before
becoming SYNCED. Request-log catch-up during concurrent traffic is not yet
implemented; recovery currently requires quiescent client traffic for consistency.

Active mode: every synchronized replica handles every request.
Passive mode: the configured primary opens a connection to each backup and
sends a checkpoint of my_state every 1/checkpoint_freq seconds. A backup
copies my_state and checkpoint_count from each checkpoint and does not answer
client requests. On primary crash, GFD/RM membership drops that replica; the
first remaining member is elected primary. A backup promotes, opens the
checkpoint channel to whoever is still alive, and starts answering clients.
A recovered former primary is appended to membership and comes back as a
backup. Mode defaults to REPLICATION_MODE (active if unset);
--replication-mode overrides it.

Registration with the paired LFD (S1→LFD1, S2→LFD2, S3→LFD3) uses connection
attempts bounded by a one-second timeout and is retried from the serve loop.
Client requests, LFD heartbeats, the backup channel and the checkpoint timer
all live in one select loop. No threads, timers, or randomness.

The listen socket binds 0.0.0.0 so other machines can connect. The port, and
the host printed at startup, come from config.


To Run:
    uv run server --id S1                         # shared config defaults
    uv run server --id S2 --checkpoint-freq 2     # frequency only matters on the primary
    uv run server --id S1 --replication-mode active
"""

import argparse
import math
import selectors
import socket
import time
from typing import cast

from distributed_system.common import BufferedJsonConnection, log, set_process_id
from distributed_system.config import (
    CHECKPOINT_FREQ,
    PRIMARY_ID,
    REPLICA_LFDS,
    REPLICATION_MODE,
    ReplicaStatus,
    get_address,
    get_server_addresses,
)
from distributed_system.server.election import backups_for, elect_primary


def lfd_id_for(replica_id: str) -> str:
    """Return the LFD that heartbeats this replica: S1→LFD1, S2→LFD2, S3→LFD3."""
    try:
        return REPLICA_LFDS[replica_id]
    except KeyError as exc:
        raise ValueError(f"No LFD mapping for replica id: {replica_id}") from exc


class Server:
    """One replica's lifecycle, application state, and asynchronous channels.

    Role (active/primary/backup) describes replication duties. ReplicaStatus
    describes readiness: UP waits for admission/state; SYNCED has usable state.
    Network availability and serving readiness are independent.
    """

    # Lifecycle: construction and the running event loop

    def __init__(
        self,
        replica_id: str,
        port_override: int | None = None,
        replication_mode: str = REPLICATION_MODE,
        primary_id: str = PRIMARY_ID,
        checkpoint_freq: float = CHECKPOINT_FREQ,
        replica_status: ReplicaStatus = ReplicaStatus.UP,
        bootstrap: bool = False,
    ):
        """Configure the replica and allocate local state without opening sockets.

        The default UP status requires RM initialization or a checkpoint before
        serving. An explicit replica_status is retained for existing callers.
        """
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
        if bootstrap and replica_id != primary_id:
            raise ValueError("Only the initial primary can bootstrap a fresh group")
        self.bootstrap = bootstrap
        self._recovery_id: str | None = None
        self._state_synced = False
        self.replica_status: ReplicaStatus = replica_status
        self.state: int = 0
        self._connections: dict[socket.socket, BufferedJsonConnection] = {}
        self._client_ids: dict[socket.socket, str] = {}
        self.sel: selectors.DefaultSelector = selectors.DefaultSelector()

        # Initial role follows PRIMARY_ID. Passive membership/promote_primary
        # from GFD/RM can later promote a backup or demote a recovered primary.
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
        self._rm: socket.socket | None = None
        self._rm_retry_at: float = 0.0
        self.recovery_source_id: str | None = None
        self._recovery: socket.socket | None = None
        self._recovery_retry_at: float = 0.0
        self._recovery_deadline: float | None = None
        self.membership: list[str] = []

    def serve(self) -> None:
        """Start networking, then advance admission/recovery and serving together.

        UP replicas still accept channels and answer heartbeats. Handlers decide
        which work is allowed by readiness and role; the event loop never blocks
        waiting for an RM instruction or a state transfer to finish.
        """
        self._start_listener()
        while True:
            self._maintain_connections()
            self._poll_connections()
            self._send_checkpoint_if_time()

    def _start_listener(self) -> None:
        """Expose the replica port and start its clock without marking it ready."""
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # lets us restart right after a ctrl-c instead of "address already in use"
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("0.0.0.0", self.port))
        listener.listen()
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

        # tag each socket so the loop knows what it's looking at
        _ = self.sel.register(listener, selectors.EVENT_READ, "listener")

        # start the checkpoint clock now that the server is actually running
        self._next_checkpoint = time.monotonic() + self._checkpoint_interval

    def _maintain_connections(self) -> None:
        """Retry LFD, checkpoint, and RM channels while preserving readiness.

        Registering with LFD reports liveness. Joining RM requests admission.
        Neither connection by itself authorizes client processing.
        """
        if self._lfd is None:
            self.try_connect_to_lfd()
        if self.role == "primary" and self._pending_backups:
            self.try_connect_to_backups()
        if self._rm is None:
            self.try_connect_to_rm()
        self.try_connect_to_recovery_source()

    # RM admission and recovery

    def handle_rm(self, msg: dict[str, object]) -> None:
        """Apply admission instructions and acknowledge only the current attempt."""
        instruction = msg.get("type")
        if instruction in {"membership", "promote_primary"}:
            self.handle_election(msg)
            return
        if instruction == "join_accepted":
            return
        if instruction == "wait":
            self.wait_for_admission()
            return
        token = msg.get("recovery_id")
        if instruction in {"recovery_cancelled", "recovery_rejected"}:
            if token == self._recovery_id:
                self.wait_for_admission()
            return
        if instruction == "ready":
            if isinstance(token, str) and token == self._recovery_id and self._state_synced:
                self.replica_status = ReplicaStatus.SYNCED
                self.bootstrap = False
                log(f"{self.replica_id}: RM confirmed readiness", kind="state")
            return
        if instruction not in {"initialize", "recover"}:
            raise ValueError("Invalid RM join instruction")
        if not isinstance(token, str) or not token:
            raise ValueError("Recovery instruction requires a token")
        if instruction == "recover":
            source = msg.get("source_id")
            if not isinstance(source, str) or source == self.replica_id or source not in get_server_addresses():
                raise ValueError("Invalid RM recovery source")
        if token == self._recovery_id:
            if self._state_synced:
                self._report_synced()
            return
        primary = msg.get("primary_id", self.primary_id)
        if self.replication_mode == "passive":
            if primary not in get_server_addresses():
                raise ValueError("Passive recovery requires a current primary")
            self.primary_id = primary
            # Membership instructions promote only synchronized backups.
            if primary != self.replica_id:
                self.role = "backup"
        self.wait_for_admission()
        self._recovery_id = token
        if instruction == "initialize":
            self.initialize_replica()
        else:
            self.prepare_recovery(msg.get("source_id"))

    def _report_synced(self) -> None:
        """Report installed state; serving waits for RM's ready acknowledgment."""
        self._state_synced = True
        self.recovery_source_id = None
        if self._rm is not None and self._recovery_id is not None:
            self._send_json(self._rm, {"type": "sync_complete",
                                      "replica_id": self.replica_id,
                                      "recovery_id": self._recovery_id})

    def handle_election(self, msg: dict[str, object]) -> None:
        """Apply a GFD/RM membership snapshot or explicit promote_primary."""
        members = self._parse_members(msg)
        if members is None:
            return
        if "primary_id" in msg:
            primary = msg["primary_id"]
            if primary is not None and primary not in members:
                return
            self.membership = list(members)
            if self.replication_mode == "passive" and primary is not None:
                if primary == self.replica_id:
                    self._become_primary(members)
                else:
                    self._become_backup(primary)
            return
        self.apply_membership(members)

    def apply_membership(self, members: list[str]) -> None:
        """Elect from membership order and promote or demote this replica."""
        self.membership = list(members)
        if self.replication_mode != "passive":
            return
        new_primary = elect_primary(members)
        if new_primary is None:
            return
        if new_primary == self.replica_id:
            self._become_primary(members)
        else:
            self._become_backup(new_primary)

    def _become_primary(self, members: list[str]) -> None:
        """Take the primary role and open checkpoint channels to remaining backups."""
        old_role = self.role
        if old_role != "primary" and self.replica_status != ReplicaStatus.SYNCED:
            log(f"{self.replica_id}: cannot promote before synchronization", kind="failure")
            return
        self.primary_id = self.replica_id
        if old_role != "primary":
            self.role = "primary"
            self.recovery_source_id = None
            if self._recovery is not None:
                self._close_sock(self._recovery, "promotion canceled recovery pull")
            self._next_checkpoint = time.monotonic() + self._checkpoint_interval
            self._close_inbound_primary_channel()
            log(f"{self.replica_id} role: {old_role} → primary", kind="membership")
            log(f"{self.replica_id} promoted to primary", kind="membership")
        self._sync_backup_targets(members)

    def _become_backup(self, new_primary: str) -> None:
        """Give up the primary role (or retarget) and wait for checkpoints."""
        old_role = self.role
        old_primary = self.primary_id
        if old_role == "primary":
            for sock in list(self._backups):
                self._close_sock(
                    sock, "demoted; closing checkpoint channel", retry_backup=False
                )
            self._pending_backups.clear()
        self.role = "backup"
        self.primary_id = new_primary
        if old_role == "primary":
            log(
                f"{self.replica_id} role: primary → backup; primary is {new_primary}",
                kind="membership",
            )
        elif old_primary != new_primary:
            log(
                f"{self.replica_id} (backup): primary is now {new_primary}",
                kind="membership",
            )

    def _sync_backup_targets(self, members: list[str]) -> None:
        """Keep outbound checkpoint channels aligned with alive backups."""
        desired = backups_for(self.replica_id, members)
        desired_set = set(desired)
        for sock, backup_id in list(self._backups.items()):
            if backup_id not in desired_set:
                self._close_sock(
                    sock, f"{backup_id} left membership", retry_backup=False
                )
        connected = set(self._backups.values())
        self._pending_backups = [
            backup_id
            for backup_id in self._pending_backups
            if backup_id in desired_set and backup_id not in connected
        ]
        pending_set = set(self._pending_backups)
        for backup_id in desired:
            if backup_id not in connected and backup_id not in pending_set:
                self._pending_backups.append(backup_id)
                pending_set.add(backup_id)

    def _close_inbound_primary_channel(self) -> None:
        """Drop the old primary's checkpoint socket after we are promoted."""
        for key in list(self.sel.get_map().values()):
            if key.data != "primary" or not isinstance(key.fileobj, socket.socket):
                continue
            self._close_sock(
                key.fileobj,
                "closing inbound checkpoint channel; this replica is now primary",
            )

    def _parse_members(self, msg: dict[str, object]) -> list[str] | None:
        raw_members = msg.get("members")
        if not isinstance(raw_members, list):
            return None
        members: list[str] = []
        seen: set[str] = set()
        for item in raw_members:
            if not isinstance(item, str) or item not in REPLICA_LFDS or item in seen:
                return None
            members.append(item)
            seen.add(item)
        count = msg.get("member_count")
        if count is not None and (type(count) is not int or count != len(members)):
            return None
        return members

    def initialize_replica(self) -> None:
        """Install initial state and report completion, awaiting RM acknowledgment.

        Construction already set state to zero. This does not reset state and
        must not be used as a replacement for recovering an existing group.
        """
        if self.replica_status != ReplicaStatus.UP:
            return
        if self.role == "backup" or self.recovery_source_id is not None:
            log(
                f"{self.replica_id}: initialization ignored; waiting for state synchronization"
            )
            return
        self._report_synced()
        log(f"{self.replica_id}: initial state installed; awaiting RM", kind="info")

    def prepare_recovery(self, source_id: object) -> None:
        """Assign a donor; the serve loop starts/retries active snapshot pulls.

        Passive backups keep receiving the configured primary's checkpoints.
        Replacing a donor cancels the previous channel before accepting new state.
        """
        if (
            not isinstance(source_id, str)
            or source_id == self.replica_id
            or source_id not in get_server_addresses()
        ):
            raise ValueError("Invalid RM recovery source")
        if self.replication_mode == "passive" and source_id != self.primary_id:
            raise ValueError("Passive recovery must use the configured primary")
        if self.replica_status != ReplicaStatus.UP:
            raise ValueError("Recovery requires an unsynchronized replica")
        if source_id == self.recovery_source_id:
            return
        if self._recovery is not None:
            self._close_sock(self._recovery, "Recovery donor reassigned")
        self.recovery_source_id = source_id
        self._recovery_retry_at = 0.0
        log(
            f"{self.replica_id}: RM selected recovery source {source_id}; waiting for state synchronization",
            kind="info",
        )

    def complete_recovery(self) -> None:
        """Report an installed active snapshot and wait for RM acknowledgment.

        The snapshot handler calls this only after validating and installing state.
        This establishes the donor's state at snapshot time; catching up with
        concurrent client requests still requires a request-log protocol.
        Passive backups report completion through checkpoint application instead.
        """
        if self.replication_mode != "active":
            raise ValueError("Active recovery completion requires active replication")
        if self.replica_status != ReplicaStatus.UP or self.recovery_source_id is None:
            raise ValueError("No active recovery in progress")
        source_id = self.recovery_source_id
        self._report_synced()
        log(
            f"{self.replica_id}: recovery from {source_id} complete; awaiting RM",
            kind="state",
        )

    def wait_for_admission(self) -> None:
        """Clear the pending donor while staying UP and available for heartbeats."""
        self.replica_status = ReplicaStatus.UP
        self._state_synced = False
        self._recovery_id = None
        self.recovery_source_id = None
        if self._recovery is not None:
            self._close_sock(self._recovery, "Recovery canceled by RM")
        log(f"{self.replica_id}: waiting for RM join instructions", kind="info")

    def try_connect_to_recovery_source(self) -> None:
        """Pull an active snapshot with a bounded connect and response deadline."""
        if (
            self.replication_mode != "active"
            or self.replica_status != ReplicaStatus.UP
            or self.recovery_source_id is None
        ):
            return
        now = time.monotonic()
        if self._recovery is not None:
            if self._recovery_deadline is not None and now >= self._recovery_deadline:
                self._close_sock(self._recovery, "Recovery snapshot timed out")
            return
        if now < self._recovery_retry_at:
            return
        try:
            sock = socket.create_connection(
                get_address(self.recovery_source_id), timeout=0.5
            )
        except OSError as exc:
            self._recovery_retry_at = time.monotonic() + 1.0
            log(
                f"Recovery donor {self.recovery_source_id} unavailable: {exc}; retrying",
                kind="failure",
            )
            return
        self._recovery = sock
        self._recovery_deadline = time.monotonic() + 2.0
        self._register(sock, "recovery")
        self._send_json(
            sock,
            {
                "type": "recovery_request",
                "replica_id": self.replica_id,
                "source_id": self.recovery_source_id,
            },
        )

    def handle_recovery_request(
        self, sock: socket.socket, msg: dict[str, object]
    ) -> None:
        """Answer an active replica's pull request while the donor is SYNCED."""
        requester = msg.get("replica_id")
        if (
            self.replication_mode != "active"
            or self.replica_status != ReplicaStatus.SYNCED
            or msg.get("type") != "recovery_request"
            or msg.get("source_id") != self.replica_id
            or not isinstance(requester, str)
            or requester == self.replica_id
            or requester not in get_server_addresses()
        ):
            raise ValueError("Invalid recovery request or unavailable donor")
        self._send_json(
            sock,
            {
                "type": "recovery_snapshot",
                "replica_id": self.replica_id,
                "target_id": requester,
                "my_state": self.state,
            },
        )
        log(
            f"{self.replica_id}: sending recovery snapshot to {requester}: my_state={self.state}",
            kind="checkpoint",
        )

    def handle_recovery_snapshot(
        self, sock: socket.socket, msg: dict[str, object]
    ) -> None:
        """Install a valid snapshot from this attempt's donor, then finish recovery."""
        state = msg.get("my_state")
        if (
            self.replication_mode != "active"
            or self.replica_status != ReplicaStatus.UP
            or self.recovery_source_id is None
            or sock is not self._recovery
            or msg.get("type") != "recovery_snapshot"
            or msg.get("replica_id") != self.recovery_source_id
            or msg.get("target_id") != self.replica_id
            or type(state) is not int
            or state < 0
        ):
            raise ValueError("Invalid recovery snapshot")
        self.state = state
        self.complete_recovery()
        self._close_sock(sock, "Recovery snapshot installed", kind="state")

    # State synchronization: passive checkpoint receive and publish

    def handle_checkpoint(self, msg: dict[str, object]) -> None:
        """Validate a checkpoint from the configured primary before applying it."""
        if (
            self.role != "backup"
            or msg.get("type") != "checkpoint"
            or msg.get("replica_id") != self.primary_id
        ):
            return
        my_state = msg.get("my_state")
        checkpoint_count = msg.get("checkpoint_count")
        if type(my_state) is not int or type(checkpoint_count) is not int:
            return

        self.apply_checkpoint(my_state, checkpoint_count)

    def apply_checkpoint(self, my_state: int, checkpoint_count: int) -> None:
        """Apply a checkpoint and report completion for an admitted recovery attempt.

        A synchronized backup still cannot serve clients: its role forbids it.
        This also handles subsequent periodic checkpoints while SYNCED.
        """
        if self.replica_status != ReplicaStatus.SYNCED and self._recovery_id is None:
            return
        previous_state = self.state
        self.state = my_state
        self.checkpoint_count = checkpoint_count
        if self.replica_status != ReplicaStatus.SYNCED:
            self._report_synced()

        log(
            f"{self.replica_id} applied checkpoint [{checkpoint_count}]: {self.primary_id}: my_state = {previous_state} -> {my_state}",
            kind="checkpoint",
        )

    def send_checkpoint(self) -> None:
        """Push my_state to every connected backup, print it, bump the count."""
        if self.role != "primary" or self.replica_status != ReplicaStatus.SYNCED:
            return
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
            self._send_json(sock, message)
            log(
                f"{self.replica_id} sending checkpoint [{self.checkpoint_count}] to {backup_id}: my_state={self.state}",
                kind="checkpoint",
            )
        self.checkpoint_count += 1

    # Serving: client admission, application mutation, and health checks

    def handle_client(self, sock: socket.socket, msg: dict[str, object]) -> None:
        """Gate client traffic by role/readiness, then execute and reply."""
        if msg.get("type") != "request":
            return

        client = msg.get("client_id")
        if isinstance(client, str) and client:
            self._client_ids[sock] = client

        if self.role == "backup":
            log(
                f"{self.replica_id} (backup): ignoring request from {client}",
                kind="info",
            )
            return

        # Check if the replica is actually synced before handling the client
        if self.replica_status != ReplicaStatus.SYNCED:
            log(
                f"{self.replica_id}: ignore request while {self.replica_status.name}",
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
        self._send_json(sock, reply)

    def handle_lfd(self, sock: socket.socket, msg: dict[str, object]) -> None:
        """Answer health checks in both UP and SYNCED states."""
        if msg.get("type") != "heartbeat":
            return

        count = msg.get("count")
        log(
            f"{self.replica_id} got heartbeat [{count}] from {self.lfd_id}",
            kind="heartbeat",
        )
        self._send_json(
            sock,
            {"type": "heartbeat_ack", "replica_id": self.replica_id, "count": count},
        )
        log(
            f"{self.replica_id} sent ack [{count}] back to {self.lfd_id}",
            kind="heartbeat",
        )

    # Channel establishment: LFD liveness, RM admission, and backups

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
            s = socket.create_connection((lfd_host, lfd_port), timeout=1.0)
        except OSError as exc:
            log(
                f"{self.replica_id} could not reach {self.lfd_id} ({exc}); retrying in 1s",
                kind="failure",
            )
            return

        self._lfd = s
        self._register(s, "lfd")
        self._send_json(s, {"type": "registration", "replica_id": self.replica_id})
        log(f"{self.replica_id} registered with {self.lfd_id}", kind="registration")

    def try_connect_to_rm(self) -> None:
        """Open the RM control channel and request instructions for joining.

        Like the client's GFD channel, this uses a bounded connection attempt,
        buffered asynchronous messages, and a one-second reconnect backoff.
        The RM owns membership decisions; this connection does not transfer state.
        Completion is reported on this connection after state installation.
        """
        if self._rm is not None or time.monotonic() < self._rm_retry_at:
            return
        rm_host, rm_port = get_address("RM")
        log(
            f"{self.replica_id} trying to connect to RM at {rm_host}:{rm_port}",
            kind="info",
        )
        try:
            # Short timeout bounds the TCP handshake if the RM is unreachable.
            s = socket.create_connection((rm_host, rm_port), timeout=1.0)
        except OSError as exc:
            log(
                f"{self.replica_id} could not reach RM ({exc}); retrying in 1s",
                kind="failure",
            )
            self._rm_retry_at = time.monotonic() + 1.0
            return

        self._rm = s
        self._register(s, "rm")
        # This request identifies the replica and asks the RM how it should join.
        self._send_json(
            s,
            {
                "type": "join_request",
                "replica_id": self.replica_id,
                "replication_mode": self.replication_mode,
                "status": self.replica_status.name,
                "bootstrap": self.bootstrap,
            },
        )
        log(
            f"{self.replica_id} requested join instructions from RM",
            kind="registration",
        )

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
            s = socket.create_connection((host, port), timeout=0.5)
        except OSError as exc:
            log(
                f"{self.replica_id} could not reach {backup_id} ({exc}); retrying",
                kind="failure",
            )
            self._pending_backups.append(backup_id)
            return

        self._backups[s] = backup_id
        self._register(s, "backup")
        # first message on the socket tells the backup this isn't a client
        self._send_json(s, {"type": "register_primary", "replica_id": self.replica_id})
        log(
            f"{self.replica_id} opened checkpoint channel to {backup_id}",
            kind="registration",
        )

    # Event scheduling and socket transport

    def _seconds_until_next_wakeup(self) -> float | None:
        """Wake for the earliest connection retry or checkpoint deadline."""
        waits: list[float] = []
        if self._lfd is None:
            waits.append(1.0)
        if self._rm is None:
            waits.append(max(0.0, self._rm_retry_at - time.monotonic()))
        if (
            self.replication_mode == "active"
            and self.replica_status == ReplicaStatus.UP
            and self.recovery_source_id is not None
        ):
            deadline = (
                self._recovery_deadline
                if self._recovery is not None
                else self._recovery_retry_at
            )
            if deadline is not None:
                waits.append(max(0.0, deadline - time.monotonic()))
        if self.role == "primary" and self._pending_backups:
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

    def _poll_connections(self) -> None:
        """Dispatch one batch of socket events, bounded by scheduled work."""
        for key, events in self.sel.select(timeout=self._seconds_until_next_wakeup()):
            if not isinstance(key.fileobj, socket.socket):
                continue
            sock = key.fileobj
            kind = cast(str, key.data)
            if kind == "listener":
                self._accept_connection(sock)
            else:
                self._handle_socket_event(sock, kind, events)

    def _accept_connection(self, listener: socket.socket) -> None:
        """Accept a peer; primary registration can later relabel its channel."""
        conn, client_addr = cast(
            tuple[socket.socket, tuple[str, int]], listener.accept()
        )
        self._register(conn, "client")
        log(
            f"new client connection from {client_addr[0]}:{client_addr[1]}",
            kind="registration",
        )

    def _handle_socket_event(self, sock: socket.socket, kind: str, events: int) -> None:
        """Read/flush one peer and isolate channel failures from the server loop."""
        try:
            if events & selectors.EVENT_READ:
                self._read_messages(sock, kind)
            if sock in self._connections and events & selectors.EVENT_WRITE:
                self._write_pending(sock)
        except BlockingIOError:
            pass  # Try again when the selector reports readiness.
        except (OSError, ValueError) as exc:
            self._close_sock(sock, f"Connection lost: {exc}")

    def _register(self, sock: socket.socket, kind: str) -> None:
        """Attach JSON buffers and a routing label to a connected socket."""
        self._connections[sock] = BufferedJsonConnection(sock)
        _ = self.sel.register(sock, selectors.EVENT_READ, kind)

    def _relabel_socket(self, sock: socket.socket, kind: str) -> None:
        """Change message routing without disturbing pending read/write events."""
        # every socket in the selector carries a label ("lfd", "client", "backup", "primary")
        # that _read_messages uses to route its messages. the primary connects to a backup
        # through the normal listen port, so it gets labelled "client" on accept. once
        # register_primary shows up, this swaps the label to "primary". selectors has no
        # "change label" call, only modify(), so the read/write mask is passed back unchanged.
        key = self.sel.get_key(sock)
        _ = self.sel.modify(sock, key.events, kind)

    def _send_json(self, sock: socket.socket, message: dict[str, object]) -> None:
        """Queue a message and request write readiness; never block on output."""
        self._connections[sock].queue(message)
        key = self.sel.get_key(sock)
        _ = self.sel.modify(
            sock, selectors.EVENT_READ | selectors.EVENT_WRITE, cast(str, key.data)
        )

    def _write_pending(self, sock: socket.socket) -> None:
        """Flush buffered output, preserving the socket's latest routing label."""
        connection = self._connections[sock]
        connection.flush()
        if not connection.has_pending_output:
            # look the label up again, a read on this same tick might have relabelled it
            _ = self.sel.modify(
                sock, selectors.EVENT_READ, cast(str, self.sel.get_key(sock).data)
            )

    def _read_messages(self, sock: socket.socket, kind: str) -> None:
        """Decode complete messages and route them to protocol handlers."""
        messages = self._connections[sock].receive()
        if messages is None:
            if kind == "lfd":
                why = f"lost connection to {self.lfd_id}"
            elif kind == "backup":
                # backup closed its end
                why = f"lost checkpoint channel to {self._backups.get(sock, '?')}"
            elif kind == "primary":
                why = "lost connection to primary"
            elif kind == "rm":
                why = "lost connection to replication manager"
            else:
                why = "client disconnected"
            self._close_sock(sock, why)
            return

        for raw_message in messages:
            if not isinstance(raw_message, dict):
                raise ValueError("Expected a JSON object")  # noqa: TRY004 - close malformed peer
            msg = cast(dict[str, object], raw_message)
            if kind == "lfd":
                self.handle_lfd(sock, msg)
            elif kind == "rm":
                self.handle_rm(msg)
            elif kind == "recovery":
                self.handle_recovery_snapshot(sock, msg)
            elif kind == "recovery_peer" or (
                kind == "client" and msg.get("type") == "recovery_request"
            ):
                self.handle_recovery_request(sock, msg)
                self._relabel_socket(sock, "recovery_peer")
                kind = "recovery_peer"
            elif kind == "backup":
                pass  # primary's outbound channel. backups don't send anything back
            elif self.role == "backup" and msg.get("type") == "register_primary":
                sender = msg.get("replica_id")
                if sender != self.primary_id:
                    log(
                        f"{self.replica_id} (backup): ignoring register_primary from {sender}",
                        kind="info",
                    )
                    continue
                # this accepted socket is the primary. later messages on it are checkpoints,
                # including one that arrived in this same read.
                self._relabel_socket(sock, "primary")
                kind = "primary"
                log(
                    f"{self.replica_id} (backup): checkpoint channel from {sender}",
                    kind="registration",
                )
            elif kind == "primary":
                self.handle_checkpoint(msg)
            else:
                self.handle_client(sock, msg)
            if sock not in self._connections:
                break

    def _close_sock(
        self,
        sock: socket.socket,
        why: str,
        kind: str = "failure",
        *,
        retry_backup: bool = True,
    ) -> None:
        """Remove a channel; losing RM revokes readiness until readmitted."""
        # have to unregister or select() keeps waking up on the dead socket
        _ = self.sel.unregister(sock)
        _ = self._connections.pop(sock, None)
        client_id = self._client_ids.pop(sock, None)
        backup_id = self._backups.pop(sock, None)
        sock.close()
        if sock is self._lfd:
            self._lfd = None  # serve loop will try to re-register
        elif sock is self._recovery:
            self._recovery = None
            self._recovery_deadline = None
            if (
                self.replica_status == ReplicaStatus.UP
                and self.recovery_source_id is not None
            ):
                self._recovery_retry_at = time.monotonic() + 1.0
                why = f"{why}; retrying recovery"
        elif sock is self._rm:
            self._rm = None
            self.wait_for_admission()
            self._rm_retry_at = time.monotonic() + 1.0
            why = "lost connection to RM; will retry"
        elif backup_id and retry_backup and self.role == "primary":
            # backup died, put it back in the queue so we reconnect if it comes back up
            self._pending_backups.append(backup_id)
            why = f"lost checkpoint channel to {backup_id}; will retry"
        elif backup_id:
            why = f"lost checkpoint channel to {backup_id}"
        elif client_id and "client disconnected" in why:
            why = f"{client_id} disconnected"
        elif client_id and why.startswith("Connection lost"):
            why = f"{client_id} disconnected ({why})"
        log(why, kind=kind)


def positive_float(text: str) -> float:
    value = float(text)
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError("must be finite and positive")
    return value


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    _ = parser.add_argument(
        "--id", choices=list(REPLICA_LFDS), required=True, help="Replica ID"
    )
    _ = parser.add_argument("--port", type=int, default=None, help="Override port")
    _ = parser.add_argument(
        "--replication-mode",
        choices=["active", "passive"],
        default=REPLICATION_MODE,
        help=f"Replication mode (default {REPLICATION_MODE})",
    )
    _ = parser.add_argument(
        "--primary",
        choices=list(REPLICA_LFDS),
        default=PRIMARY_ID,
        help=f"who the primary is in passive mode (default PRIMARY_ID={PRIMARY_ID})",
    )
    _ = parser.add_argument(
        "--checkpoint-freq",
        type=positive_float,
        default=CHECKPOINT_FREQ,
        help=f"checkpoints per second, primary only (default {CHECKPOINT_FREQ})",
    )

    parser.add_argument("--bootstrap", action="store_true",
                        help="initialize the first replica of a fresh group only")
    return parser.parse_args()


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
        bootstrap=args.bootstrap,
    )
    try:
        server.serve()
    except KeyboardInterrupt:
        log(f"{replica_id} shutting down", kind="failure")


if __name__ == "__main__":
    main()
