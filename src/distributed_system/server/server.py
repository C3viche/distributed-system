"""Server replica for 18-749 (S1, S2, or S3).

Active mode: every replica handles every request.
Passive mode: the configured primary opens a connection to each backup and
sends a checkpoint of my_state every 1/checkpoint_freq seconds. Backup request
suppression and checkpoint application are still unfinished. Mode defaults to
REPLICATION_MODE (active if unset); --replication-mode overrides it.

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
    get_address,
    get_server_addresses,
)


def lfd_id_for(replica_id: str) -> str:
    """Return the LFD that heartbeats this replica: S1→LFD1, S2→LFD2, S3→LFD3."""
    try:
        return REPLICA_LFDS[replica_id]
    except KeyError as exc:
        raise ValueError(f"No LFD mapping for replica id: {replica_id}") from exc


class Server:
    def __init__(
        self,
        replica_id: str,
        port_override: int | None = None,
        replication_mode: str = REPLICATION_MODE,
        primary_id: str = PRIMARY_ID,
        checkpoint_freq: float = CHECKPOINT_FREQ,
    ):
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
        self._connections: dict[socket.socket, BufferedJsonConnection] = {}
        self._client_ids: dict[socket.socket, str] = {}
        self.sel: selectors.DefaultSelector = selectors.DefaultSelector()

        # here we define the type of server (role)
        if replication_mode == "active":
            self.role: str = "active"  # M2: every replica handles every request
        elif replica_id == primary_id:
            self.role = "primary"  # M3: handles requests, sends checkpoints
        else:
            self.role = "backup"  # M3: ignores requests, receives checkpoints

        # this section controls the checkpoint timer, only the primary uses it
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
            self._send_json(sock, message)
            log(
                f"{self.replica_id} sending checkpoint [{self.checkpoint_count}] to {backup_id}: my_state={self.state}",
                kind="checkpoint",
            )
        self.checkpoint_count += 1

    def _seconds_until_next_wakeup(self) -> float | None:
        # no threads, so the select() timeout is the only clock. 1s while retrying a
        # connection, until the next checkpoint on the primary, None (forever) otherwise
        waits: list[float] = []
        if self._lfd is None:
            waits.append(1.0)
        if self._pending_backups:
            waits.append(max(0.0, self._next_backup_retry - time.monotonic()))
        if self.role == "primary":
            waits.append(max(0.0, self._next_checkpoint - time.monotonic()))
        return min(waits) if waits else None

    def _send_checkpoint_if_time(self) -> None:
        # the other half of the timer: runs once per loop, after select() wakes up
        now = time.monotonic()
        if self.role != "primary" or now < self._next_checkpoint:
            return
        self.send_checkpoint()
        # step from the old deadline not from now, otherwise the timer drifts
        while self._next_checkpoint <= now:
            self._next_checkpoint += self._checkpoint_interval

    def handle_lfd(self, sock: socket.socket, msg: dict[str, object]) -> None:
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

    def handle_client(self, sock: socket.socket, msg: dict[str, object]) -> None:
        if msg.get("type") != "request":
            return

        client = msg.get("client_id")
        if isinstance(client, str) and client:
            self._client_ids[sock] = client
        req = msg.get("request_num")
        payload = msg.get("payload", "")

        # sonia: if self.role == "backup", print the ignore line and return

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

    def _register(self, sock: socket.socket, kind: str) -> None:
        self._connections[sock] = BufferedJsonConnection(sock)
        _ = self.sel.register(sock, selectors.EVENT_READ, kind)

    def _relabel_socket(self, sock: socket.socket, kind: str) -> None:
        # every socket in the selector carries a label ("lfd", "client", "backup", "primary")
        # that _read_messages uses to route its messages. the primary connects to a backup
        # through the normal listen port, so it gets labelled "client" on accept. once
        # register_primary shows up, this swaps the label to "primary". selectors has no
        # "change label" call, only modify(), so the read/write mask is passed back unchanged.
        key = self.sel.get_key(sock)
        _ = self.sel.modify(sock, key.events, kind)

    def _send_json(self, sock: socket.socket, message: dict[str, object]) -> None:
        self._connections[sock].queue(message)
        key = self.sel.get_key(sock)
        _ = self.sel.modify(
            sock, selectors.EVENT_READ | selectors.EVENT_WRITE, cast(str, key.data)
        )

    def _write_pending(self, sock: socket.socket) -> None:
        connection = self._connections[sock]
        connection.flush()
        if not connection.has_pending_output:
            # look the label up again, a read on this same tick might have relabelled it
            _ = self.sel.modify(
                sock, selectors.EVENT_READ, cast(str, self.sel.get_key(sock).data)
            )

    def _read_messages(self, sock: socket.socket, kind: str) -> None:
        messages = self._connections[sock].receive()
        if messages is None:
            if kind == "lfd":
                why = f"lost connection to {self.lfd_id}"
            elif kind == "backup":
                # backup closed its end
                why = f"lost checkpoint channel to {self._backups.get(sock, '?')}"
            elif kind == "primary":
                why = "lost connection to primary"
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
            elif kind == "backup":
                pass  # primary's outbound channel. backups don't send anything back
            elif kind == "primary":
                pass  # sonia: handle_checkpoint(msg) goes here
            elif msg.get("type") == "register_primary":
                # backup side: this 'client' is actually the primary, route its messages as checkpoints from now on
                self._relabel_socket(sock, "primary")
                log(
                    f"{self.replica_id} (backup): checkpoint channel from {msg.get('replica_id')}",
                    kind="registration",
                )
            else:
                self.handle_client(sock, msg)

    def _close_sock(self, sock: socket.socket, why: str):
        # have to unregister or select() keeps waking up on the dead socket
        _ = self.sel.unregister(sock)
        _ = self._connections.pop(sock, None)
        client_id = self._client_ids.pop(sock, None)
        backup_id = self._backups.pop(sock, None)
        sock.close()
        if sock is self._lfd:
            self._lfd = None  # serve loop will try to re-register
        elif backup_id:
            # backup died, put it back in the queue so we reconnect if it comes back up
            self._pending_backups.append(backup_id)
            why = f"lost checkpoint channel to {backup_id}; will retry"
        elif client_id and "client disconnected" in why:
            why = f"{client_id} disconnected"
        elif client_id and why.startswith("Connection lost"):
            why = f"{client_id} disconnected ({why})"
        log(why, kind="failure")

    def serve(self):
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

        while True:
            if self._lfd is None:
                self.try_connect_to_lfd()
            if self._pending_backups:
                self.try_connect_to_backups()

            for key, events in self.sel.select(
                timeout=self._seconds_until_next_wakeup()
            ):
                # Safely narrow key.fileobj to socket.socket
                if not isinstance(key.fileobj, socket.socket):
                    continue

                sock: socket.socket = key.fileobj
                data = cast(str, key.data)

                if data == "listener":
                    conn, client_addr = cast(
                        tuple[socket.socket, tuple[str, int]], listener.accept()
                    )

                    self._register(conn, "client")
                    log(
                        f"new client connection from {client_addr[0]}:{client_addr[1]}",
                        kind="registration",
                    )

                else:
                    try:
                        if events & selectors.EVENT_READ:
                            self._read_messages(sock, data)
                        if sock in self._connections and events & selectors.EVENT_WRITE:
                            self._write_pending(sock)
                    except BlockingIOError:
                        pass  # Try again when the selector reports readiness.
                    except (OSError, ValueError) as exc:
                        self._close_sock(sock, f"Connection lost: {exc}")

            # after handling whatever woke select() up, see if it's checkpoint time
            self._send_checkpoint_if_time()


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
        help=f"Replication mode (default {REPLICATION_MODE}); passive server support is incomplete",
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
    )
    try:
        server.serve()
    except KeyboardInterrupt:
        log(f"{replica_id} shutting down", kind="failure")


if __name__ == "__main__":
    main()
