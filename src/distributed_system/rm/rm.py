"""Track liveness and coordinate replica recovery.

Run with ``uv run rm``. See README for the proposed GFD message format.
"""

from __future__ import annotations

import argparse
import selectors
import socket
from dataclasses import dataclass
from uuid import uuid4

from distributed_system.common import BufferedJsonConnection, log, set_process_id
from distributed_system.config import PRIMARY_ID, REPLICATION_MODE, SERVERS, resolve_address


@dataclass
class Recovery:
    """One replica control connection and its current recovery attempt."""

    sock: socket.socket
    bootstrap: bool = False
    recovery_id: str | None = None
    donor: str | None = None
    ready: bool = False


class ReplicationManager:
    def __init__(self, host_override: str | None = None, port_override: int | None = None):
        """Initialize membership and recovery state for this RM run."""
        self.host, self.port = resolve_address("RM", host_override, port_override)
        self.membership: list[str] = []
        self.replicas: dict[str, Recovery] = {}
        self.primary_id: str | None = PRIMARY_ID
        self._snapshot_received = False
        self._bootstrapped = False
        self._gfd: socket.socket | None = None
        self._connections: dict[socket.socket, BufferedJsonConnection] = {}
        self._selector = selectors.DefaultSelector()

    @property
    def member_count(self) -> int:
        """Count replicas reported alive by GFD, including recovering ones."""
        return len(self.membership)

    def _print_membership(self) -> None:
        """Print the latest GFD membership in the required console format."""
        suffix = f": {', '.join(self.membership)}" if self.membership else ""
        word = "member" if self.member_count == 1 else "members"
        log(f"RM: {self.member_count} {word}{suffix}", kind="membership")

    @staticmethod
    def _replica(value: object) -> str:
        """Validate a replica identifier against configured addresses."""
        if not isinstance(value, str) or value not in SERVERS:
            raise ValueError("Expected a configured replica_id")
        return value

    def _send(self, sock: socket.socket, message: dict) -> None:
        """Queue a control message without blocking the event loop."""
        self._connections[sock].queue(message)
        self._selector.modify(sock, selectors.EVENT_READ | selectors.EVENT_WRITE)

    def _instruction(self, replica: str, state: Recovery) -> dict:
        """Describe the current transfer, or acknowledge a completed join."""
        kind = "ready" if state.ready else ("recover" if state.donor else "initialize")
        message = {"type": kind,
                   "replica_id": replica, "recovery_id": state.recovery_id,
                   "primary_id": self.primary_id}
        if not state.ready:
            message["source_id"] = state.donor
            message["source_address"] = SERVERS[state.donor] if state.donor else None
        return message

    def _coordinate(self) -> None:
        """Invalidate failed transfers and assign ready donors to waiting replicas."""
        for replica, state in self.replicas.items():
            if replica not in self.membership:
                if state.recovery_id:
                    self._send(state.sock, {"type": "recovery_cancelled",
                                           "recovery_id": state.recovery_id})
                state.ready = False
                state.recovery_id = None
                state.donor = None
        for replica, state in self.replicas.items():
            if state.ready:
                continue
            donor = self.replicas.get(state.donor)
            valid = (self._snapshot_received and replica in self.membership
                     and ((state.donor is None and self.primary_id == PRIMARY_ID) or (
                         donor is not None and donor.ready
                         and (REPLICATION_MODE == "active"
                              or state.donor == self.primary_id))))
            if state.recovery_id and not valid:
                self._send(state.sock, {"type": "recovery_cancelled",
                                       "recovery_id": state.recovery_id})
                state.recovery_id = None
                state.donor = None
            if state.recovery_id or not self._snapshot_received or replica not in self.membership:
                continue
            candidates = [name for name, peer in self.replicas.items()
                          if peer.ready and name in self.membership and name != replica
                          and (REPLICATION_MODE == "active" or name == self.primary_id)]
            bootstrap = (state.bootstrap and not self._bootstrapped
                         and replica == PRIMARY_ID and self.primary_id == PRIMARY_ID)
            if not candidates and not bootstrap:
                continue
            state.donor = sorted(candidates)[0] if candidates else None
            state.recovery_id = uuid4().hex
            self._send(state.sock, self._instruction(replica, state))

    def _handle_replica(self, sock: socket.socket, message: dict) -> None:
        """Handle join requests and completion for the current control connection."""
        replica = self._replica(message.get("replica_id"))
        if sock is self._gfd:
            raise ValueError("GFD cannot register as a replica")
        state = self.replicas.get(replica)
        if message["type"] in {"join", "join_request"}:
            bootstrap = message.get("bootstrap", False)
            if type(bootstrap) is not bool:
                raise ValueError("bootstrap must be a boolean")
            if any(peer.sock is sock for name, peer in self.replicas.items() if name != replica):
                raise ValueError("A connection cannot change replica identity")
            if state is None or state.sock is not sock:
                if state is not None:
                    self._close_peer(state.sock)
                state = Recovery(sock, bootstrap)
                self.replicas[replica] = state
            self._send(sock, {"type": "join_accepted", "replica_id": replica})
            if state.recovery_id:
                self._send(sock, self._instruction(replica, state))
            self._coordinate()
            return
        if state is None or state.sock is not sock:
            raise ValueError("Replica must join on this connection first")
        token = message.get("recovery_id")
        if not token or token != state.recovery_id or not self._snapshot_received:
            self._send(sock, {"type": "recovery_rejected", "recovery_id": token,
                              "reason": "stale recovery"})
            return
        if not state.ready:
            state.ready = True
            log(f"RM: {replica} synchronized and ready", kind="state")
        self._bootstrapped = True
        self._send(sock, self._instruction(replica, state))
        self._coordinate()

    def _handle_message(self, sock: socket.socket, message: object) -> None:
        """Route replica control messages and apply validated GFD updates."""
        if not isinstance(message, dict):
            raise ValueError("Expected a JSON object")
        kind = message.get("type")
        if not isinstance(kind, str):
            raise ValueError("Expected a string message type")
        if kind in {"join", "join_request", "sync_complete"}:
            self._handle_replica(sock, message)
            return
        if kind == "register_gfd":
            if any(state.sock is sock for state in self.replicas.values()):
                raise ValueError("Replica cannot register as GFD")
            if message.get("gfd_id") != "GFD":
                raise ValueError("Expected gfd_id GFD")
            if self._gfd is not None and self._gfd is not sock:
                self._close_peer(self._gfd)
            self._gfd = sock
            self._snapshot_received = False
            self._coordinate()
            log("GFD registered with RM", kind="registration")
            return
        if sock is not self._gfd:
            raise ValueError("GFD must register before reporting membership")

        primary = self.primary_id
        if "primary_id" in message:
            primary = message["primary_id"]
            if primary is not None:
                primary = self._replica(primary)
        updated = list(self.membership)
        if kind in {"add_replica", "delete_replica"}:
            replica = self._replica(message.get("replica_id"))
            if kind == "add_replica" and replica not in updated:
                updated.append(replica)
            elif kind == "delete_replica" and replica in updated:
                updated.remove(replica)
        elif kind == "membership":
            members = message.get("members")
            if not isinstance(members, list):
                raise ValueError("Expected a members list")
            updated = [self._replica(member) for member in members]
            count = message.get("member_count")
            if (len(set(updated)) != len(updated) or type(count) is not int
                    or count != len(updated)):
                raise ValueError("Invalid membership snapshot count or duplicate members")
        else:
            raise ValueError("Unsupported RM message type")

        if primary in self.membership and primary not in updated:
            primary = None
        self.primary_id = primary
        if kind == "membership":
            self._snapshot_received = True
        # Print snapshots even when unchanged, including GFD's initial empty report.
        if kind == "membership" or updated != self.membership:
            self.membership = updated
            self._print_membership()
        self._coordinate()

    def _close_peer(self, sock: socket.socket) -> None:
        """Drop a peer and cancel recovery attempts that depended on it."""
        for replica, state in list(self.replicas.items()):
            if state.sock is sock:
                del self.replicas[replica]
        self._connections.pop(sock, None)
        try:
            self._selector.unregister(sock)
        except KeyError:
            pass
        sock.close()
        if self._gfd is sock:
            self._gfd = None
            self._snapshot_received = False
            log("GFD disconnected; retaining last reported membership until resync",
                kind="failure")
        self._coordinate()

    def serve(self) -> None:
        """Accept peers and service buffered reads and writes until shutdown."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener.bind((self.host, self.port))
                listener.listen()
                listener.setblocking(False)
                self._selector.register(listener, selectors.EVENT_READ)
                log(f"RM listening on {self.host}:{listener.getsockname()[1]}")
                self._print_membership()
                while True:
                    for key, events in self._selector.select():
                        sock = key.fileobj
                        if sock is listener:
                            peer, _ = listener.accept()
                            self._connections[peer] = BufferedJsonConnection(peer)
                            self._selector.register(peer, selectors.EVENT_READ)
                            continue
                        if sock not in self._connections:
                            continue
                        try:
                            connection = self._connections[sock]
                            if events & selectors.EVENT_WRITE:
                                connection.flush()
                                if not connection.has_pending_output:
                                    self._selector.modify(sock, selectors.EVENT_READ)
                            if not events & selectors.EVENT_READ:
                                continue
                            messages = connection.receive()
                            if messages is None:
                                self._close_peer(sock)
                                continue
                            for message in messages:
                                self._handle_message(sock, message)
                        except BlockingIOError:
                            pass
                        except (OSError, ValueError) as exc:
                            log(f"RM rejected/disconnected peer: {exc}", kind="failure")
                            self._close_peer(sock)
        finally:
            for sock in list(self._connections):
                self._close_peer(sock)
            self._selector.close()


def main() -> None:
    """Parse listener overrides and start the replication manager."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=None, help="override RM_HOST")
    parser.add_argument("--port", type=int, default=None, help="override RM_PORT")
    args = parser.parse_args()
    set_process_id("RM")
    manager = ReplicationManager(args.host, args.port)
    try:
        manager.serve()
    except KeyboardInterrupt:
        log("RM shutting down")


if __name__ == "__main__":
    main()
