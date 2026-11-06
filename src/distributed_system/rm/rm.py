"""M4 RM receiver. GFD forwarding and recovery integration are pending.

Provisional JSON-line contract: register_gfd (gfd_id=GFD), then add_replica /
delete_replica (replica_id), or membership (members, member_count) for resync.
This tracks reported membership only; it does not certify recovery readiness.
"""

from __future__ import annotations

import argparse
import selectors
import socket

from distributed_system.common import BufferedJsonConnection, log, set_process_id
from distributed_system.config import SERVERS, resolve_address


class ReplicationManager:
    def __init__(self, host_override: str | None = None, port_override: int | None = None):
        self.host, self.port = resolve_address("RM", host_override, port_override)
        self.membership: list[str] = []
        self._gfd: socket.socket | None = None
        self._connections: dict[socket.socket, BufferedJsonConnection] = {}
        self._selector = selectors.DefaultSelector()

    @property
    def member_count(self) -> int:
        # Derive the count so duplicate notifications cannot make it drift.
        return len(self.membership)

    def _print_membership(self) -> None:
        suffix = f": {', '.join(self.membership)}" if self.membership else ""
        word = "member" if self.member_count == 1 else "members"
        log(f"RM: {self.member_count} {word}{suffix}", kind="membership")

    @staticmethod
    def _replica(value: object) -> str:
        if not isinstance(value, str) or value not in SERVERS:
            raise ValueError("Expected a configured replica_id")
        return value

    def _handle_message(self, sock: socket.socket, message: object) -> None:
        if not isinstance(message, dict):
            raise ValueError("Expected a JSON object")
        kind = message.get("type")
        if not isinstance(kind, str):
            raise ValueError("Expected a string message type")
        if kind == "register_gfd":
            if message.get("gfd_id") != "GFD":
                raise ValueError("Expected gfd_id GFD")
            if self._gfd is not None and self._gfd is not sock:
                self._close_peer(self._gfd)
            self._gfd = sock
            log("GFD registered with RM", kind="registration")
            return
        if sock is not self._gfd:
            raise ValueError("GFD must register before reporting membership")

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

        # Full reports must be visible even when the initial group is still empty.
        if kind == "membership" or updated != self.membership:
            self.membership = updated
            self._print_membership()

    def _close_peer(self, sock: socket.socket) -> None:
        self._connections.pop(sock, None)
        try:
            self._selector.unregister(sock)
        except KeyError:
            pass
        sock.close()
        if self._gfd is sock:
            self._gfd = None
            log("GFD disconnected; retaining last reported membership until resync",
                kind="failure")

    def serve(self) -> None:
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
                    for key, _ in self._selector.select():
                        sock = key.fileobj
                        if sock is listener:
                            peer, _ = listener.accept()
                            self._connections[peer] = BufferedJsonConnection(peer)
                            self._selector.register(peer, selectors.EVENT_READ)
                            continue
                        if sock not in self._connections:
                            continue
                        try:
                            messages = self._connections[sock].receive()
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
