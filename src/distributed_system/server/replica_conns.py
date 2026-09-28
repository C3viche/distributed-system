"""Client-side connections to every active server replica.

One ReplicaConnections instance lives inside a client process. It connects
to S1, S2, and S3, sends each request to every replica that is still alive,
and collects the replies that arrive before a timeout. The first matching
reply is delivered. Later replies for the same request_num are logged and
discarded.

A replica is marked dead on connect failure, send failure, or a closed
socket, and is never waited on again. A timeout only ends the wait for this
request; it does not by itself mark a replica dead.
"""

import select
import socket
import time

from distributed_system.common import BufferedJsonConnection, log, send_json
from distributed_system.config import get_server_addresses


class ReplicaConnections:
    """Sockets from one client to every server replica."""

    def __init__(
        self,
        client_id: str,
        replicas: dict[str, tuple[str, int]] | None = None,
        timeout: float = 1.0,
    ) -> None:
        self.client_id: str = client_id
        self.replicas: dict[str, tuple[str, int]] = (
            replicas if replicas is not None else get_server_addresses()
        )
        self.timeout: float = timeout
        self.alive: dict[str, socket.socket] = {}
        self.dead: set[str] = set()

    def connect_all(self) -> None:
        """Open one TCP connection per replica. A failed connect marks that replica dead."""
        for replica_id, (host, port) in self.replicas.items():
            if replica_id in self.dead:
                continue
            try:
                sock = socket.create_connection((host, port), timeout=self.timeout)
                sock.settimeout(None)
            except OSError as exc:
                log(
                    f"{self.client_id} could not connect to {replica_id} at {host}:{port}: {exc}",
                    kind="failure",
                )
                self.mark_dead(replica_id)
                continue
            self.alive[replica_id] = sock
            log(
                f"{self.client_id} connected to {replica_id} at {host}:{port}",
                kind="registration",
            )

    def mark_dead(self, replica_id: str) -> None:
        """Close the replica socket and never wait on it again."""
        self.dead.add(replica_id)
        sock = self.alive.pop(replica_id, None)
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass

    def send_and_collect(self, request_num: int, payload: str) -> dict[str, object] | None:
        """Send one request to every alive replica and return the first matching reply.

        Every arrival is printed. Replies after the first for this request_num
        are printed as discarded duplicates. Returns None when no alive replica
        answers before the timeout.
        """
        pending: dict[socket.socket, str] = {}
        buffers: dict[str, BufferedJsonConnection] = {}

        for replica_id, sock in list(self.alive.items()):
            request = {
                "type": "request",
                "client_id": self.client_id,
                "replica_id": replica_id,
                "request_num": request_num,
                "payload": payload,
            }
            log(
                f"Sent <{self.client_id}, {replica_id}, {request_num}, {payload}>",
                kind="send",
            )
            try:
                sock.settimeout(self.timeout)
                send_json(sock, request)
            except OSError as exc:
                log(f"{self.client_id} send to {replica_id} failed: {exc}", kind="failure")
                self.mark_dead(replica_id)
                continue
            buffers[replica_id] = BufferedJsonConnection(sock)
            pending[sock] = replica_id

        delivered: dict[str, object] | None = None
        deadline = time.monotonic() + self.timeout
        while pending:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            readable, _, errored = select.select(list(pending), [], list(pending), remaining)
            for sock in errored:
                replica_id = pending.pop(sock, None)
                if replica_id is not None:
                    self.mark_dead(replica_id)
            for sock in readable:
                replica_id = pending.get(sock)
                if replica_id is None:
                    continue
                try:
                    messages = buffers[replica_id].receive()
                except BlockingIOError:
                    continue
                except (OSError, ValueError):
                    pending.pop(sock, None)
                    self.mark_dead(replica_id)
                    continue
                if messages is None:
                    pending.pop(sock, None)
                    self.mark_dead(replica_id)
                    continue
                if not messages:
                    continue
                pending.pop(sock, None)
                for reply in messages:
                    if not self._reply_matches(reply, replica_id, request_num):
                        continue
                    log(
                        f"Received <{self.client_id}, {replica_id}, {request_num}, reply>",
                        kind="receive",
                    )
                    if delivered is None:
                        delivered = reply
                    else:
                        log(
                            f"request_num {request_num}: Discarded duplicate reply from {replica_id}.",
                            kind="info",
                        )
        return delivered

    def _reply_matches(
        self,
        reply: dict[str, object],
        expected_replica_id: str,
        request_num: int,
    ) -> bool:
        return (
            reply.get("type") == "reply"
            and reply.get("client_id") == self.client_id
            and reply.get("replica_id") == expected_replica_id
            and reply.get("request_num") == request_num
        )

    def close(self) -> None:
        for replica_id in list(self.alive):
            self.mark_dead(replica_id)
