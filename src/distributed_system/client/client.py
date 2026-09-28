"""Client for Milestone 2 active replication.

Runs as an independent process (C1, C2, or C3). Each client opens one
long-lived TCP connection per replica and sends the same tagged request to
every live replica before the next request. The first reply is delivered;
later replies for that request_num are discarded.

No coordination between C1/C2/C3. Each holds its own request_num.
"""

import time

from distributed_system.common import log
from distributed_system.config import get_server_addresses
from distributed_system.server.replica_conns import ReplicaConnections


class Client:
    """One independent client process (C1, C2, or C3)."""

    def __init__(
        self,
        client_id: str,
        num_replicas: int | None,
        interval: float = 1.0,
        count: int | None = None,
        payload_template: str = "hello from {client_id} #{request_num}",
    ) -> None:
        """Initialize the client configuration and state."""
        self.client_id: str = client_id
        self.num_replicas: int | None = num_replicas
        self.replicas: dict[str, tuple[str, int]] = get_server_addresses(num_replicas)
        self.interval: float = interval
        self.count: int | None = count  # None -> loop until Ctrl-C / server closes
        self.payload_template: str = payload_template
        self.request_num: int = 1
        self.conns: ReplicaConnections = ReplicaConnections(client_id, replicas=self.replicas)

    # Public entry point
    def run(self) -> None:
        """Connect to every replica and start the request/reply loop."""
        log(f"{self.client_id} starting", kind="info")
        self.conns.connect_all()
        if not self.conns.alive:
            log(f"{self.client_id} could not connect to any server replicas", kind="failure")
            return

        try:
            self._loop()
            print("done looping")
        except KeyboardInterrupt:
            log(f"{self.client_id} shutting down", kind="info")
        finally:
            self.conns.close()

    def _loop(self) -> None:
        """Send the same request to every live replica, then the next request."""
        sent = 0
        while self.count is None or sent < self.count:
            if not self._send_and_await_reply():
                return
            sent += 1
            # Skip the sleep after the final send so --count exits promptly.
            if self.count is None or sent < self.count:
                time.sleep(self.interval)

    def _send_and_await_reply(self) -> bool:
        """Send one request through ReplicaConnections. True when a reply is delivered."""
        payload = self.payload_template.format(
            client_id=self.client_id,
            request_num=self.request_num,
        )
        reply = self.conns.send_and_collect(self.request_num, payload)
        if reply is None:
            return False
        self.request_num += 1
        return True
