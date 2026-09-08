"""S1 server process for Milestone 1 18-749.

The server starts serving clients immediately. Registration with the LFD
is non-blocking: if the LFD is not up yet the select loop wakes once a
second and retries, and it re-registers if the LFD connection drops.
Client requests and LFD heartbeats are handled in the same select loop.
No threads, timers, or randomness, it is deterministic per the guidelines


To Run:
    uv run server --id S1           # host/port from config.py
"""

import argparse
import selectors
import socket
from typing import cast

from distributed_system.common import BufferedJsonConnection, log
from distributed_system.config import get_address

class Server:
    def __init__(self, replica_id: str = "S1", port_override: int | None = None):
        self.replica_id: str = replica_id
        self.state: int = 0
        self._connections: dict[socket.socket, BufferedJsonConnection] = {}
        self.sel: selectors.DefaultSelector = selectors.DefaultSelector()
        
        # Load host/port configuration
        host, port = get_address(self.replica_id)

        self.host: str = host
        self.port = port

        if port_override:
            self.port: int = port_override

        # Outbound connection to LFD1; None while we have no LFD.
        self._lfd: socket.socket | None = None
        self._lfd_warned: bool = False

    def try_connect_to_lfd(self) -> None:
        """Attempt one registration with LFD1; never blocks the serve loop.

        On failure the socket stays None and the serve loop retries on its
        next tick. On success the socket joins the selector so heartbeats
        are handled alongside client traffic.
        """
        lfd_host, lfd_port = get_address("LFD1")
        try:
            # Short timeout bounds the TCP handshake if the LFD host is unreachable.
            s = socket.create_connection((lfd_host, lfd_port), timeout=1.0)
        except OSError:
            if not self._lfd_warned:
                log(f"{self.replica_id}: no LFD at {lfd_host}:{lfd_port} yet, retrying every second", kind="info")
                self._lfd_warned = True
            return

        self._lfd = s
        self._lfd_warned = False
        self._register(s, "lfd")
        self._send_json(s, {"type": "registration", "replica_id": self.replica_id})
        log(f"{self.replica_id} registered with LFD1", kind="registration")


    def handle_lfd(self, sock: socket.socket, msg: dict[str, object]) -> None:
        if msg.get("type") != "heartbeat":
            return
        
        count = msg.get("count")
        log(f"{self.replica_id} got heartbeat [{count}] from LFD1", kind="heartbeat")
        self._send_json(sock, {"type": "heartbeat_ack", "replica_id": self.replica_id, "count": count})
        log(f"{self.replica_id} sent ack [{count}] back to LFD1", kind="heartbeat")


    def handle_client(self, sock: socket.socket, msg: dict[str, object]) -> None:
        if msg.get("type") != "request":
            return
    
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


    def _register(self, sock: socket.socket, kind: str) -> None:
        self._connections[sock] = BufferedJsonConnection(sock)
        self.sel.register(sock, selectors.EVENT_READ, kind)

    def _send_json(self, sock: socket.socket, message: dict[str, object]) -> None:
        self._connections[sock].queue(message)
        key = self.sel.get_key(sock)
        self.sel.modify(sock, selectors.EVENT_READ | selectors.EVENT_WRITE, key.data)

    def _write_pending(self, sock: socket.socket, kind: str) -> None:
        connection = self._connections[sock]
        connection.flush()
        if not connection.has_pending_output:
            self.sel.modify(sock, selectors.EVENT_READ, kind)

    def _read_messages(self, sock: socket.socket, kind: str) -> None:
        messages = self._connections[sock].receive()
        if messages is None:
            self._close_sock(sock, "lost connection to LFD1" if kind == "lfd" else "client disconnected")
            return
        for msg in messages:
            if kind == "lfd":
                self.handle_lfd(sock, msg)
            else:
                self.handle_client(sock, msg)

    def _close_sock(self, sock: socket.socket, why: str):
        # have to unregister or select() keeps waking up on the dead socket
        _ = self.sel.unregister(sock)
        self._connections.pop(sock, None)
        sock.close()
        if sock is self._lfd:
            self._lfd = None  # serve loop will try to re-register
        log(why, kind="failure")
    
    
    def serve(self):
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # lets us restart right after a ctrl-c instead of "address already in use"
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((self.host, self.port))
        listener.listen()
        log(f"{self.replica_id} up, waiting for clients on {self.host}:{self.port}", kind="info")
    
        # tag each socket so the loop knows what it's looking at
        _ = self.sel.register(listener, selectors.EVENT_READ, "listener")
    
        while True:
            if self._lfd is None:
                self.try_connect_to_lfd()
            # With no LFD, wake every second to retry; otherwise block until I/O.
            timeout = None if self._lfd is not None else 1.0
            for key, events in self.sel.select(timeout=timeout):
                # Safely narrow key.fileobj to socket.socket
                if not isinstance(key.fileobj, socket.socket):
                    continue
                
                sock: socket.socket = key.fileobj
                data = cast(str, key.data)

                if data == "listener":
                    conn, client_addr = cast(tuple[socket.socket, tuple[str, int]], listener.accept())

                    self._register(conn, "client")
                    log(f"new client connection from {client_addr[0]}:{client_addr[1]}", kind="info")                
                
                else:
                    try:
                        if events & selectors.EVENT_READ:
                            self._read_messages(sock, data)
                        if sock in self._connections and events & selectors.EVENT_WRITE:
                            self._write_pending(sock, data)
                    except BlockingIOError:
                        pass  # Try again when the selector reports readiness.
                    except OSError as exc:
                        self._close_sock(sock, f"Connection lost: {exc}")


def main() -> None:
    parser = argparse.ArgumentParser()
    _ = parser.add_argument("--id", default="S1", help="Replica ID")
    _ = parser.add_argument("--port", type=int, default=None, help="Override port")

    args = parser.parse_args()
    replica_id = cast(str, args.id)
    port_override = cast(int | None, args.port)

    server = Server(replica_id=replica_id, port_override=port_override)
    try:
        server.serve()
    except KeyboardInterrupt:
        log(f"{replica_id} shutting down", kind="failure")

if __name__ == "__main__":
    main()
