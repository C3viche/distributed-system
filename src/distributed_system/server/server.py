"""S1 server process for Milestone 1 18-749.

On startup the server registers with the LFD
Then serves client requests and answers LFD heartbeats in a loop.
No threads, timers, or randomness, it is deterministic per the guidelines


To Run:
    uv run server --id S1           # host/port from config.py
"""

import argparse
import selectors
import socket
import time
from typing import cast

from distributed_system.common import log, recv_json, send_json
from distributed_system.config import get_address

class Server:
    def __init__(self, replica_id: str = "S1", port_override: int | None = None):
        self.replica_id: str = replica_id
        self.state: int = 0
        self.sel: selectors.DefaultSelector = selectors.DefaultSelector()
        
        # Load host/port configuration
        host, port = get_address(self.replica_id)

        self.host: str = host
        self.port = port

        if port_override:
            self.port: int = port_override

    def connect_to_lfd(self) -> socket.socket:
        # Connect outward to LFD1 as a client
        lfd_host, lfd_port = get_address("LFD1")
        while True:
            try:
                s = socket.create_connection((lfd_host, lfd_port))
                break
            except OSError:
                log(f"{self.replica_id}: no LFD at {lfd_host}:{lfd_port} yet, retrying", kind="info")
                time.sleep(1)
                
        send_json(s, {"type": "registration", "replica_id": self.replica_id})
        log(f"{self.replica_id} registered with LFD1", kind="registration")
        return s


    def handle_lfd(self, sock: socket.socket) -> None:
        msg = self._read_or_none(sock)
        if msg is None:
            self._close_sock(sock, "lost connection to LFD1")
            return
        if msg.get("type") != "heartbeat":
            return
        
        count = msg.get("count")
        log(f"{self.replica_id} got heartbeat [{count}] from LFD1", kind="heartbeat")
        send_json(sock, {"type": "heartbeat_ack", "replica_id": self.replica_id, "count": count})
        log(f"{self.replica_id} sent ack [{count}] back to LFD1", kind="heartbeat")


    def handle_client(self, sock: socket.socket) -> None:
        msg = self._read_or_none(sock)
        if msg is None:
            self._close_sock(sock, "client disconnected")
            return
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
        send_json(sock, reply)


    def _read_or_none(self, sock: socket.socket) -> dict[str, object] | None:
        # a client dying mid-message raises, treat it the same as a clean close
        try:
            return recv_json(sock)
        except (ConnectionError, OSError):
            return None
    
    
    def _close_sock(self, sock: socket.socket, why: str):
        # have to unregister or select() keeps waking up on the dead socket
        _ = self.sel.unregister(sock)
        sock.close()
        log(why, kind="failure")
    
    
    def serve(self):
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # lets us restart right after a ctrl-c instead of "address already in use"
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((self.host, self.port))
        listener.listen()
        log(f"{self.replica_id} up, waiting for clients on {self.host}:{self.port}", kind="info")
    
        lfd = self.connect_to_lfd()
    
        # tag each socket so the loop knows what it's looking at
        _ = self.sel.register(listener, selectors.EVENT_READ, "listener")
        _ = self.sel.register(lfd, selectors.EVENT_READ, "lfd")
    
        while True:
            for key, _ in self.sel.select():
                # Safely narrow key.fileobj to socket.socket
                if not isinstance(key.fileobj, socket.socket):
                    continue
                
                sock: socket.socket = key.fileobj
                data = cast(str, key.data)

                if data == "listener":
                    conn, client_addr = cast(tuple[socket.socket, tuple[str, int]], listener.accept())

                    _ = self.sel.register(conn, selectors.EVENT_READ, "client")
                    log(f"new client connection from {client_addr[0]}:{client_addr[1]}", kind="info")                
                
                elif data == "lfd":
                    self.handle_lfd(sock)
                
                else:
                    self.handle_client(sock)


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
