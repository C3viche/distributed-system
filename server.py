"""S1 server process for Milestone 1 18-749.

On startup the server registers with the LFD
Then serves client requests and answers LFD heartbeats in a loop.
No threads, timers, or randomness, it is deterministic per the guidelines


To Run:
    python server.py                # host/port from config.py
"""

import argparse
import selectors
import socket
import time

from common import log, recv_json, send_json
from config import get_address

sel = selectors.DefaultSelector()
replica_id = "S1"
my_state = 0


def connect_to_lfd():
    host, port = get_address("LFD1")
    # we start S1 before LFD1 in the demo, so keep retrying until it's up
    while True:
        try:
            s = socket.create_connection((host, port))
            break
        except OSError:
            log(f"{replica_id}: no LFD at {host}:{port} yet, retrying", kind="info")
            time.sleep(1)
    send_json(s, {"type": "registration", "replica_id": replica_id})
    log(f"{replica_id} registered with LFD1", kind="registration")
    return s


def handle_lfd(sock):
    msg = read_or_none(sock)
    if msg is None:
        close_sock(sock, "lost connection to LFD1")
        return
    if msg.get("type") != "heartbeat":
        return
    count = msg.get("count")
    log(f"{replica_id} got heartbeat [{count}] from LFD1", kind="heartbeat")
    send_json(sock, {"type": "heartbeat_ack", "replica_id": replica_id, "count": count})
    log(f"{replica_id} sent ack [{count}] back to LFD1", kind="heartbeat")


def handle_client(sock):
    global my_state
    msg = read_or_none(sock)
    if msg is None:
        close_sock(sock, "client disconnected")
        return
    if msg.get("type") != "request":
        return

    client = msg.get("client_id")
    num = msg.get("request_num")
    payload = msg.get("payload", "")

    log(f"Received <{client}, {replica_id}, {num}, {payload}>", kind="receive")
    log(f"my_state = {my_state} before processing", kind="state")
    my_state += 1
    log(f"my_state = {my_state} after processing", kind="state")

    reply = {
        "type": "reply",
        "client_id": client,
        "replica_id": replica_id,
        "request_num": num,
        "state": my_state,
    }
    log(f"Sending <{client}, {replica_id}, {num}, reply>", kind="send")
    send_json(sock, reply)


def read_or_none(sock):
    # a client dying mid-message raises, treat it the same as a clean close
    try:
        return recv_json(sock)
    except (ConnectionError, OSError):
        return None


def close_sock(sock, why):
    # have to unregister or select() keeps waking up on the dead socket
    sel.unregister(sock)
    sock.close()
    log(why, kind="failure")


def serve(port_override):
    host, port = get_address(replica_id)
    if port_override:
        port = port_override

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # lets us restart right after a ctrl-c instead of "address already in use"
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((host, port))
    listener.listen()
    log(f"{replica_id} up, waiting for clients on {host}:{port}", kind="info")

    lfd = connect_to_lfd()

    # tag each socket so the loop knows what it's looking at
    sel.register(listener, selectors.EVENT_READ, "listener")
    sel.register(lfd, selectors.EVENT_READ, "lfd")

    while True:
        for key, _ in sel.select():
            if key.data == "listener":
                conn, addr = listener.accept()
                sel.register(conn, selectors.EVENT_READ, "client")
                log(f"new client connection from {addr[0]}:{addr[1]}", kind="info")
            elif key.data == "lfd":
                handle_lfd(key.fileobj)
            else:
                handle_client(key.fileobj)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", default="S1")
    parser.add_argument("--port", type=int, default=None, help="override the port from config.py")
    args = parser.parse_args()

    replica_id = args.id
    try:
        serve(args.port)
    except KeyboardInterrupt:
        log(f"{replica_id} shutting down", kind="failure")
