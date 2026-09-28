# 18-749 Distributed System

Fault-tolerant client/server app for CMU 18-749. Python, plain TCP sockets,
one JSON object per line. No threads anywhere; every process is a single
`selectors` loop so the servers stay deterministic.

Current milestone: **M2, active replication.** Three replicas, three LFDs, one
GFD, three clients. Clients send every request to all live replicas, keep the
first reply, and discard the rest. Kill a replica and the LFD notices, the GFD
updates membership, and the clients keep going.

## What runs where

| Laptop | Runs | Command |
|---|---|---|
| Replica 1 | LFD1, S1 | `./run_all.sh replica 1` |
| Replica 2 | LFD2, S2 | `./run_all.sh replica 2` |
| Replica 3 | LFD3, S3 | `./run_all.sh replica 3` |
| Client laptop | GFD, C1, C2, C3 | `./run_all.sh gfd` then `./run_all.sh clients` |

Each LFD has to be on the same machine as the server it heartbeats. The GFD
and the clients share a machine because the guide says so and because none of
them are replicated anyway.

## Setup

Python 3.13 and [uv](https://docs.astral.sh/uv/). Then:

```bash
uv sync
cp .env.example .env
```

`.env` holds the host and port of every process and is gitignored, so each
laptop keeps its own copy. The template runs everything on `127.0.0.1`, which
is all you need to develop on one machine.

### Before a multi-laptop run

1. **Everyone on the same Wi-Fi.** Turn VPNs off; they route traffic away from
   the local network. Test on the network you'll demo on, since some CMU
   subnets block laptop-to-laptop traffic.
2. **Find your IP.** On a Mac:
   ```bash
   ipconfig getifaddr en0
   ```
   If that prints nothing, try `en1`, or look in System Settings → Wi-Fi →
   Details. This address changes every time you join a different network, so
   check it again on demo day.
3. **Fill in `.env` with the four real addresses.** The replica-1 laptop's IP
   goes in `S1_HOST` and `LFD1_HOST`, and so on. The client laptop's IP goes in
   `GFD_HOST`. Leave the ports alone. **All four `.env` files should be
   identical.** Paste the finished one in Slack so nobody types it by hand.
4. **Firewall.** The first time Python listens on a port, macOS asks whether to
   allow incoming connections. Click Allow. If a client can't reach a server,
   this prompt is the usual reason.

## Running it

`run_all.sh` opens each process in its own Terminal window, in the order the
rubric wants. It just runs the `uv run` commands listed below; use those
directly if you'd rather.

Launch order matters. GFD first, then the LFDs, then the servers one at a
time, then the clients.

```bash
# client laptop
./run_all.sh gfd

# each replica laptop, one after another
./run_all.sh replica 1
./run_all.sh replica 2
./run_all.sh replica 3

# client laptop, once all three replicas show up in the GFD window
./run_all.sh clients
```

Everything on one machine:

```bash
./run_all.sh local
```

Other things it can do:

```bash
./run_all.sh kill server 1     # crash S1, same as Ctrl-C in its window
./run_all.sh kill all          # stop everything it started
./run_all.sh local --headless  # no windows; output goes to logs/*.log
HEARTBEAT_FREQ=2 INTERVAL=0.5 ./run_all.sh local   # faster heartbeats and requests
```

It refuses to start if a port is already taken, which usually means a process
from an earlier run is still alive. `kill all` clears that.

### The commands it runs

```bash
uv run gfd --heartbeat_freq 1 --timeout 2
uv run lfd --id LFD1 --heartbeat_freq 1 --timeout 2
uv run server --id S1
uv run client --id C1 --interval 1
```

`--heartbeat_freq` is in Hz. Use the same value on the GFD and all three LFDs.
`--timeout` is how many seconds without an ack before the LFD declares the
server dead. `--interval` is seconds between client requests.

## Demo walkthrough

What to point at in each window, in the order Priya's rubric goes.

1. GFD starts and prints `GFD: 0 members`.
2. Each LFD prints that it registered with the GFD. GFD and LFD windows start
   showing numbered heartbeats in both directions.
3. S1 starts, registers with LFD1, and LFD1 prints `LFD1: add replica S1`.
   GFD prints `GFD: 1 member: S1`. Same for S2 and S3, ending at
   `GFD: 3 members: S1, S2, S3`.
4. Each client registers with the GFD, gets the member list, and opens a
   socket to every replica. It then loops forever: three `Sent` lines, one per
   replica, then `Received` for the replies. `request_num` goes up after the
   first reply, not after all three.
5. Every server window shows `Received`, `my_state` before, `my_state` after,
   and `Sending` for every request. `my_state` counts requests, so the three
   servers track each other; they can differ briefly when requests from
   different clients arrive in a different order.
6. Ctrl-C S1. Within one heartbeat interval LFD1 prints `S1 has died` and
   `LFD1: delete replica S1`. GFD prints `GFD: 2 members: S2, S3` and pushes
   the new list to the clients. Clients print that they removed S1 and carry
   on with two `Sent` lines per request. No pause.
7. Wait a bit, then Ctrl-C S2. Same thing again, ending at `GFD: 1 member: S3`.

Not part of M2: bringing a dead replica back, checkpointing, the RM.

## Messages on the wire

Newline-delimited JSON over TCP. Every client/server message carries
`client_id`, `replica_id`, and `request_num` so requests and replies can be
matched up.

```json
{"type": "request", "client_id": "C1", "replica_id": "S1", "request_num": 1, "payload": "hello from C1 #1"}
{"type": "reply",   "client_id": "C1", "replica_id": "S1", "request_num": 1, "state": 1}
```

Server ↔ LFD (unchanged from M1):

```json
{"type": "registration", "replica_id": "S1"}
{"type": "heartbeat", "lfd_id": "LFD1", "count": 1}
{"type": "heartbeat_ack", "replica_id": "S1", "count": 1}
```

LFD ↔ GFD and client ↔ GFD (new in M2):

```json
{"type": "register_lfd", "lfd_id": "LFD1"}
{"type": "heartbeat", "from": "GFD", "to": "LFD1", "count": 1}
{"type": "heartbeat_ack", "from": "LFD1", "to": "GFD", "count": 1}
{"type": "add_replica", "lfd_id": "LFD1", "replica_id": "S1"}
{"type": "delete_replica", "lfd_id": "LFD1", "replica_id": "S1"}
{"type": "register_client", "client_id": "C1"}
{"type": "membership", "members": ["S1", "S2"], "member_count": 2}
```

## Console colors

Timestamps are UTC. Colors by message kind:

- yellow: requests and replies (bold for sends)
- magenta: heartbeats
- green: `my_state` updates
- blue: registrations
- cyan: membership changes
- red: failures and dropped connections

## Tests

```bash
uv run python -m unittest discover -s tests -p 'test_gfd_integration.py' -v
```

Spins up a real GFD, LFDs, servers, and clients on random localhost ports and
checks their output. Takes about five seconds.

## Layout

```
src/distributed_system/
  common.py        JSON framing, buffered connections, colored log()
  config.py        reads .env, maps S1→LFD1 etc.
  server/server.py
  lfd/lfd.py
  gfd/gfd.py
  client/client.py
run_all.sh
tests/
```

## Milestones

1. One server, one LFD, three clients. Done.
2. Active replication with the GFD. This one.
3. Passive replication with checkpointing.
4. RM plus manual recovery.
5. Automatic recovery under repeated faults.

*AI was used as assistance in the making of this document*
