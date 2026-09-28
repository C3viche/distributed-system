# Distributed Fault-Tolerant Application (18-749)
A fault-tolerant, deterministic distributed client-server application built in Python for *CMU 18-749: Building Reliable Distributed Systems*.

This project implements a complete distributed fault-tolerance infrastructure featuring heartbeating, membership updates, active/passive replication, state logging, checkpointing, and automatic failure recovery.

## System Architecture
The overall system models an asynchronous distributed application split into nodes that communicate over newline-delimited JSON TCP sockets:
- **Server Replicas (`S1`, `S2`, `S3`)**: Stateful, deterministic server nodes. Each server maintains internal state (`my_state`) and handles incoming client requests and heartbeat checks.
- **Local Fault Detector (`LFD1`, `LFD2`, `LFD3`)**: Resides on the same physical/virtual host as its assigned server replica. Sends periodic heartbeats to monitor replica health and reports crashes.
- **Global Fault Detector (`GFD`)**: Aggregates local health notifications from all LFDs, maintains central system group membership, and broadcasts membership updates.
- **Replication Manager (`RM`)**: Orchestrates high-level system fault tolerance, tracks healthy members, and automates replica recovery/re-launch.
- **Clients (`C1`, `C2`, `C3`)**: Independent client processes issuing requests with unique `<client_id, replica_id, request_num>` tuples.

## Prerequisites & Installation
- Python: >= 3.13
- Package & Task Runner: uv

Install dependencies and set up the local virtual environment:

```bash
uv sync
```

## Configuration (`.env`)

Copy the shared template to create your local configuration:

```bash
cp .env.example .env
```

`.env` is ignored by Git; `.env.example` lists all supported host/port settings.
The template runs everything on one Mac. For multiple Macs, replace loopback
addresses with the corresponding machines' reachable LAN IPs, keeping each
server and its LFD on the same machine. Use consistent addresses across machines.
Existing shell environment variables take precedence over `.env` values.

Heartbeat frequency and timeout remain CLI options (`--heartbeat_freq` and
`--timeout`); use the same frequency for all three LFDs. GFD uses `GFD_HOST` and `GFD_PORT`; RM remains reserved for a future milestone.

### Setting up `.env` for a multi-laptop run

1. Make sure everyone is on the same Wi-Fi network and has their VPN turned off. Since the IPs are handed out by the network, they will be different on campus Wi-Fi than at home, so this has to be redone on the day of the demo.
2. Find your laptop's IP address. On a Mac:
   ```bash
   ipconfig getifaddr en0
   ```
   If that prints nothing, try `en1`, or check System Settings → Wi-Fi → Details.
3. Put the four addresses into `.env`. The replica 1 laptop's IP goes in `S1_HOST` and `LFD1_HOST`, replica 2's in `S2_HOST` and `LFD2_HOST`, and so on. The client laptop's IP goes in `GFD_HOST`. The ports can stay as they are. Every laptop should end up with the same `.env`, so it is easiest to have one person fill it in and share it.
4. The first time a process listens on a port, macOS will ask whether to allow incoming connections. Allow it. If a client cannot reach a server, this prompt is usually the reason.

## Milestone #2 Execution Guide
Milestone #2 runs three active replicas with an LFD each, a GFD that tracks membership, and three clients that send every request to all replicas and discard duplicate replies.

### Laptop division

| Laptop | Processes |
|---|---|
| Replica 1 | LFD1, S1 |
| Replica 2 | LFD2, S2 |
| Replica 3 | LFD3, S3 |
| Client laptop | GFD, C1, C2, C3 |

Each LFD has to be on the same machine as the server it heartbeats. The GFD and the clients go on the fourth machine, as the project guide suggests. This layout stays the same for later milestones; the RM joins the client laptop in Milestone 4.

The launch order is GFD, then the three LFDs, then S1, S2, and S3 one at a time, then the clients.

### Using `run_all.sh`

`run_all.sh` opens each process in its own Terminal window and runs the `uv run` commands listed below. Each laptop runs the command for its role:

```bash
# Client laptop, first
./run_all.sh gfd

# Replica laptops, one at a time (2 and 3 on the other laptops)
./run_all.sh replica 1

# Client laptop, once the GFD window shows all three replicas
./run_all.sh clients
```

To run everything on one machine for testing:

```bash
# Client C1
uv run client --id C1 --num_replicas 1

# Client C2
uv run client --id C2 --num_replicas 1

# Client C3
uv run client --id C3 --num_replicas 1
./run_all.sh local
```

Other options:

```bash
./run_all.sh kill server 1     # crash S1, same as Ctrl-C in its window
./run_all.sh kill all          # stop everything the script started
./run_all.sh local --headless  # no windows, output goes to logs/*.log
HEARTBEAT_FREQ=2 INTERVAL=0.5 ./run_all.sh local   # faster heartbeats and requests
```

The script will not start if one of the ports is already in use, which usually means a process from an earlier run is still going. `kill all` takes care of that. If the port belongs to some other program (OrbStack uses 8080, for example), either quit it or change the port in `.env` on every laptop. `kill` only stops processes that were started from this repository.

### Running the commands by hand

Run each command in a separate terminal tab/window:

```bash
uv run gfd --heartbeat_freq 1 --timeout 2
uv run lfd --id LFD1 --heartbeat_freq 1 --timeout 2
uv run lfd --id LFD2 --heartbeat_freq 1 --timeout 2
uv run lfd --id LFD3 --heartbeat_freq 1 --timeout 2
uv run server --id S1
uv run server --id S2
uv run server --id S3
uv run client --id C1 --interval 1
uv run client --id C2 --interval 1
uv run client --id C3 --interval 1
```

`--heartbeat_freq` is in heartbeats per second (Hz); use the same value for the GFD and all three LFDs. `--timeout` is how many seconds the LFD waits for an ACK before declaring the server dead. `--interval` is the number of seconds between client requests.

### What happens at each step

1. The GFD starts and prints `GFD: 0 members`.
2. Each LFD registers with the GFD. The GFD and LFD windows start showing numbered heartbeats in both directions.
3. S1 starts and registers with LFD1. After the first successful heartbeat, LFD1 prints `LFD1: add replica S1` and the GFD prints `GFD: 1 member: S1`. The same happens for S2 and S3, ending with `GFD: 3 members: S1, S2, S3`.
4. Each client registers with the GFD, receives the member list, and opens a connection to every replica. It then loops continuously, printing a `Sent` line for each replica followed by `Received` for the replies. `request_num` is incremented after the first reply arrives, not after all three.
5. Each server window shows `Received`, `my_state` before, `my_state` after, and `Sending` for every request. `my_state` counts requests, so the three servers track each other, though they can differ briefly when requests from different clients arrive in a different order.
6. Press Ctrl-C in the S1 window. LFD1 prints `S1 has died` and `LFD1: delete replica S1`. The GFD prints `GFD: 2 members: S2, S3` and sends the new list to the clients, which print that they removed S1 and continue with two `Sent` lines per request.
7. After some time, press Ctrl-C in the S2 window. The same sequence happens again, ending with `GFD: 1 member: S3`.

Recovery of a dead replica, checkpointing, and the RM are not part of this milestone.

### LFD and GFD behavior

Each LFD registers with GFD before reporting a replica. The first successful
server heartbeat adds the replica; timeout or disconnection removes it.
The LFD keeps answering GFD heartbeats while its server is absent. If GFD is
unavailable, server monitoring continues; the LFD retries once per second
and reports current healthy membership after reconnecting.

Clients register with GFD before sending requests. GFD immediately sends the
current replica IDs and member count, then sends a new full snapshot whenever
membership changes. Both sides log each delivery. Clients use those IDs to
open or close replica connections; `--num_replicas` is a legacy option and does
not limit GFD membership.

Run the local integration tests with:
```bash
uv run python -m unittest discover -s tests -p 'test_gfd_integration.py' -v
```

The GFD channel uses these JSON-line messages (server heartbeats retain the M1 format):
```json
{"type":"register_lfd","lfd_id":"LFD1"}
{"type":"heartbeat","from":"GFD","to":"LFD1","count":1}
{"type":"heartbeat_ack","from":"LFD1","to":"GFD","count":1}
{"type":"add_replica","lfd_id":"LFD1","replica_id":"S1"}
{"type":"delete_replica","lfd_id":"LFD1","replica_id":"S1"}
{"type":"register_client","client_id":"C1"}
{"type":"membership","members":["S1","S2"],"member_count":2}
```

## Wire Protocol & Logging Format

### Message Framing
All network communication uses newline-delimited JSON payloads over TCP (\n framing)

```json
{"type": "request", "client_id": "C1", "replica_id": "S1", "request_num": 1, "payload": "Hello"}
{"type": "reply", "client_id": "C1", "replica_id": "S1", "request_num": 1, "state": 1}
```

Server heartbeats keep the M1 format:

```json
{"type": "registration", "replica_id": "S1"}
{"type": "heartbeat", "lfd_id": "LFD1", "count": 1}
{"type": "heartbeat_ack", "replica_id": "S1", "count": 1}
```

The GFD channel uses these JSON-line messages:

```json
{"type":"register_lfd","lfd_id":"LFD1"}
{"type":"heartbeat","from":"GFD","to":"LFD1","count":1}
{"type":"heartbeat_ack","from":"LFD1","to":"GFD","count":1}
{"type":"add_replica","lfd_id":"LFD1","replica_id":"S1"}
{"type":"delete_replica","lfd_id":"LFD1","replica_id":"S1"}
{"type":"register_client","client_id":"C1"}
{"type":"membership","members":["S1","S2"],"member_count":2}
```

### Protocol Tuple Specification
Per project requirements, every client-server exchange is logged and tracked via:

$$
\langle \text{client\_id}, \text{replica\_id}, \text{request\_num}, \text{payload/direction} \rangle
$$

### Color-Coded Log Visuals
Every line begins with a tag such as `[C1]` in that process's own color. A server and its LFD share a color (S1 and LFD1 are both orange), each client has its own color, and the GFD is white.

Console outputs are timestamped in UTC and the rest of the line is color-coded by message type:
- Yellow: Client/Server requests & replies (send/receive)
- Magenta: Heartbeat checks and acknowledgments
- Green: State processing updates (my_state)
- Blue: Registration events (`S1` $\rightarrow$ `LFD1`)
- Cyan: Membership changes
- Red: Process failures & socket tear-downs

## Project Milestones Roadmap
- Milestone #1: Single stateful server replica (`S1`), 3 clients, and `LFD1` heartbeat monitoring.
- Milestone #2 (Current): Active replication across 3 replicas (`S1`, `S2`, `S3`), `GFD` membership tracking, and client-side duplicate suppression.
- Milestone #3: Warm passive replication with primary-to-backup state checkpointing.
- Milestone #4: Integrated Fault-Tolerance infrastructure (`RM` + `GFD` + `LFD`s) with manual crash recovery.
- Milestone #5: Full fault-tolerance standard featuring multi-fault handling and automated `RM` recovery.

*AI was used as assistance in the making of this document*
