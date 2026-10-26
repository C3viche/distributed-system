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

## Milestone #3 Execution Guide
Milestone #3 switches the system from active to warm-passive replication without
changing the fault-detection infrastructure. One replica, the primary, serves
all client requests. The other two are warm backups: alive and heartbeated by
their LFDs, but idle with respect to clients, kept up to date by periodic state
checkpoints from the primary over a dedicated secondary channel. The demo kills
two backups one at a time while clients keep running.

### Laptop division

Same four-laptop layout as Milestone #2 — primary is just a role, not a machine.

| Laptop | Processes |
|---|---|
| Replica 1 | LFD1, S1 (primary) |
| Replica 2 | LFD2, S2 (backup) |
| Replica 3 | LFD3, S3 (backup) |
| Client laptop | GFD, C1, C2, C3 |

S1 must come up before S2 and S3 so the primary can open its outbound
checkpoint channel to each backup as soon as the backups' listen sockets exist.
The primary retries missing backups once per second, so an out-of-order start
recovers on its own, but following the launch order avoids the "could not
reach" lines in the demo.

### `.env` additions for M3

On top of the Milestone #2 settings, every laptop needs these shared values:

```dotenv
REPLICATION_MODE=passive
PRIMARY_ID=S1
CHECKPOINT_FREQ=1
```

`REPLICATION_MODE` governs both client and server behaviour: `passive` makes
the server pick primary/backup roles from `PRIMARY_ID` and the client route
only to the primary. `CHECKPOINT_FREQ` is in Hz; `1` means one checkpoint per
second. `PRIMARY_ID` is explicit here so the demo is reproducible; the project
guide says the primary is "the first replica to show up," which S1 is in the
launch order below. The Replication Manager replaces this static assignment
in Milestone #4.

Exported shell variables still win over `.env`, and explicit CLI flags still
win over both.

### Using `run_all.sh`

The same commands work for M3; the mode is driven by `.env`:

```bash
# Client laptop, first
./run_all.sh gfd

# Replica laptops, S1 first, then S2 and S3 on the other laptops
./run_all.sh replica 1

# Client laptop, once the GFD window shows all three replicas
./run_all.sh clients
```

To run everything on one machine for localhost rehearsal with M3 defaults
(`REPLICATION_MODE=passive`, `PRIMARY_ID=S1`, `--checkpoint-freq` on every
server command so the frequency is a CLI parameter, not hardcoded):

```bash
./run_all.sh local m3
```

Kill scenarios for the demo. The project guide injects two consecutive
**backup** faults, one at a time, with time elapsed in between. S1 (the
primary) is never killed in Milestone #3.

```bash
./run_all.sh kill server 2     # first fault: kill backup S2
./run_all.sh kill server 3     # second fault: kill backup S3
./run_all.sh kill all          # tear everything down between runs
```

`CHECKPOINT_FREQ=2 ./run_all.sh local m3` speeds up checkpointing for debugging.

### Running the commands by hand

Run each command in a separate terminal tab/window:

```bash
uv run gfd --heartbeat_freq 1 --timeout 2
uv run lfd --id LFD1 --heartbeat_freq 1 --timeout 2
uv run lfd --id LFD2 --heartbeat_freq 1 --timeout 2
uv run lfd --id LFD3 --heartbeat_freq 1 --timeout 2
uv run server --id S1 --replication-mode passive --primary S1 --checkpoint-freq 1
uv run server --id S2 --replication-mode passive --primary S1
uv run server --id S3 --replication-mode passive --primary S1
uv run client --id C1 --replication-mode passive --interval 1
uv run client --id C2 --replication-mode passive --interval 1
uv run client --id C3 --replication-mode passive --interval 1
```

The project guide requires `heartbeat_freq` and `checkpoint_freq` as command-line
parameters, not hardcoded values. `--checkpoint-freq` is in Hz; `1` means one
checkpoint per second. `checkpoint_count` starts at 1 on every replica at
launch. Server flags use hyphens (`--replication-mode`, `--checkpoint-freq`);
underscore spellings are rejected. With `.env` set as above those flags are
optional overrides; `run_all.sh local m3` passes them explicitly so they show
up in each Terminal window. `--count N` on the client exits after N issued
requests, including unanswered ones.

### What happens at each step

Numbered to the Milestone #3 rubric in [the project guide](https://docs.google.com/document/d/1Ak8UT_oD9V5kxCQ8wkgjq97D_UdT3qHRVV6i9AHLKpc/edit).

1. **GFD starts** on the client laptop and prints `GFD: 0 members`.
2. **Each LFD registers with GFD** and the two heartbeat each other on the
   configured frequency. GFD and LFD windows show numbered sends and receipts.
3. **S1 launches** (primary by default because `PRIMARY_ID=S1` and it is first
   in the launch order). LFD1↔S1 heartbeats start; after the first successful
   heartbeat LFD1 prints `LFD1: add replica S1` and GFD prints
   `GFD: 1 member: S1`.
4. **S2 launches**. Same LFD2↔S2 flow; GFD prints `GFD: 2 members: S1, S2`.
5. **S3 launches**. Same LFD3↔S3 flow; GFD prints
   `GFD: 3 members: S1, S2, S3`.
6. **S1 opens the secondary channel**. S1 logs
   `S1 trying to open checkpoint channel to S2 at <host>:<port>`, then
   `S1 opened checkpoint channel to S2`, and the same two lines for S3. Each
   backup prints `S2 (backup): checkpoint channel from S1` once per connect.
7. **Clients launch one at a time**. Each client opens three TCP connections
   (one per GFD member) and prints `C1 connected to S1`, `… to S2`, `… to S3`.
8. **Clients send only to the primary**. Each client prints a single
   `Sent <C1, S1, 1, hello from C1 #1>` per request, since only S1 is addressed.
9. **S1 processes requests**. S1 logs `Received <C1, S1, 1, hello from C1 #1>`,
   then `my_state = 0 before processing`, then `my_state = 1 after processing`.
   S2 and S3 stay idle with respect to client traffic; if a client ever
   misroutes to a backup, that backup logs `S2 (backup): ignoring request
   from C1` and does not reply.
10. **S1 replies**. S1 logs `Sending <C1, S1, 1, reply>`; C1 logs
    `Received <C1, S1, 1, reply, state=1>`. Only one reply per request —
    no duplicates to suppress.
11. **S1 checkpoints every `1 / checkpoint_freq` seconds**. The checkpoint
    payload carries the primary's current `my_state` and `checkpoint_count`
    (the guide also calls this `checkpoint_num`). S1 logs
    `S1 sending checkpoint [k] to S2: my_state=X` and the same line for S3,
    then increments `checkpoint_count`. Both replicas start at
    `checkpoint_count = 1`.
12. **Backups apply each checkpoint**. S2 and S3 log
    `S2 received checkpoint from S1: my_state = X, checkpoint_count = k`
    and overwrite their own `my_state` and `checkpoint_count` from the message.
    They stay idle on client traffic while this is happening.
13. At this point the fault-free passive system is up and tolerating two
    simultaneous faults. The next steps inject them one at a time.
14. **Kill backup S2** (`./run_all.sh kill server 2`, or Ctrl-C in its window).
    LFD2 times out waiting for an ACK and prints `S2 has died`.
15. **LFD2 reports to GFD**. LFD2 sends `LFD2: delete replica S2`; GFD prints
    `GFD: 2 members: S1, S3` and broadcasts the new snapshot to the clients.
16. **Clients keep sending to S1 without pause**. Each client logs the
    membership update, drops its connection to S2, and continues printing
    `Sent <Cx, S1, N, ...>` on its interval.
17. **S1 keeps replying**. `Received`, `my_state` before/after, and `Sending`
    lines on S1 are unchanged; C1/C2/C3 keep logging matching `Received` lines.
18. **Checkpoints to the surviving backup continue**. S1 keeps logging
    `S1 sending checkpoint [k] to S3: my_state=X` and S3 keeps logging
    `S3 received checkpoint from S1: ...`. After some elapsed time, kill S3 as
    well: GFD prints `GFD: 1 member: S1`, and S1 starts logging
    `S1: no backups connected, checkpoint [k] skipped` instead of sending.
    Clients keep exchanging requests and replies with S1 throughout.

### What Priya will look for

Copied from the Milestone #3 rubric in [the project guide](https://docs.google.com/document/d/1Ak8UT_oD9V5kxCQ8wkgjq97D_UdT3qHRVV6i9AHLKpc/edit):

- Membership-change messages printed at the GFD (`GFD: 0 members` through
  `GFD: 3 members: S1, S2, S3`, then `GFD: 2 members: S1, S3`, then
  `GFD: 1 member: S1`).
- Fault-detection messages printed at the LFDs (`S2 has died`) and relayed to
  the GFD (`LFD2: delete replica S2`).
- All replies to each client come from only one replica, the primary (S1).
- Checkpoint send printed at S1 and receipt printed at S2/S3, with `my_state`
  and `checkpoint_count` on every line.
- Two consecutive backup faults, one at a time, with time elapsed in between
  (kill S2, then kill S3). Clients keep looping on S1 with no pause.

What she will not look for (out of scope for Milestone #3):

- Automated recovery of a dead backup.
- Crash of the primary replica.

Primary re-election, state transfer on relaunch, `i_am_ready`, and the RM are
Milestone #4.

### Client routing details

Each client opens one TCP connection per GFD member and keeps all three open
even though only the primary is addressed. Routing follows this precedence:

1. An explicit non-null `primary_id` in a `membership` snapshot from GFD.
2. The configured `PRIMARY_ID` (or `--primary`), used until GFD overrides it.

GFD does not yet publish `primary_id`, so clients stay on `PRIMARY_ID` for all
of Milestone #3. A membership snapshot that omits `primary_id` preserves the
previous assignment; `null` clears it. An explicit assignment must name a
member listed in the same snapshot, otherwise the whole snapshot is rejected
and the previous view is kept.

The optional field format, for Milestone #4:

```json
{"type":"membership","members":["S1","S2","S3"],"member_count":3,"primary_id":"S2"}
```

A client request without a reply within its timeout is logged and the next
request uses a fresh number; requests are not replayed because the current
server does not deduplicate operations, and late replies are discarded. If the
primary is unreachable, the client keeps looping, retries its connections on a
one-second backoff, and waits for either reconnection or a GFD snapshot that
changes the assignment. Clients never promote a backup themselves.

### Tests

Localhost unit and integration tests:

```bash
uv run python -m unittest discover -s tests -v
```

### Four-laptop rehearsal checklist

Do this on the demo day Wi-Fi with VPNs off and a shared `.env` of real LAN IPs. The localhost dry-run already walked the full rubric; this is the same sequence on four machines.

1. One person fills `.env` from `.env.example` (`REPLICATION_MODE=passive`, `PRIMARY_ID=S1`, `CHECKPOINT_FREQ=1`) and shares it. Confirm `ipconfig getifaddr en0` on each laptop.
2. Client laptop: `./run_all.sh gfd`. GFD window must show `GFD: 0 members` before anyone else starts.
3. Replica 1: `./run_all.sh replica 1`. Wait for `LFD1: add replica S1` and `GFD: 1 member: S1`.
4. Replica 2, then replica 3: `./run_all.sh replica 2` / `replica 3`. GFD should end at `GFD: 3 members: S1, S2, S3`. S1 should log `opened checkpoint channel to S2` and `to S3`.
5. Client laptop: `./run_all.sh clients` (C1, then C2, then C3, one at a time). Each client logs three connections, then only `Sent`/`Received` tuples naming S1. S2 and S3 keep printing checkpoint receipts (`my_state` and `checkpoint_count`) and no client replies.
6. Wait ~15–30 seconds so several checkpoint `[k]` lines are visible on S1, S2, and S3. Screenshot or record each window.
7. First fault: `./run_all.sh kill server 2` (or Ctrl-C in the S2 window — not `kill` on the `uv` wrapper, which can leave the Python child running). Confirm `S2 has died`, `LFD2: delete replica S2`, `GFD: 2 members: S1, S3`, clients keep `Sent`/`Received` with S1, and S1 checkpoints only S3.
8. Wait another 15–30 seconds, then `./run_all.sh kill server 3`. Confirm `S3 has died`, `GFD: 1 member: S1`, `S1: no backups connected, checkpoint [k] skipped`, and clients still looping on S1.
9. `./run_all.sh kill all` between runs. Optional defensive run: start S2 before S1 and confirm S1's one-second retry opens the checkpoint channel once S2 appears.

The `S2 (backup): ignoring request from C1` line only appears if a client misroutes. In a correct demo it stays quiet because clients send only to S1.

### Demo talking points (PM / Q&A)

- **Primary designation.** `PRIMARY_ID=S1` in `.env` is read by both the server (role = primary vs backup) and the clients (who to send to). The project guide says the primary is "the first replica to show up"; we make that explicit and launch S1 first. The Replication Manager replaces this static assignment in Milestone #4.
- **Secondary channel.** The primary dials each backup on the backup's existing listen port (two TCP connections, S1→S2 and S1→S3). The first message is `register_primary`, which retags that socket so later `checkpoint` payloads are applied instead of being treated as client requests. `checkpoint_freq` is a CLI parameter (`--checkpoint-freq`, default 1 Hz); `checkpoint_count` starts at 1 and is sent with `my_state` on every checkpoint. The timer lives in the same `select` loop (no threads).
- **Why backups still accept client connections.** Rubric step 7 requires each client to open three TCP connections. Backups accept the socket, ignore any request (`S2 (backup): ignoring request from C1`), and do not reply. Clients never promote a backup themselves.
- **What this demo does not show.** Primary crash, primary re-election, recovery of a dead backup, state transfer on relaunch, and the RM. Those are Milestone #4. GFD does not yet publish `primary_id`, so clients stay on the configured `PRIMARY_ID` for the whole demo.
- **Log colors to watch.** Yellow = client/server request/reply; magenta = heartbeats; green = `my_state`; blue = registration / checkpoint-channel open; cyan = membership; light cyan = checkpoint send/receive; red = deaths and socket tear-downs. Confirm on each teammate's terminal that light cyan is distinct from heartbeat magenta.

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

In passive mode (Milestone #3) a `membership` snapshot may additionally carry
an optional `primary_id`:

```json
{"type":"membership","members":["S1","S2","S3"],"member_count":3,"primary_id":"S1"}
```

The primary-to-backup secondary channel adds two message types:

```json
{"type":"register_primary","replica_id":"S1"}
{"type":"checkpoint","replica_id":"S1","my_state":7,"checkpoint_count":3}
```

`register_primary` is the first message on each outbound socket the primary
opens to a backup; it reclassifies the socket from "client" to "primary" on
the backup side so later `checkpoint` messages are applied instead of being
treated as client requests.

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
- Light cyan: Checkpoint send and receive (primary $\leftrightarrow$ backups)
- Red: Process failures & socket tear-downs

## Project Milestones Roadmap
- Milestone #1: Single stateful server replica (`S1`), 3 clients, and `LFD1` heartbeat monitoring.
- Milestone #2: Active replication across 3 replicas (`S1`, `S2`, `S3`), `GFD` membership tracking, and client-side duplicate suppression.
- Milestone #3 (Current): Warm passive replication with primary-to-backup state checkpointing.
- Milestone #4: Integrated Fault-Tolerance infrastructure (`RM` + `GFD` + `LFD`s) with manual crash recovery.
- Milestone #5: Full fault-tolerance standard featuring multi-fault handling and automated `RM` recovery.

*AI was used as assistance in the making of this document*
