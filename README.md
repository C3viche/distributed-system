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

Configure default hosts and ports for nodes using environment variables. Create a .env file in the project root:  

```python
# Server Replicas
S1_HOST="127.0.0.1"
S1_PORT=8080

S2_HOST="127.0.0.1"
S2_PORT=8082

S3_HOST="127.0.0.1"
S3_PORT=8083

# Local Fault Detectors
LFD1_HOST="127.0.0.1"
LFD1_PORT=8081
...
```

## Milestone #1 Execution Guide
Milestone #1 validates single-server communication and Local Fault Detector (LFD1) heartbeating.

Run each command in a separate terminal tab/window:
### 1. Launch Local Fault Detector (`LFD1`)

```bash
uv run python -m distributed_system.lfd.lfd --id LFD1 --freq 2
```

### 2. Launch Server Replica (`S1`)

```bash
uv run server --id S1
```
(On startup, S1 registers with LFD1 and begins answering periodic heartbeats)

### 3. Launch Independent Clients

```bash
# Client C1
uv run client --id C1

# Client C2
uv run client --id C2

# Client C3
uv run client --id C3
```

## Wire Protocol & Logging Format

### Message Framing
All network communication uses newline-delimited JSON payloads over TCP (\n framing)

```json
JSON{"type": "request", "client_id": "C1", "replica_id": "S1", "request_num": 1, "payload": "Hello"}
```

### Protocol Tuple Specification
Per project requirements, every client-server exchange is logged and tracked via:

$$
\langle \text{client\_id}, \text{replica\_id}, \text{request\_num}, \text{payload/direction} \rangle
$$

### Color-Coded Log Visuals
Console outputs are timestamped in UTC and color-coded by message type:
- Yellow: Client/Server requests & replies (send/receive)
- Magenta: Heartbeat checks and acknowledgments
- Green: State processing updates (my_state)
- Blue: Registration events (`S1` $\rightarrow$ `LFD1`)
- Red: Process failures & socket tear-downs

## Project Milestones Roadmap
- Milestone #1 (Current): Single stateful server replica (`S1`), 3 clients, and `LFD1` heartbeat monitoring.
- Milestone #2: Active replication across 3 replicas (`S1`, `S2`, `S3`), `GFD` membership tracking, and client-side duplicate suppression.
- Milestone #3: Warm passive replication with primary-to-backup state checkpointing.
- Milestone #4: Integrated Fault-Tolerance infrastructure (`RM` + `GFD` + `LFD`s) with manual crash recovery.
- Milestone #5: Full fault-tolerance standard featuring multi-fault handling and automated `RM` recovery.

*AI was used as assistance in the making of this document*
