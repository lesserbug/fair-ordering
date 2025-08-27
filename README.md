# Fair ordering Protocols — 5-Node EC2 Deployment Guide

This repository provides a guide for running both Themis and AUTIG consensus protocol implementations across 5 EC2 instances (one node per instance).

## Protocols

- **Themis (Order-Fairness)** - Located in `SpeedFair_simplify/` folder
- **AUTIG** - Located in `utig/` folder

## Overview

Both protocols follow the same basic pattern:

**Themis:** Each replica periodically emits a LocalOrder (controlled by `-lo-interval` and `-lo-size`). The leader (ID 0) collects n−f orders, builds the dependency graph, prunes the shaded tail, and proposes. Replicas apply updates; a height is finalized once the graph becomes a tournament.

**AUTIG:** Uses similar consensus mechanics with the same configuration parameters and deployment process.

The program prints TPS and average latency during the run and a summary at the end.

## Requirements

- Go 1.22+
- 5 Linux EC2 instances (same VPC/subnet recommended)
- Security group rules that allow the chosen TCP port between all instances
- The same `config.json` present on every instance

## Setup Instructions

### 1. Create config.json

Create a configuration file listing all replicas (0..4). Use private VPC IPs (do not use 0.0.0.0 or 127.0.0.1).
Since we run one node per instance, we reuse port 8000 on each host.

```json
{
  "nodes": {
    "0": "10.0.0.10:8000",
    "1": "10.0.1.11:8000",
    "2": "10.0.2.12:8000",
    "3": "10.0.3.13:8000",
    "4": "10.0.4.14:8000"
  }
}
```

**Notes:**
- Node 0 is the leader (assumed by the code)
- All 5 entries must appear in every instance's `config.json`
- If you later colocate multiple nodes on one machine, give them distinct ports

### 2. Configure Security Group

**Inbound:** Allow TCP 8000 from the same security group (or from the 5 private IPs / VPC CIDR).

**Outbound:** Allow all (default is fine).

### 3. Install Go & Clone Repository

Run on each EC2 instance:

```bash
sudo apt-get update -y && sudo apt-get install -y git
# Install Go 1.22+ via your preferred method (snap/tarball/pkg manager)
git clone <YOUR_REPO_URL> consensus-protocols
cd consensus-protocols
```

#### For Themis:

```bash
cd SpeedFair_simplify
```

Optionally build the binary:

```bash
go build -o themis
# You can also run without building: `go run main.go ...`
```

#### For AUTIG:

```bash
cd utig
```

Optionally build the binary:

```bash
go build -o autig
# You can also run without building: `go run main.go ...`
```

### 4. Run the Cluster

We use parameters: n=5, f=1, γ=1, lo-interval=250ms, lo-size=100, tx-rate=400, sim-duration=60s.

**Important:** Per the current code, only the leader process (node 0) emits client transactions.

You can use either `go run` (no build) or the built binary.

#### Running Themis

Navigate to the `SpeedFair_simplify/` directory on each instance, then run:

**Instance A (10.0.0.10) — Node 0 (Leader):**

```bash
go run main.go -config="config.json" -nodes="0" \
  -f=1 -gamma=1 \
  -lo-interval=250 -lo-size=100 \
  -tx-rate=400 \
  -sim-duration=60
```

**Instance B (10.0.1.11) — Node 1:**

```bash
go run main.go -config="config.json" -nodes="1" \
  -f=1 -gamma=1 \
  -lo-interval=250 -lo-size=100 \
  -tx-rate=400 \
  -sim-duration=60
```

**Instance C (10.0.2.12) — Node 2:**

```bash
go run main.go -config="config.json" -nodes="2" \
  -f=1 -gamma=1 \
  -lo-interval=250 -lo-size=100 \
  -tx-rate=400 \
  -sim-duration=60
```

**Instance D (10.0.3.13) — Node 3:**

```bash
go run main.go -config="config.json" -nodes="3" \
  -f=1 -gamma=1 \
  -lo-interval=250 -lo-size=100 \
  -tx-rate=400 \
  -sim-duration=60
```

**Instance E (10.0.4.14) — Node 4:**

```bash
go run main.go -config="config.json" -nodes="4" \
  -f=1 -gamma=1 \
  -lo-interval=250 -lo-size=100 \
  -tx-rate=400 \
  -sim-duration=60
```

#### Running AUTIG

Navigate to the `utig/` directory on each instance, then run the same commands as above. AUTIG uses identical configuration parameters and command structure.

## Expected Output

After "waiting for peers…", processes begin printing periodic metrics:

```
[12.0s] Finalized: 2150 | Submitted: 4800 | TPS: 179.18 |
Latency: 560ms | Pool: 940 | LastOrder: 101
```

At the end:

```
--- FINAL RESULTS (Themis) ---
Total Submitted: 24000
Total Finalized: 10850
Throughput: 180.83 TPS
Average Latency: 574ms (from 10850 txs)
```

## Command Line Flags

| Flag | Type | Description |
|------|------|-------------|
| `-config` | string | Path to JSON config of node addresses |
| `-nodes` | string | IDs to run on this instance, e.g. "0" |
| `-f` | uint | Tolerated Byzantine replicas (for 5 nodes set to 1) |
| `-gamma` | float | Fairness parameter γ (e.g., 0.9) |
| `-lo-interval` | ms | LocalOrder emission period |
| `-lo-size` | int | Max transactions per LocalOrder |
| `-tx-rate` | tx/s | Synthetic client submission rate (effective on leader process) |
| `-sim-duration` | s | Run duration |
| `-cpuprofile` | bool | Write a local CPU profile (themis_cpu_nodes_*.pprof) |


## Repository Structure (Themis)

```
├── main.go                          # Entry point & runner
├── pkg/
│   ├── network/
│   │   └── network.go              # TCP network layer
│   ├── ofo/
│   │   ├── service.go              # Leader/replica service & pipeline
│   │   └── dependency.go           # Graph build, SCC, updates, finalize
│   └── types/
│       └── types.go                # Types & messages
└── config.json                     # Cluster addresses
```
