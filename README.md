# Fair Ordering Protocols — EC2 Experiment Deployment (Script-Based)

This repository contains implementations and experiment tooling for two order-fair consensus protocols, **Themis** and **AUTIG**.  
The current version supports **script-based deployment and execution** across multiple EC2 instances, replacing the previous manual per-instance setup.

## Protocols

- **Themis (Order-Fairness)** — located in `SpeedFair_simplify/`
- **AUTIG** — located in `utig/`

## Overview

Both protocols follow the same basic pattern:

**Themis:** Each replica periodically emits a LocalOrder (controlled by `-lo-interval` and `-lo-size`). The leader (ID 0) collects n−f orders, builds the dependency graph, prunes the shaded tail, and proposes. Replicas apply updates; a height is finalized once the graph becomes a tournament.

**AUTIG:** Uses similar consensus mechanics with the same configuration parameters and deployment process.

Experiment results (throughput and latency) are recorded to a log file on the **leader node**.

## Automation Scripts

The experiment workflow is fully automated using three PowerShell scripts:

- `1-prepare-experiment.ps1`  
  Prepares all EC2 instances by installing dependencies and cloning this repository.

- `run-speedfair-experiment.ps1`  
  Runs the **Themis** experiment across the cluster.

- `run-utig-experiment.ps1`  
  Runs the **AUTIG** experiment across the cluster.

These scripts eliminate the need for manual SSH sessions and ensure consistent configuration across experiments.

## Requirements

- Go 1.22+
- Multiple Linux EC2 instances (one node per instance recommended)
- All instances in the same VPC/subnet
- Security group rules allowing the experiment TCP port between all instances
- PowerShell (Windows PowerShell 5.1+ or PowerShell 7+) on the local machine
- SSH key access to all EC2 instances

## Quick Start

### Step 0 — Launch EC2 Instances

Launch the desired number of EC2 instances and ensure they can communicate over private IP addresses within the VPC.

### Step 1 — Prepare Environment

From your local machine, run:

```powershell
.\1-prepare-experiment.ps1

This script installs required packages and clones the repository on all EC2 instances.

## Step 2 — Run Experiments

Choose one of the following scripts depending on the protocol:

### Run Themis

```powershell
.\run-speedfair-experiment.ps1

### Run AUTIG

```powershell
.\run-utig-experiment.ps1

## Experiment Parameters

All experiment parameters are configured inside the run scripts.

You can modify them by editing the following variables (example):

```powershell
$paramF = 8
$paramGamma = 1
$paramLoInterval = 400
$paramLoSize = 50
$paramTxRate = 200
$paramSimDuration = 35
$leaderWaitDelay = 2


### Parameter Meanings

| Parameter | Description |
| :--- | :--- |
| `$paramF` | Tolerated Byzantine replicas ($f$) |
| `$paramGamma` | Fairness parameter $\gamma$ |
| `$paramLoInterval` | LocalOrder emission interval (ms) |
| `$paramLoSize` | Maximum transactions per LocalOrder |
| `$paramTxRate` | Synthetic transaction submission rate (leader only) |
| `$paramSimDuration` | Experiment duration (seconds) |
| `$leaderWaitDelay` | Delay before starting the leader to allow replicas to connect |

> **Note:** In the current implementation, only the leader process (node 0) generates client transactions.

## Output and Logs

Experiment output is not printed to the terminal. Instead, all metrics are written to a log file on the **leader node**:

`bft.log`

This file contains periodic measurements and final summaries, including:

* Total submitted transactions
* Total finalized transactions
* Throughput (TPS)
* Average latency

### Example final summary (excerpt from `bft.log`):

```text
--- FINAL RESULTS (Themis) ---
Total Submitted: 24000
Total Finalized: 10850
Throughput: 180.83 TPS
Average Latency: 574ms (from 10850 txs)
```

## Repository Structure (Themis)

```text
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

