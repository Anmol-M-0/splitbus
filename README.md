# SplitBus: Decoupled Communication Substrate for Reliable Desktop Agents

> **Zero-dependency Python runtime separating low-latency interactive dialogue from crash-resilient durable job execution.**

[![CI](https://github.com/VirindaAI/splitbus/actions/workflows/ci.yml/badge.svg)](https://github.com/VirindaAI/splitbus/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Checked with mypy](https://www.mypy-lang.org/static/mypy_badge.svg)](https://mypy-lang.org/)
[![Security: bandit](https://img.shields.io/badge/security-bandit-yellow.svg)](https://github.com/PyCQA/bandit)
[![Tests: 7/7 Passing](https://img.shields.io/badge/tests-7%2F7%20passing-brightgreen.svg)](tests/)
[![Paper: arXiv 2026](https://img.shields.io/badge/paper-arXiv%20Preprint-orange.svg)](#citation)

---

## 1. The Dual-Workload Dilemma

Personal AI desktop agents operate directly on user workstations, combining high-frequency interactive modalities with autonomous, long-running computational tools. Conventional assistant architectures multiplex both workloads across a single unified message bus, causing severe **head-of-line blocking** and degraded tail latencies:

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                          THE DUAL-WORKLOAD DILEMMA                          │
│                                                                             │
│   WORKLOAD A: Realtime Interaction            WORKLOAD B: Durable Execution │
│   - Voice Audio Frames & VAD                  - Multi-Hop Tool Execution    │
│   - Push-to-Talk & Hotkeys                    - Vector Memory Indexing      │
│   - GUI Visualizer Projections                - Web Scraping & Synthesis    │
│   - Barge-In Cancellation                     - Human Approval Checkpoints  │
│                                                                             │
│   Requirement: Ephemeral, <10ms Latency,      Requirement: Durable, ACKed,  │
│   Drop-on-Lag, Monotonic Preemption           WAL Log, Crash-Recoverable    │
└─────────────────────────────────────────────────────────────────────────────┘
```

`splitbus` resolves this dilemma by decomposing communication into two orthogonal substrates governed by distinct mathematical contracts:
1. **Interaction Bus ($\mathcal{B}_{\text{int}}$)**: In-memory, sub-millisecond dispatch for ephemeral audio, GUI, and user preemption events.
2. **Durable Job Broker ($\mathcal{B}_{\text{dur}}$)**: Log-structured, SQLite WAL-backed execution queue with deterministic Human-in-the-Loop (HITL) checkpoints and zero-loss crash recovery.
3. **Monotonic Epoch Fencing ($\mathcal{G}_{\text{epoch}}$)**: Guarantees that when a user barges in, stale background job completions cannot corrupt the active conversational turn.

---

## 2. Architectural Model

```text
                                  User Interaction
                            (Voice / GUI / Hotkeys / VAD)
                                         │
                                         ▼
                 ┌───────────────────────────────────────────────┐
                 │    INTERACTION BUS (B_int: In-Memory IPC)     │
                 │   - Ephemeral, Sub-Millisecond Dispatch       │
                 │   - Monotonic Epoch Fencing (G_epoch)         │
                 │   - Drop-on-Lag / Non-Persisted               │
                 └───────┬───────────────────────────────┬───────┘
                         │                               │
                [Realtime Events]              [Job Dispatch Bridge]
                         │                               │
                         ▼                               ▼
                 ┌───────────────┐               ┌───────────────┐
                 │ Interactive   │               │ Durable Job   │
                 │ Dialogue Core │               │ Gateway       │
                 └───────┬───────┘               └───────┬───────┘
                         │                               │
                         │                               ▼
                         │               ┌───────────────────────────────┐
                         │               │  DURABLE JOB BROKER (B_dur)   │
                         │               │  - SQLite WAL Journaling      │
                         │               │  - At-Least-Once Delivery     │
                         │               │  - Pause / Resume Checkpoints │
                         │               └───────┬───────────────┬───────┘
                         │                       │               │
                         ▼                       ▼               ▼
                 ┌───────────────┐       ┌───────────────┐ ┌─────────────┐
                 │ Audio / GUI   │       │ Worker Pool / │ │ Persistence │
                 │ Presentation  │       │ Tool Sandbox  │ │ Store (DB)  │
                 └───────────────┘       └───────────────┘ └─────────────┘
```

---

## 3. Quickstart

### Installation
`splitbus` is self-contained with **zero required external dependencies** beyond Python 3.10+:

```bash
git clone https://github.com/VirindaAI/splitbus.git
cd splitbus
pip install -e .
```

### Basic Example

```python
from splitbus import InteractionBus, JobBroker, SplitBusBridge, EpochFence, InteractionEvent

# 1. Initialize Epoch Fence and Interaction Bus
fence = EpochFence()
bus = InteractionBus(epoch_fence=fence)

# 2. Initialize Durable Job Broker (WAL-backed)
broker = JobBroker(db_path="jobs.db", max_workers=4)


def summarize_documents(args):
    return {"status": "success", "summary": f"Processed {args['doc_count']} documents"}


broker.register_routine("summarize", summarize_documents)

# 3. Connect Bridge
bridge = SplitBusBridge(bus, broker, fence)

# Subscribe to real-time events on B_int
bus.subscribe("job:completed", lambda e: print("Completed:", e.payload))

# Dispatch durable job from interaction layer
bus.publish(
    InteractionEvent(
        topic="task:request_durable",
        payload={"routine": "summarize", "arguments": {"doc_count": 42}},
    )
)
```

### Human-in-the-Loop Approval Checkpoint

```python
# Pause a consequential tool for human approval
job_id = broker.submit("delete_files", {"path": "/tmp/test"})
token = broker.pause_for_approval(job_id)

print(f"Job paused. Approval token: {token}")

# Later, upon explicit human authorization:
broker.approve_and_resume(job_id, token)
```

---

## 4. Reproducing Paper Benchmarks

The benchmark suite evaluates latency isolation and crash recovery under high synthetic load.

### Run Automated Unit Tests
```bash
pytest tests/ -v
```

### Run Latency Isolation Benchmark
Measures p50, p90, p95, and p99 interactive latency across 10, 25, 50, and 100 concurrent background tasks:
```bash
python benchmarks/bench_latency_isolation.py --runs 5 --output-dir data/
```

### Run Crash Recovery Benchmark
Simulates abrupt process termination mid-flight and measures Recovery Time Objective (RTO) and job loss rate:
```bash
python benchmarks/bench_crash_recovery.py --runs 5 --output-dir data/
```

### Generate Vector Figures and LaTeX Tables
```bash
python benchmarks/generate_paper_plots.py --data-dir data/ --output-dir figures/
```

This generates:
* `figures/latency_comparison.svg`: Vector plot of tail-latency isolation under background bursts.
* `figures/table_latency_isolation.tex`: Ready-to-publish LaTeX table.

### Developer & CI Tooling
Run the complete quality, linting, type-checking, and security pipeline:

```bash
# 1. Lint and code style formatting (Ruff)
ruff check .
ruff format --check .

# 2. Strict static type analysis (Mypy)
mypy src/

# 3. Security vulnerability analysis (Bandit)
bandit -r src/ -c .bandit.yaml

# 4. Automated unit and integration test suite (Pytest)
pytest tests/ -v
```

---

## 5. Empirical Results

| Architecture | Load (Tasks) | p50 Latency (ms) | p99 Latency (ms) | Recovery Time (RTO) | Job Loss Rate |
|---|---|---|---|---|---|
| **Split-Bus** | 100 | **0.08 ± 0.01** | **0.25 ± 0.04** | **18.2 ± 1.1 ms** | **0.00%** |
| Unified Bus | 100 | 2.14 ± 0.32 | 14.80 ± 2.15 | Unbounded | Unsafe |

---

## 6. Citation

If you use this architecture or artifact in your research, please cite:

```bibtex
@article{virindai2026splitbus,
  title={A Split-Bus Architecture for Reliable Desktop Agents: Separating Realtime Interaction from Durable Asynchronous Execution},
  author={VirindaAI Research Team},
  journal={arXiv preprint arXiv:2603.xxxxx},
  year={2026}
}
```

## 7. License

MIT License. Copyright (c) 2026 Anmol M. / VirindaAI Research.
