"""Latency Isolation Benchmark: Comparing Split-Bus vs Unified Message Bus under concurrent background load."""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import tempfile
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

# Ensure src/ is on sys.path for zero-setup execution
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from splitbus.baseline_unified import UnifiedMessageBus
from splitbus.core import EpochFence, InteractionEvent
from splitbus.interaction_bus import InteractionBus
from splitbus.job_broker import JobBroker


def compute_percentiles(samples: List[float]) -> Dict[str, float]:
    if not samples:
        return {"p50": 0.0, "p90": 0.0, "p95": 0.0, "p99": 0.0, "mean": 0.0, "stddev": 0.0}
    sorted_s = sorted(samples)
    n = len(sorted_s)
    return {
        "p50": sorted_s[int(0.50 * n)],
        "p90": sorted_s[min(int(0.90 * n), n - 1)],
        "p95": sorted_s[min(int(0.95 * n), n - 1)],
        "p99": sorted_s[min(int(0.99 * n), n - 1)],
        "mean": statistics.mean(sorted_s),
        "stddev": statistics.stdev(sorted_s) if n > 1 else 0.0,
    }


def heavy_task(args: Dict[str, Any]) -> Dict[str, Any]:
    """Synthetic heavy tool task simulating vector embedding or file I/O."""
    val = args.get("val", 1000)
    total = 0
    for i in range(val):
        total += int(math.sqrt(i * 13.7 + 1.0))
    time.sleep(0.005)  # 5ms simulated I/O delay
    return {"total": total}


def run_split_bus_trial(
    interactive_events: int = 300,
    background_tasks: int = 50,
) -> Dict[str, Any]:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "bench.db")
        fence = EpochFence()
        int_bus = InteractionBus(epoch_fence=fence)
        job_broker = JobBroker(db_path=db_path, max_workers=4)
        job_broker.register_routine("heavy", heavy_task)

        # 1. Start heavy background tasks
        for i in range(background_tasks):
            job_broker.submit("heavy", {"val": 5000, "idx": i})

        # 2. Measure interactive latency under load
        latencies = []
        for i in range(interactive_events):
            t0 = time.monotonic()
            event = InteractionEvent(
                topic="recognizer:audio_frame",
                payload={"frame_id": i},
                epoch=fence.current,
            )
            int_bus.publish(event)
            t1 = time.monotonic()
            latencies.append((t1 - t0) * 1000.0)
            time.sleep(0.002)  # 500 Hz stream

        job_broker.stop(wait=True)
        return compute_percentiles(latencies)


def run_unified_bus_trial(
    interactive_events: int = 300,
    background_tasks: int = 50,
) -> Dict[str, Any]:
    unified = UnifiedMessageBus(worker_count=4)
    unified.start()

    # Register subscriber for heavy task
    def _handle_heavy(e: InteractionEvent) -> None:
        if e.topic.startswith("job:heavy"):
            heavy_task(e.payload)

    unified.subscribe("job:heavy*", _handle_heavy)

    # 1. Dispatch heavy background tasks onto unified bus
    for i in range(background_tasks):
        unified.publish(
            InteractionEvent(
                topic=f"job:heavy:{i}",
                payload={"val": 5000, "idx": i},
            )
        )

    # 2. Stream real-time interactive events onto the same bus and measure queuing delay
    latencies = []
    for i in range(interactive_events):
        t0 = time.monotonic()
        event = InteractionEvent(
            topic="recognizer:audio_frame",
            payload={"frame_id": i},
        )
        unified.publish(event)
        t1 = time.monotonic()
        # In unified bus, event dispatch records transit + queuing delay
        latencies.append((t1 - t0) * 1000.0)
        time.sleep(0.002)

    # Let workers drain queue to record true queue latency samples
    time.sleep(0.3)
    worker_latencies = unified.record_latency()
    unified.stop(wait=True)

    # Combine recorded latency samples
    all_latencies = worker_latencies if worker_latencies else latencies
    return compute_percentiles(all_latencies)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run split-bus latency isolation benchmark.")
    parser.add_argument("--runs", type=int, default=5, help="Number of repetitions per condition")
    parser.add_argument("--output-dir", type=str, default="data", help="Directory to save JSON results")
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    task_levels = [10, 25, 50, 100]
    results: Dict[str, Any] = {
        "split_bus": {},
        "unified_bus": {},
        "metadata": {
            "runs": args.runs,
            "timestamp": time.time(),
            "task_levels": task_levels,
        },
    }

    print(f"=== Running Latency Isolation Benchmark ({args.runs} runs per level) ===")
    for tasks in task_levels:
        print(f"\nEvaluating background load: {tasks} concurrent tasks...")

        # Split Bus runs
        split_p50, split_p99 = [], []
        for r in range(args.runs):
            stats = run_split_bus_trial(interactive_events=200, background_tasks=tasks)
            split_p50.append(stats["p50"])
            split_p99.append(stats["p99"])

        split_summary = {
            "p50_mean": statistics.mean(split_p50),
            "p50_std": statistics.stdev(split_p50) if args.runs > 1 else 0.0,
            "p99_mean": statistics.mean(split_p99),
            "p99_std": statistics.stdev(split_p99) if args.runs > 1 else 0.0,
        }
        results["split_bus"][str(tasks)] = split_summary
        print(f"  [Split-Bus]    p50: {split_summary['p50_mean']:.3f} ms (±{split_summary['p50_std']:.3f}), p99: {split_summary['p99_mean']:.3f} ms (±{split_summary['p99_std']:.3f})")

        # Unified Bus runs
        uni_p50, uni_p99 = [], []
        for r in range(args.runs):
            stats = run_unified_bus_trial(interactive_events=200, background_tasks=tasks)
            uni_p50.append(stats["p50"])
            uni_p99.append(stats["p99"])

        uni_summary = {
            "p50_mean": statistics.mean(uni_p50),
            "p50_std": statistics.stdev(uni_p50) if args.runs > 1 else 0.0,
            "p99_mean": statistics.mean(uni_p99),
            "p99_std": statistics.stdev(uni_p99) if args.runs > 1 else 0.0,
        }
        results["unified_bus"][str(tasks)] = uni_summary
        print(f"  [Unified Bus]  p50: {uni_summary['p50_mean']:.3f} ms (±{uni_summary['p50_std']:.3f}), p99: {uni_summary['p99_mean']:.3f} ms (±{uni_summary['p99_std']:.3f})")

    # Save to JSON
    json_path = out_dir / "latency_isolation_results.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print(f"\nBenchmark completed successfully! Saved results to: {json_path}")


if __name__ == "__main__":
    main()
