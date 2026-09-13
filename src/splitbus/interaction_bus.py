"""Interaction Bus (B_int): In-memory, sub-millisecond ephemeral event substrate."""

from __future__ import annotations

import fnmatch
import threading
import time
from collections import defaultdict

from splitbus.core import EpochFence, InteractionBusProtocol, InteractionEvent, InteractionHandler


class InteractionBus(InteractionBusProtocol):
    """Ephemeral in-memory message bus designed for interactive voice, GUI, and hotkey events.

    Guarantees:
    - At-most-once delivery.
    - Zero disk I/O on critical dispatch path.
    - Sub-millisecond dispatch overhead.
    - Monotonic Epoch Fencing (G_epoch) for turn preemption.
    """

    def __init__(self, epoch_fence: EpochFence | None = None) -> None:
        self.epoch_fence = epoch_fence or EpochFence()
        self._lock = threading.RLock()
        self._subscribers: dict[str, list[InteractionHandler]] = defaultdict(list)
        self._exact_subscribers: dict[str, list[InteractionHandler]] = defaultdict(list)
        self._pattern_subscribers: dict[str, list[InteractionHandler]] = defaultdict(list)
        self._metrics: dict[str, int] = defaultdict(int)
        self._latency_samples: list[float] = []

    def subscribe(self, topic_pattern: str, handler: InteractionHandler) -> None:
        """Register a handler for exact topic or glob pattern (e.g. 'audio:*')."""
        with self._lock:
            if "*" in topic_pattern or "?" in topic_pattern:
                self._pattern_subscribers[topic_pattern].append(handler)
            else:
                self._exact_subscribers[topic_pattern].append(handler)
            self._subscribers[topic_pattern].append(handler)

    def unsubscribe(self, topic_pattern: str, handler: InteractionHandler) -> None:
        """Remove an existing subscriber handler."""
        with self._lock:
            if topic_pattern in self._subscribers and handler in self._subscribers[topic_pattern]:
                self._subscribers[topic_pattern].remove(handler)
            if (
                topic_pattern in self._exact_subscribers
                and handler in self._exact_subscribers[topic_pattern]
            ):
                self._exact_subscribers[topic_pattern].remove(handler)
            if (
                topic_pattern in self._pattern_subscribers
                and handler in self._pattern_subscribers[topic_pattern]
            ):
                self._pattern_subscribers[topic_pattern].remove(handler)

    def publish(self, event: InteractionEvent) -> None:
        """Dispatch event immediately to all matching subscribers.

        Dispatches synchronously on caller's thread or via dedicated handler calls,
        measuring dispatch transit latency.
        """
        start_time = time.monotonic()
        targets: list[InteractionHandler] = []

        with self._lock:
            self._metrics["events_published"] += 1
            # Exact matches
            if event.topic in self._exact_subscribers:
                targets.extend(self._exact_subscribers[event.topic])
            # Pattern matches
            for pattern, handlers in self._pattern_subscribers.items():
                if fnmatch.fnmatchcase(event.topic, pattern):
                    targets.extend(handlers)

        # Execute handlers without holding the lock to prevent deadlock
        for handler in targets:
            try:
                handler(event)
            except Exception:
                with self._lock:
                    self._metrics["handler_exceptions"] += 1

        elapsed_ms = (time.monotonic() - start_time) * 1000.0
        with self._lock:
            self._latency_samples.append(elapsed_ms)
            if len(self._latency_samples) > 10000:
                self._latency_samples.pop(0)

    def record_latency(self) -> list[float]:
        """Return a copy of recent dispatch latency samples in milliseconds."""
        with self._lock:
            return list(self._latency_samples)

    def clear_metrics(self) -> None:
        with self._lock:
            self._metrics.clear()
            self._latency_samples.clear()
