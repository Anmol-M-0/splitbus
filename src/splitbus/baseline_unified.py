"""Unified Message Bus Baseline: A well-specified single-bus baseline multiplexing real-time and durable tasks."""

from __future__ import annotations

import contextlib
import fnmatch
import queue
import threading
import time
from collections import defaultdict

from splitbus.core import InteractionEvent, InteractionHandler


class UnifiedMessageBus:
    """Conventional single-bus assistant baseline architecture.

    In a unified architecture, all event classes—including ephemeral voice/GUI frames
    and heavy, durable tool/routine invocations—are multiplexed over a single shared
    message bus queue and dispatcher thread pool (similar to classic WebSocket / IPC daemons).

    Under tool bursts, real-time events experience Head-of-Line (HoL) blocking and
    contention delay.
    """

    def __init__(self, maxsize: int = 10000, worker_count: int = 2) -> None:
        self._queue: queue.Queue[InteractionEvent] = queue.Queue(maxsize=maxsize)
        self._subscribers: dict[str, list[InteractionHandler]] = defaultdict(list)
        self._lock = threading.RLock()
        self._running = False
        self._worker_threads: list[threading.Thread] = []
        self._latency_samples: list[float] = []
        self.worker_count = worker_count

    def start(self) -> None:
        with self._lock:
            if self._running:
                return
            self._running = True
            for i in range(self.worker_count):
                t = threading.Thread(
                    target=self._worker_loop, name=f"unified-bus-worker-{i}", daemon=True
                )
                t.start()
                self._worker_threads.append(t)

    def stop(self, wait: bool = True) -> None:
        with self._lock:
            self._running = False
        # Unblock worker loops
        for _ in self._worker_threads:
            self._queue.put(InteractionEvent(topic="__shutdown__"))
        if wait:
            for t in self._worker_threads:
                t.join(timeout=2.0)
            self._worker_threads.clear()

    def subscribe(self, topic_pattern: str, handler: InteractionHandler) -> None:
        with self._lock:
            self._subscribers[topic_pattern].append(handler)

    def publish(self, event: InteractionEvent) -> None:
        """Enqueue event onto the unified multiplexed channel."""
        with contextlib.suppress(queue.Full):
            self._queue.put_nowait(event)

    def publish_sync(self, event: InteractionEvent) -> float:
        """Synchronously enqueue and measure round-trip queueing latency in milliseconds."""
        enqueued_at = time.monotonic()
        done_event = threading.Event()

        def _ack_handler(_: InteractionEvent) -> None:
            done_event.set()

        # Temporary subscription for completion ack
        ack_topic = f"__ack__:{event.event_id}"
        self.subscribe(ack_topic, _ack_handler)
        self.publish(event)
        # Notify completion
        self.publish(InteractionEvent(topic=ack_topic, event_id=event.event_id))

        done_event.wait(timeout=5.0)
        elapsed_ms = (time.monotonic() - enqueued_at) * 1000.0
        with self._lock:
            self._latency_samples.append(elapsed_ms)
        return elapsed_ms

    def _worker_loop(self) -> None:
        while self._running:
            try:
                event = self._queue.get(timeout=0.1)
            except queue.Empty:
                continue

            if event.topic == "__shutdown__":
                self._queue.task_done()
                break

            # Calculate transit / queueing latency
            queue_latency_ms = (time.monotonic() - event.timestamp) * 1000.0
            with self._lock:
                self._latency_samples.append(queue_latency_ms)
                if len(self._latency_samples) > 10000:
                    self._latency_samples.pop(0)

            # Dispatch to subscribers
            handlers_to_call: list[InteractionHandler] = []
            with self._lock:
                for pattern, handlers in self._subscribers.items():
                    if fnmatch.fnmatchcase(event.topic, pattern):
                        handlers_to_call.extend(handlers)

            for handler in handlers_to_call:
                with contextlib.suppress(Exception):
                    handler(event)

            self._queue.task_done()

    def record_latency(self) -> list[float]:
        with self._lock:
            return list(self._latency_samples)

    def clear_metrics(self) -> None:
        with self._lock:
            self._latency_samples.clear()
