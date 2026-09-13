"""Comprehensive test suite for the splitbus package."""

import os
import tempfile
import time

from splitbus.baseline_unified import UnifiedMessageBus
from splitbus.bridge import SplitBusBridge
from splitbus.core import EpochFence, InteractionEvent, JobDescriptor, JobState
from splitbus.interaction_bus import InteractionBus
from splitbus.job_broker import JobBroker


def test_interaction_bus_exact_and_pattern_dispatch():
    bus = InteractionBus()
    exact_received = []
    pattern_received = []

    bus.subscribe("dialogue:utterance", lambda e: exact_received.append(e))
    bus.subscribe("dialogue:*", lambda e: pattern_received.append(e))

    event1 = InteractionEvent(topic="dialogue:utterance", payload={"text": "hello"})
    bus.publish(event1)

    assert len(exact_received) == 1
    assert len(pattern_received) == 1
    assert exact_received[0].payload["text"] == "hello"

    event2 = InteractionEvent(topic="dialogue:state", payload={"state": "listening"})
    bus.publish(event2)

    assert len(exact_received) == 1
    assert len(pattern_received) == 2


def test_epoch_fence_monotonicity_and_preemption():
    fence = EpochFence(initial_epoch=0)
    assert fence.current == 0
    assert fence.is_valid(0)

    # User barge-in triggers epoch increment
    next_epoch = fence.advance()
    assert next_epoch == 1
    assert fence.current == 1
    assert not fence.is_valid(0)
    assert fence.is_valid(1)


def test_job_broker_execution_and_completion():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "jobs.db")
        broker = JobBroker(db_path=db_path, max_workers=2)

        def square_routine(args):
            return {"result": args["x"] * args["x"]}

        broker.register_routine("square", square_routine)

        job_id = broker.submit("square", {"x": 7})
        assert job_id is not None

        # Wait for worker thread to process
        for _ in range(50):
            job = broker.get_job(job_id)
            if job and job.state == JobState.COMPLETED:
                break
            time.sleep(0.05)

        job = broker.get_job(job_id)
        assert job is not None
        assert job.state == JobState.COMPLETED
        assert job.result == {"result": 49}
        broker.stop()


def test_job_broker_hitl_pause_and_approval_resume():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "jobs.db")
        broker = JobBroker(db_path=db_path, max_workers=2)

        def echo_routine(args):
            return {"echo": args["msg"]}

        broker.register_routine("echo", echo_routine)

        job_id = broker.submit("echo", {"msg": "requires_permission"})
        # Immediately pause for human approval
        token = broker.pause_for_approval(job_id)
        assert len(token) > 0

        job = broker.get_job(job_id)
        assert job.state == JobState.PAUSED_APPROVAL

        # Invalid token rejection
        assert not broker.approve_and_resume(job_id, "wrong-token")
        assert broker.get_job(job_id).state == JobState.PAUSED_APPROVAL

        # Valid token approval
        assert broker.approve_and_resume(job_id, token)

        # Wait for completion
        for _ in range(50):
            job = broker.get_job(job_id)
            if job and job.state == JobState.COMPLETED:
                break
            time.sleep(0.05)

        job = broker.get_job(job_id)
        assert job.state == JobState.COMPLETED
        assert job.result == {"echo": "requires_permission"}
        broker.stop()


def test_job_broker_crash_recovery():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "crash_test.db")
        broker = JobBroker(db_path=db_path, max_workers=2, auto_start=False)

        # Simulate job running when daemon process crashed abruptly
        job_id = broker.submit("heavy_task", {"step": 1})
        with broker._lock:
            # Manually simulate in-flight RUNNING state before sudden crash
            import sqlite3

            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "UPDATE durable_jobs SET state = 'RUNNING' WHERE job_id = ?", (job_id,)
                )
                conn.commit()

        # Verify it was in RUNNING state
        assert broker.get_job(job_id).state == JobState.RUNNING

        # Simulate daemon reboot and crash recovery execution
        recovered = broker.recover()
        assert recovered == 1
        recovered_job = broker.get_job(job_id)
        assert recovered_job.state == JobState.PENDING


def test_split_bus_bridge_and_epoch_fencing():
    fence = EpochFence(initial_epoch=1)
    bus = InteractionBus(epoch_fence=fence)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "bridge_test.db")
        broker = JobBroker(db_path=db_path, max_workers=2, auto_start=False)
        bridge = SplitBusBridge(bus, broker, fence)

        completed_events = []
        fenced_events = []
        bus.subscribe("job:completed", lambda e: completed_events.append(e))
        bus.subscribe("job:fenced_out", lambda e: fenced_events.append(e))

        job = JobDescriptor(
            job_id="job_001", routine="test", epoch=1, state=JobState.COMPLETED, result={"ok": True}
        )

        # Valid epoch delivery
        assert bridge.notify_job_completed(job) is True
        assert len(completed_events) == 1

        # User interrupts! Epoch advances
        fence.advance()  # now epoch 2

        # Job with older epoch (1) completes now
        assert bridge.notify_job_completed(job) is False
        assert len(fenced_events) == 1
        assert fenced_events[0].payload["job_epoch"] == 1
        assert fenced_events[0].payload["current_epoch"] == 2


def test_unified_bus_baseline():
    unified = UnifiedMessageBus(worker_count=2)
    unified.start()

    received = []
    unified.subscribe("test:topic", lambda e: received.append(e))

    unified.publish(InteractionEvent(topic="test:topic", payload={"val": 42}))

    for _ in range(50):
        if len(received) > 0:
            break
        time.sleep(0.02)

    assert len(received) == 1
    assert received[0].payload["val"] == 42
    unified.stop()
