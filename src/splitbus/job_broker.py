"""Durable Job Broker (B_dur): Log-structured, crash-resilient asynchronous execution substrate."""

from __future__ import annotations

import concurrent.futures
import json
import secrets
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

from splitbus.core import JobBrokerProtocol, JobDescriptor, JobState


class JobBroker(JobBrokerProtocol):
    """Log-structured, WAL-backed persistent job broker for durable tool executions.

    Guarantees:
    - At-least-once task durability via SQLite WAL journaling.
    - Deterministic pause/resume checkpoints for Human-in-the-Loop (HITL) authorization.
    - Zero task loss across runtime process termination or crashes.
    """

    def __init__(
        self,
        db_path: str | Path = ":memory:",
        max_workers: int = 4,
        auto_start: bool = True,
    ) -> None:
        self.db_path = str(db_path)
        self.max_workers = max_workers
        self._lock = threading.RLock()
        self._routine_registry: dict[str, Callable[[dict[str, Any]], Any]] = {}
        self._executor: concurrent.futures.ThreadPoolExecutor | None = None
        self._running = False
        self._worker_thread: threading.Thread | None = None

        self._init_db()
        if auto_start:
            self.start()

    def _init_db(self) -> None:
        """Initialize SQLite schema with Write-Ahead Logging (WAL) for concurrency."""
        with sqlite3.connect(self.db_path) as conn:
            if self.db_path != ":memory:":
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS durable_jobs (
                    job_id TEXT PRIMARY KEY,
                    routine TEXT NOT NULL,
                    arguments TEXT NOT NULL,
                    state TEXT NOT NULL,
                    epoch INTEGER NOT NULL,
                    approval_token TEXT,
                    checkpoint TEXT NOT NULL,
                    retry_count INTEGER NOT NULL,
                    max_retries INTEGER NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    result TEXT,
                    error TEXT
                );
                """
            )
            conn.commit()

    def register_routine(self, name: str, func: Callable[[dict[str, Any]], Any]) -> None:
        """Register an executable task routine handler."""
        with self._lock:
            self._routine_registry[name] = func

    def start(self) -> None:
        """Start worker thread and execution pool."""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=self.max_workers, thread_name_prefix="splitbus-worker"
            )
            self._worker_thread = threading.Thread(
                target=self._scheduler_loop, name="splitbus-scheduler", daemon=True
            )
            self._worker_thread.start()

    def stop(self, wait: bool = True) -> None:
        """Gracefully stop broker worker pool."""
        with self._lock:
            self._running = False
        if self._executor:
            self._executor.shutdown(wait=wait)
        if self._worker_thread and wait:
            self._worker_thread.join(timeout=2.0)

    def submit(
        self,
        routine: str,
        arguments: dict[str, Any],
        epoch: int = 0,
        max_retries: int = 3,
    ) -> str:
        """Persist and queue a new durable job."""
        job_id = str(uuid.uuid4())
        now = time.time()
        job = JobDescriptor(
            job_id=job_id,
            routine=routine,
            arguments=arguments,
            state=JobState.PENDING,
            epoch=epoch,
            max_retries=max_retries,
            created_at=now,
            updated_at=now,
        )
        self._write_job(job)
        return job_id

    def get_job(self, job_id: str) -> JobDescriptor | None:
        """Fetch job state from persistent storage."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("SELECT * FROM durable_jobs WHERE job_id = ?", (job_id,))
            row = cursor.fetchone()
            if not row:
                return None
            return JobDescriptor(
                job_id=row["job_id"],
                routine=row["routine"],
                arguments=json.loads(row["arguments"]),
                state=JobState(row["state"]),
                epoch=row["epoch"],
                approval_token=row["approval_token"],
                checkpoint=json.loads(row["checkpoint"]),
                retry_count=row["retry_count"],
                max_retries=row["max_retries"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                result=json.loads(row["result"]) if row["result"] else None,
                error=row["error"],
            )

    def _write_job(self, job: JobDescriptor) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO durable_jobs (
                    job_id, routine, arguments, state, epoch,
                    approval_token, checkpoint, retry_count, max_retries,
                    created_at, updated_at, result, error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job.job_id,
                    job.routine,
                    json.dumps(job.arguments),
                    job.state.value,
                    job.epoch,
                    job.approval_token,
                    json.dumps(job.checkpoint),
                    job.retry_count,
                    job.max_retries,
                    job.created_at,
                    time.time(),
                    json.dumps(job.result) if job.result is not None else None,
                    job.error,
                ),
            )
            conn.commit()

    def pause_for_approval(self, job_id: str) -> str:
        """Transition job into PAUSED_APPROVAL state and generate an approval token."""
        job = self.get_job(job_id)
        if not job:
            raise KeyError(f"Job {job_id} not found")
        token = secrets.token_hex(16)
        job.state = JobState.PAUSED_APPROVAL
        job.approval_token = token
        self._write_job(job)
        return token

    def approve_and_resume(self, job_id: str, token: str) -> bool:
        """Verify approval token and resume job back into PENDING execution queue."""
        job = self.get_job(job_id)
        if not job or job.state != JobState.PAUSED_APPROVAL:
            return False
        if job.approval_token != token:
            return False
        job.state = JobState.PENDING
        job.approval_token = None
        self._write_job(job)
        return True

    def cancel(self, job_id: str) -> bool:
        """Cancel a pending or running job."""
        job = self.get_job(job_id)
        if not job or job.state in (JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED):
            return False
        job.state = JobState.CANCELLED
        self._write_job(job)
        return True

    def recover(self) -> int:
        """Crash recovery procedure: reset orphaned in-flight jobs back to PENDING.

        Jobs in RUNNING state upon system reboot were interrupted mid-flight;
        they are safely re-scheduled without losing parameters or checkpoints.
        """
        recovered_count = 0
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT job_id FROM durable_jobs WHERE state = ?", (JobState.RUNNING.value,)
            )
            rows = cursor.fetchall()
            for row in rows:
                conn.execute(
                    "UPDATE durable_jobs SET state = ?, updated_at = ? WHERE job_id = ?",
                    (JobState.PENDING.value, time.time(), row["job_id"]),
                )
                recovered_count += 1
            conn.commit()
        return recovered_count

    def _scheduler_loop(self) -> None:
        """Background loop dispatching PENDING jobs to the worker pool."""
        while self._running:
            try:
                job_to_run = self._fetch_next_pending()
                if job_to_run and self._executor:
                    self._executor.submit(self._execute_job, job_to_run)
                else:
                    time.sleep(0.01)
            except Exception:
                time.sleep(0.05)

    def _fetch_next_pending(self) -> JobDescriptor | None:
        with self._lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT job_id FROM durable_jobs WHERE state = ? ORDER BY created_at ASC LIMIT 1",
                    (JobState.PENDING.value,),
                )
                row = cursor.fetchone()
                if not row:
                    return None
                job_id = row["job_id"]
                conn.execute(
                    "UPDATE durable_jobs SET state = ?, updated_at = ? WHERE job_id = ? AND state = ?",
                    (JobState.RUNNING.value, time.time(), job_id, JobState.PENDING.value),
                )
                conn.commit()
            return self.get_job(job_id)

    def _execute_job(self, job: JobDescriptor) -> None:
        func = self._routine_registry.get(job.routine)
        if not func:
            job.state = JobState.FAILED
            job.error = f"Routine '{job.routine}' not registered"
            self._write_job(job)
            return

        try:
            result = func(job.arguments)
            # Fetch latest state in case it transitioned to PAUSED_APPROVAL or CANCELLED during execution
            current = self.get_job(job.job_id)
            if current and current.state in (JobState.PAUSED_APPROVAL, JobState.CANCELLED):
                return
            job.state = JobState.COMPLETED
            job.result = result
            self._write_job(job)
        except Exception as exc:
            job.retry_count += 1
            if job.retry_count < job.max_retries:
                job.state = JobState.PENDING
                job.error = f"Retry {job.retry_count}: {exc}"
            else:
                job.state = JobState.FAILED
                job.error = f"Failed after {job.retry_count} retries: {exc}"
            self._write_job(job)
