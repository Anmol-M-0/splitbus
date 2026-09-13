"""Core domain models, state enums, and mathematical contracts for the Split-Bus architecture."""

from __future__ import annotations

import enum
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Protocol, runtime_checkable


class JobState(str, enum.Enum):
    """Lifecycle states of a durable job in the execution broker."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    PAUSED_APPROVAL = "PAUSED_APPROVAL"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


@dataclass(frozen=True)
class InteractionEvent:
    """Ephemeral, low-latency event dispatched over the Interaction Bus (B_int).

    Attributes:
        topic: Hierarchical event channel (e.g. 'recognizer:audio_frame', 'dialogue:utterance').
        payload: Event data dictionary.
        epoch: Monotonic epoch counter for preemption gating.
        timestamp: Monotonic timestamp in seconds when the event was dispatched.
        event_id: Unique event identifier.
    """
    topic: str
    payload: Dict[str, Any] = field(default_factory=dict)
    epoch: int = 0
    timestamp: float = field(default_factory=time.monotonic)
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))


@dataclass
class JobDescriptor:
    """Persistent unit of execution managed by the Durable Job Broker (B_dur).

    Attributes:
        job_id: Unique deterministic job identifier.
        routine: Target tool, function, or routine name to execute.
        arguments: Inputs passed to the routine.
        state: Current lifecycle state.
        epoch: Interaction epoch active when job was dispatched.
        approval_token: Cryptographic or random token required to resume from PAUSED_APPROVAL.
        checkpoint: Arbitrary state dictionary persisted across crashes.
        retry_count: Number of execution attempts completed.
        max_retries: Maximum permitted retries before marking as FAILED.
        created_at: Wall-clock epoch timestamp of creation.
        updated_at: Wall-clock epoch timestamp of last state mutation.
        result: Execution output upon completion.
        error: Error message upon failure.
    """
    job_id: str
    routine: str
    arguments: Dict[str, Any] = field(default_factory=dict)
    state: JobState = JobState.PENDING
    epoch: int = 0
    approval_token: Optional[str] = None
    checkpoint: Dict[str, Any] = field(default_factory=dict)
    retry_count: int = 0
    max_retries: int = 3
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    result: Optional[Any] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "routine": self.routine,
            "arguments": self.arguments,
            "state": self.state.value,
            "epoch": self.epoch,
            "approval_token": self.approval_token,
            "checkpoint": self.checkpoint,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "result": self.result,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> JobDescriptor:
        return cls(
            job_id=data["job_id"],
            routine=data["routine"],
            arguments=data.get("arguments", {}),
            state=JobState(data.get("state", JobState.PENDING.value)),
            epoch=data.get("epoch", 0),
            approval_token=data.get("approval_token"),
            checkpoint=data.get("checkpoint", {}),
            retry_count=data.get("retry_count", 0),
            max_retries=data.get("max_retries", 3),
            created_at=data.get("created_at", time.time()),
            updated_at=data.get("updated_at", time.time()),
            result=data.get("result"),
            error=data.get("error"),
        )


class EpochFence:
    """Monotonic Epoch Fence (G_epoch) to enforce turn isolation and instant preemption.

    Whenever a user interrupts (barge-in) or initiates a new conversational turn,
    the epoch increments monotonically:
        epoch_{n+1} = epoch_n + 1
    Any background job running with epoch < epoch_current is disqualified from
    mutating interactive dialogue state or synthesizing output.
    """

    def __init__(self, initial_epoch: int = 0) -> None:
        self._epoch = initial_epoch

    @property
    def current(self) -> int:
        return self._epoch

    def advance(self) -> int:
        """Advance epoch counter monotonically upon barge-in or turn reset."""
        self._epoch += 1
        return self._epoch

    def is_valid(self, candidate_epoch: int) -> bool:
        """Verify if a given epoch matches the current active fence."""
        return candidate_epoch == self._epoch


InteractionHandler = Callable[[InteractionEvent], None]


@runtime_checkable
class InteractionBusProtocol(Protocol):
    """Formal protocol for the ephemeral Interaction Bus (B_int)."""

    def publish(self, event: InteractionEvent) -> None: ...
    def subscribe(self, topic_pattern: str, handler: InteractionHandler) -> None: ...
    def unsubscribe(self, topic_pattern: str, handler: InteractionHandler) -> None: ...


@runtime_checkable
class JobBrokerProtocol(Protocol):
    """Formal protocol for the log-structured Durable Job Broker (B_dur)."""

    def submit(self, routine: str, arguments: Dict[str, Any], epoch: int = 0) -> str: ...
    def get_job(self, job_id: str) -> Optional[JobDescriptor]: ...
    def pause_for_approval(self, job_id: str) -> str: ...
    def approve_and_resume(self, job_id: str, token: str) -> bool: ...
    def cancel(self, job_id: str) -> bool: ...
    def recover(self) -> int: ...
