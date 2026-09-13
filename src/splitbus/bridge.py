"""Bridge Protocol: Non-blocking state transition gateway between B_int and B_dur."""

from __future__ import annotations

from typing import Any, Dict, Optional

from splitbus.core import EpochFence, InteractionBusProtocol, InteractionEvent, JobBrokerProtocol, JobDescriptor


class SplitBusBridge:
    """Non-blocking mediation bridge between the Ephemeral Interaction Bus and Durable Job Broker.

    Functions:
    1. Translates interactive tool requests from B_int into durable tasks on B_dur.
    2. Emits lightweight state projection notifications back to B_int upon job progress.
    3. Enforces monotonic epoch gating so that stale background completions cannot corrupt
       the current interactive dialogue turn.
    """

    def __init__(
        self,
        interaction_bus: InteractionBusProtocol,
        job_broker: JobBrokerProtocol,
        epoch_fence: EpochFence,
    ) -> None:
        self.interaction_bus = interaction_bus
        self.job_broker = job_broker
        self.epoch_fence = epoch_fence

        # Wire bridge to listen for interactive task requests
        self.interaction_bus.subscribe("task:request_durable", self._on_task_request)

    def _on_task_request(self, event: InteractionEvent) -> None:
        """Handle request from interaction thread without blocking."""
        routine = event.payload.get("routine", "default")
        arguments = event.payload.get("arguments", {})
        active_epoch = event.epoch or self.epoch_fence.current

        job_id = self.job_broker.submit(
            routine=routine,
            arguments=arguments,
            epoch=active_epoch,
        )

        # Notify interaction bus of job submission
        self.interaction_bus.publish(
            InteractionEvent(
                topic="job:submitted",
                payload={"job_id": job_id, "routine": routine, "epoch": active_epoch},
                epoch=active_epoch,
            )
        )

    def notify_job_completed(self, job: JobDescriptor) -> bool:
        """Publish job completion event to Interaction Bus with epoch fencing.

        Returns True if event was delivered within valid epoch, False if fenced out.
        """
        if not self.epoch_fence.is_valid(job.epoch):
            # Dropped due to user barge-in / newer turn!
            self.interaction_bus.publish(
                InteractionEvent(
                    topic="job:fenced_out",
                    payload={
                        "job_id": job.job_id,
                        "job_epoch": job.epoch,
                        "current_epoch": self.epoch_fence.current,
                    },
                    epoch=self.epoch_fence.current,
                )
            )
            return False

        self.interaction_bus.publish(
            InteractionEvent(
                topic="job:completed",
                payload={
                    "job_id": job.job_id,
                    "result": job.result,
                    "epoch": job.epoch,
                },
                epoch=job.epoch,
            )
        )
        return True
