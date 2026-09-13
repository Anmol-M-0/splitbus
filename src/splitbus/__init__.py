"""Split-Bus Architecture: Decoupled communication substrate for reliable desktop agents."""

from splitbus.baseline_unified import UnifiedMessageBus
from splitbus.bridge import SplitBusBridge
from splitbus.core import (
    EpochFence,
    InteractionBusProtocol,
    InteractionEvent,
    JobBrokerProtocol,
    JobDescriptor,
    JobState,
)
from splitbus.interaction_bus import InteractionBus
from splitbus.job_broker import JobBroker

__version__ = "0.1.0"
__all__ = [
    "EpochFence",
    "InteractionBus",
    "InteractionBusProtocol",
    "InteractionEvent",
    "JobBroker",
    "JobBrokerProtocol",
    "JobDescriptor",
    "JobState",
    "SplitBusBridge",
    "UnifiedMessageBus",
]
