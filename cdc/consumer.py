"""Consumer base class with offset tracking and replay support.

Consumers read events from an ``EventLog``.  Each consumer (or consumer
group) persists its *last committed offset* so that processing can resume
after a restart and events can be replayed from any earlier offset.
"""

from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from typing import Any, Optional

from .event_log import Event, EventLog


class Consumer(ABC):
    """Base class for CDC event consumers.

    Sub-classes must implement ``process(event)``.

    Parameters
    ----------
    name:
        Unique consumer identifier used as the offset-tracking key.
    event_log:
        The ``EventLog`` this consumer reads from.
    """

    def __init__(self, name: str, event_log: EventLog) -> None:
        self.name = name
        self._log = event_log
        self._committed_offset: int = -1
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Subclass contract
    # ------------------------------------------------------------------

    @abstractmethod
    def process(self, event: Event) -> None:
        """Handle a single event.  Raise to signal processing failure."""

    # ------------------------------------------------------------------
    # Offset management
    # ------------------------------------------------------------------

    @property
    def committed_offset(self) -> int:
        """The last successfully processed offset, or -1 if none."""
        with self._lock:
            return self._committed_offset

    def commit(self, offset: int) -> None:
        """Advance the committed offset to *offset*."""
        with self._lock:
            if offset > self._committed_offset:
                self._committed_offset = offset

    def seek(self, offset: int) -> None:
        """Reset committed offset so the next ``run`` starts from *offset*."""
        with self._lock:
            self._committed_offset = offset - 1

    # ------------------------------------------------------------------
    # Processing
    # ------------------------------------------------------------------

    def run(self, topic: Optional[str] = None) -> int:
        """Process all unread events from the log.

        Returns the number of events processed in this call.
        """
        start = self.committed_offset + 1
        count = 0
        for event in self._log.replay(from_offset=start, topic=topic):
            self.process(event)
            self.commit(event.offset)
            count += 1
        return count

    def replay_from(self, offset: int, topic: Optional[str] = None) -> int:
        """Replay events from *offset* without advancing committed offset.

        Useful for re-processing past events (e.g. backfilling a new
        projection) without disturbing normal processing state.

        Returns the number of events replayed.
        """
        count = 0
        for event in self._log.replay(from_offset=offset, topic=topic):
            self.process(event)
            count += 1
        return count


class ConsumerGroup:
    """Runs multiple consumers over the same ``EventLog``.

    Parameters
    ----------
    consumers:
        List of ``Consumer`` instances.
    """

    def __init__(self, consumers: Optional[list[Consumer]] = None) -> None:
        self._consumers: list[Consumer] = list(consumers or [])

    def add(self, consumer: Consumer) -> None:
        self._consumers.append(consumer)

    def run(self, topic: Optional[str] = None) -> dict[str, int]:
        """Run all consumers and return a mapping of name → events processed."""
        return {c.name: c.run(topic=topic) for c in self._consumers}

    def replay_from(self, offset: int, topic: Optional[str] = None) -> dict[str, int]:
        """Replay all consumers from *offset*."""
        return {c.name: c.replay_from(offset, topic=topic) for c in self._consumers}
