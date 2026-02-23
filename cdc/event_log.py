"""Append-only event log with offset-based replay support.

The EventLog stores CDC events in insertion order and allows consumers to
replay from any offset, enabling at-least-once delivery guarantees.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterator, Optional


@dataclass
class Event:
    """A single entry in the event log."""

    offset: int
    topic: str
    key: str
    payload: dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    schema_version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Event":
        return cls(**data)


class EventLog:
    """Append-only, thread-safe event log backed by an optional JSONL file.

    Consumers replay the log from a given *offset* (0-based index of the
    event within the log).  Appending is O(1); replay is O(n) from the
    requested offset.

    Parameters
    ----------
    path:
        Optional path to a JSONL file used for durable storage.  When
        provided every append is immediately flushed to disk so that the
        log survives process restarts.  Pass ``None`` (default) for a
        purely in-memory log.
    """

    def __init__(self, path: Optional[str | Path] = None) -> None:
        self._events: list[Event] = []
        self._lock = threading.Lock()
        self._path = Path(path) if path else None
        self._subscribers: list[Callable[[Event], None]] = []

        if self._path and self._path.exists():
            self._load_from_disk()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def append(self, topic: str, key: str, payload: dict[str, Any], schema_version: int = 1) -> Event:
        """Append a new event and return it."""
        with self._lock:
            offset = len(self._events)
            event = Event(
                offset=offset,
                topic=topic,
                key=key,
                payload=payload,
                schema_version=schema_version,
            )
            self._events.append(event)
            if self._path:
                self._write_event(event)

        for subscriber in self._subscribers:
            subscriber(event)
        return event

    def replay(self, from_offset: int = 0, topic: Optional[str] = None) -> Iterator[Event]:
        """Yield events starting from *from_offset*.

        Parameters
        ----------
        from_offset:
            First offset to include (inclusive).
        topic:
            When provided only events whose ``topic`` matches are yielded.
        """
        with self._lock:
            snapshot = list(self._events)

        for event in snapshot[from_offset:]:
            if topic is None or event.topic == topic:
                yield event

    def subscribe(self, callback: Callable[[Event], None]) -> None:
        """Register a callback invoked synchronously after each ``append``."""
        self._subscribers.append(callback)

    def size(self) -> int:
        """Return total number of events in the log."""
        with self._lock:
            return len(self._events)

    def latest_offset(self) -> int:
        """Return the offset of the most-recently appended event, or -1."""
        with self._lock:
            return len(self._events) - 1

    def get(self, offset: int) -> Event:
        """Return the event at *offset*."""
        with self._lock:
            return self._events[offset]

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    def _write_event(self, event: Event) -> None:
        with self._path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(event.to_dict()) + "\n")

    def _load_from_disk(self) -> None:
        with self._path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    self._events.append(Event.from_dict(json.loads(line)))
