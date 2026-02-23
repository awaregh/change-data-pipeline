"""CDC Pipeline – orchestrates DB stream → EventLog → Consumers.

The ``Pipeline`` ties together:
- A ``DBStream`` (source of change events)
- An ``EventLog`` (durable ordered store)
- An optional ``SchemaRegistry`` (schema validation on ingest)
- One or more ``Consumer`` instances (downstream processors)

Usage::

    from cdc import Pipeline, SimulatedDBStream, EventLog

    stream = SimulatedDBStream(events=[...])
    log    = EventLog()
    pipeline = Pipeline(stream=stream, log=log)
    pipeline.run_once()   # drain the stream into the log, then run consumers
"""

from __future__ import annotations

import logging
from typing import Optional

from .consumer import Consumer, ConsumerGroup
from .db_stream import ChangeEvent, DBStream
from .event_log import EventLog
from .schema_registry import SchemaRegistry

logger = logging.getLogger(__name__)


class Pipeline:
    """Wires a ``DBStream`` to an ``EventLog`` and a set of ``Consumer``s.

    Parameters
    ----------
    stream:
        Source of database change events.
    log:
        Event log where ingested changes are stored.
    consumers:
        Optional list of ``Consumer`` instances to run after ingestion.
    schema_registry:
        Optional registry used to validate events on ingest.
    topic_prefix:
        String prepended to the table name to form the event topic,
        e.g. ``"db."`` → topic ``"db.orders"``.
    """

    def __init__(
        self,
        stream: DBStream,
        log: EventLog,
        consumers: Optional[list[Consumer]] = None,
        schema_registry: Optional[SchemaRegistry] = None,
        topic_prefix: str = "",
    ) -> None:
        self._stream = stream
        self._log = log
        self._group = ConsumerGroup(consumers)
        self._registry = schema_registry
        self._topic_prefix = topic_prefix
        self._ingested = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add_consumer(self, consumer: Consumer) -> None:
        """Register an additional consumer."""
        self._group.add(consumer)

    def ingest(self) -> int:
        """Drain the stream into the event log.

        Returns the number of change events ingested.
        """
        count = 0
        for change_event in self._stream.stream():
            self._ingest_one(change_event)
            count += 1
        self._ingested += count
        logger.debug("Ingested %d change events (total: %d)", count, self._ingested)
        return count

    def run_consumers(self, topic: Optional[str] = None) -> dict[str, int]:
        """Run all registered consumers.

        Returns a mapping of consumer name → number of events processed.
        """
        return self._group.run(topic=topic)

    def run_once(self, topic: Optional[str] = None) -> dict[str, int]:
        """Ingest from stream and then run all consumers.

        Returns consumer processing counts (same as ``run_consumers``).
        """
        self.ingest()
        return self.run_consumers(topic=topic)

    def replay(self, from_offset: int, topic: Optional[str] = None) -> dict[str, int]:
        """Replay the event log from *from_offset* through all consumers.

        This does *not* advance consumer committed offsets so that normal
        processing is unaffected.

        Returns a mapping of consumer name → number of events replayed.
        """
        return self._group.replay_from(from_offset, topic=topic)

    @property
    def total_ingested(self) -> int:
        """Total change events ingested since this Pipeline was created."""
        return self._ingested

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ingest_one(self, change: ChangeEvent) -> None:
        topic = f"{self._topic_prefix}{change.table}"
        payload = change.to_dict()

        if self._registry:
            subject = topic
            schema_ver = self._registry.get_latest(subject)
            if schema_ver:
                try:
                    self._registry.validate(subject, payload)
                except Exception as exc:
                    logger.warning("Schema validation failed for %s: %s", topic, exc)

        self._log.append(
            topic=topic,
            key=change.key,
            payload=payload,
            schema_version=change.schema_version,
        )
