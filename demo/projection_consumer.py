"""Demo projection consumer.

``ProjectionConsumer`` maintains an in-memory read model (projection) of
the current state of a table by applying INSERT / UPDATE / DELETE events
from the event log.

It demonstrates how a CDC consumer can build a queryable view of the
database that stays in sync as data changes.
"""

from __future__ import annotations

from typing import Any, Optional

from cdc import Consumer, EventLog
from cdc.db_stream import Operation
from cdc.event_log import Event


class ProjectionConsumer(Consumer):
    """Builds an in-memory projection (current state) from CDC events.

    The projection maps primary-key values to the latest row data for a
    given table/topic.

    Parameters
    ----------
    name:
        Consumer identifier.
    event_log:
        The ``EventLog`` to consume from.
    table:
        The table name (used as topic filter, e.g. ``"orders"``).
    pk_field:
        Name of the primary-key field in the row payload (default: ``"id"``).
    topic_prefix:
        Prefix to prepend to *table* when filtering events, e.g. ``"db."``.
    """

    def __init__(
        self,
        name: str,
        event_log: EventLog,
        table: str,
        pk_field: str = "id",
        topic_prefix: str = "",
    ) -> None:
        super().__init__(name, event_log)
        self._table = table
        self._pk_field = pk_field
        self._topic = f"{topic_prefix}{table}"
        self._projection: dict[Any, dict[str, Any]] = {}
        self._events_processed = 0

    # ------------------------------------------------------------------
    # Consumer contract
    # ------------------------------------------------------------------

    def process(self, event: Event) -> None:
        """Apply an event to the projection."""
        if event.topic != self._topic:
            return

        payload = event.payload
        op = payload.get("operation")

        if op == Operation.INSERT.value:
            row = payload.get("after") or {}
            pk = row.get(self._pk_field)
            if pk is not None:
                self._projection[pk] = dict(row)
                self._events_processed += 1

        elif op == Operation.UPDATE.value:
            row = payload.get("after") or {}
            pk = row.get(self._pk_field)
            if pk is not None:
                self._projection[pk] = dict(row)
                self._events_processed += 1

        elif op == Operation.DELETE.value:
            row = payload.get("before") or {}
            pk = row.get(self._pk_field)
            if pk is not None:
                self._projection.pop(pk, None)
                self._events_processed += 1

    # ------------------------------------------------------------------
    # Projection query API
    # ------------------------------------------------------------------

    def get(self, pk: Any) -> Optional[dict[str, Any]]:
        """Return the current row for *pk*, or ``None`` if not found."""
        return self._projection.get(pk)

    def all(self) -> list[dict[str, Any]]:
        """Return all rows in the projection."""
        return list(self._projection.values())

    def count(self) -> int:
        """Return the number of rows currently in the projection."""
        return len(self._projection)

    @property
    def events_processed(self) -> int:
        """Total change events applied to this projection."""
        return self._events_processed
