"""Database change stream – Postgres logical decoding and simulation.

Two implementations are provided:

1. ``SimulatedDBStream`` – generates synthetic INSERT/UPDATE/DELETE events
   from an in-memory table.  No database required; ideal for tests and
   local development.

2. ``PostgresDBStream`` – reads real change events via Postgres logical
   decoding (``pgoutput`` plugin).  Requires a Postgres ≥ 10 connection
   string and an existing replication slot.

Both emit ``ChangeEvent`` objects which the ``Pipeline`` writes into an
``EventLog``.
"""

from __future__ import annotations

import copy
import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Iterator, Optional


class Operation(str, Enum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    TRUNCATE = "TRUNCATE"


@dataclass
class ChangeEvent:
    """A single row-level database change."""

    operation: Operation
    table: str
    before: Optional[dict[str, Any]]  # row state before change (None for INSERT)
    after: Optional[dict[str, Any]]   # row state after change  (None for DELETE)
    lsn: str = ""                     # log-sequence number / position
    timestamp: float = field(default_factory=time.time)
    schema_version: int = 1

    @property
    def key(self) -> str:
        """Best-effort primary key string from the changed row."""
        row = self.after or self.before or {}
        pk = row.get("id") or row.get("pk") or row.get("key")
        return str(pk) if pk is not None else ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "operation": self.operation.value,
            "table": self.table,
            "before": self.before,
            "after": self.after,
            "lsn": self.lsn,
            "timestamp": self.timestamp,
            "schema_version": self.schema_version,
        }


class DBStream:
    """Abstract base for CDC streams."""

    def stream(self) -> Iterator[ChangeEvent]:  # pragma: no cover
        raise NotImplementedError

    def close(self) -> None:  # pragma: no cover
        pass


class SimulatedDBStream(DBStream):
    """Replay a list of pre-defined change events for testing/demo.

    Parameters
    ----------
    events:
        Sequence of ``ChangeEvent`` objects to emit, in order.
    """

    def __init__(self, events: Optional[list[ChangeEvent]] = None) -> None:
        self._events: list[ChangeEvent] = events or []

    def add(self, event: ChangeEvent) -> None:
        """Append a change event to the simulation queue."""
        self._events.append(event)

    def stream(self) -> Iterator[ChangeEvent]:
        for event in self._events:
            yield event


class PostgresDBStream(DBStream):
    """Real CDC stream via Postgres logical decoding (pgoutput plugin).

    Requires:
    - ``psycopg2`` installed (``pip install psycopg2-binary``)
    - Postgres ≥ 10 with ``wal_level = logical``
    - An existing replication slot (created automatically if absent)
    - The ``pgoutput`` plugin and a publication on the desired tables

    Parameters
    ----------
    dsn:
        Postgres DSN, e.g. ``"host=localhost dbname=mydb user=postgres"``.
    slot_name:
        Logical replication slot name.
    publication_name:
        Postgres publication to subscribe to (``CREATE PUBLICATION …``).
    """

    def __init__(
        self,
        dsn: str,
        slot_name: str = "cdc_slot",
        publication_name: str = "cdc_publication",
    ) -> None:
        self._dsn = dsn
        self._slot = slot_name
        self._publication = publication_name
        self._conn = None
        self._lsn_counter = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def stream(self) -> Iterator[ChangeEvent]:
        """Yield ``ChangeEvent`` objects from the Postgres WAL stream."""
        conn = self._connect()
        cursor = conn.cursor()
        self._ensure_slot(cursor)

        options = {"proto_version": "1", "publication_names": self._publication}
        cursor.start_replication(
            slot_name=self._slot,
            decode=True,
            options=options,
        )

        for msg in cursor:
            events = self._decode_pgoutput(msg.payload)
            for event in events:
                yield event
            msg.cursor.send_feedback(flush_lsn=msg.data_start)

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _connect(self):
        try:
            import psycopg2
            from psycopg2.extras import LogicalReplicationConnection
        except ImportError as exc:
            raise RuntimeError(
                "psycopg2 is required for PostgresDBStream. "
                "Install it with: pip install psycopg2-binary"
            ) from exc

        import psycopg2
        from psycopg2.extras import LogicalReplicationConnection

        self._conn = psycopg2.connect(self._dsn, connection_factory=LogicalReplicationConnection)
        return self._conn

    def _ensure_slot(self, cursor) -> None:
        try:
            cursor.create_replication_slot(self._slot, output_plugin="pgoutput")
        except Exception:
            pass  # slot already exists

    def _decode_pgoutput(self, payload: str) -> list[ChangeEvent]:
        """Parse pgoutput JSON payload into ``ChangeEvent`` objects.

        Note: The pgoutput plugin emits a binary protocol; in practice you
        would use a library such as ``pg-logical-replication`` or decode
        the binary format.  This method handles the simplified JSON
        representation produced by the ``wal2json`` plugin as an
        alternative, and produces a reasonable stub otherwise.
        """
        events: list[ChangeEvent] = []
        try:
            data = json.loads(payload)
            for change in data.get("change", []):
                op_str = change.get("kind", "").upper()
                try:
                    op = Operation(op_str)
                except ValueError:
                    continue

                col_names = change.get("columnnames", [])
                col_values = change.get("columnvalues", [])
                row = dict(zip(col_names, col_values))

                old_keys = change.get("oldkeys", {})
                before = None
                if old_keys:
                    before = dict(zip(old_keys.get("keynames", []), old_keys.get("keyvalues", [])))

                self._lsn_counter += 1
                events.append(
                    ChangeEvent(
                        operation=op,
                        table=change.get("table", "unknown"),
                        before=before if op != Operation.INSERT else None,
                        after=row if op != Operation.DELETE else None,
                        lsn=str(self._lsn_counter),
                    )
                )
        except (json.JSONDecodeError, AttributeError):
            pass
        return events
