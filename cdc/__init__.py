"""Change Data Capture (CDC) pipeline library.

Provides:
- EventLog: append-only event log with offset-based replay
- DBStream: database change stream (Postgres logical decoding + simulation)
- Consumer: base consumer with offset tracking and replay support
- SchemaRegistry: schema registry for schema evolution
- Pipeline: orchestrates CDC pipeline components
"""

from .event_log import EventLog, Event
from .db_stream import DBStream, ChangeEvent, Operation
from .consumer import Consumer, ConsumerGroup
from .schema_registry import SchemaRegistry, SchemaVersion, SchemaEvolutionError
from .pipeline import Pipeline

__all__ = [
    "EventLog",
    "Event",
    "DBStream",
    "ChangeEvent",
    "Operation",
    "Consumer",
    "ConsumerGroup",
    "SchemaRegistry",
    "SchemaVersion",
    "SchemaEvolutionError",
    "Pipeline",
]
