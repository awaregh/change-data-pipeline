"""Demo: Change Data Capture pipeline end-to-end.

This script demonstrates the full CDC pipeline:

1. A ``SimulatedDBStream`` emits INSERT / UPDATE / DELETE events for an
   ``orders`` table.
2. A ``Pipeline`` ingests those events into an ``EventLog``.
3. A ``ProjectionConsumer`` builds a live projection (current order state).
4. Events are replayed from offset 0 to show replay functionality.
5. A ``SchemaRegistry`` enforces schema evolution rules.

Run::

    python demo/run_demo.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Allow running from the repo root or the demo/ directory
sys.path.insert(0, str(Path(__file__).parent.parent))

from cdc import (
    EventLog,
    Pipeline,
    SchemaRegistry,
)
from cdc.db_stream import ChangeEvent, Operation, SimulatedDBStream
from cdc.schema_registry import Compatibility, SchemaEvolutionError
from demo.projection_consumer import ProjectionConsumer


def print_section(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print("=" * 60)


def demo_db_event_stream(pipeline: Pipeline, log: EventLog) -> None:
    print_section("1. DB Event Stream — ingest changes")

    ingested = pipeline.ingest()
    print(f"  Ingested {ingested} change events into the event log.")
    print(f"  Event log size: {log.size()} events")

    print("\n  Raw events in log:")
    for event in log.replay():
        op = event.payload.get("operation")
        table = event.topic
        after = event.payload.get("after") or event.payload.get("before")
        print(f"    [{event.offset}] {table:20s} {op:8s} {after}")


def demo_consumers(pipeline: Pipeline, projection: ProjectionConsumer) -> None:
    print_section("2. Consumers — build projections")

    counts = pipeline.run_consumers(topic="orders")
    print(f"  Consumer '{projection.name}' processed: {counts.get(projection.name)} events")
    print(f"  Committed offset: {projection.committed_offset}")
    print(f"\n  Current order projection ({projection.count()} rows):")
    for row in sorted(projection.all(), key=lambda r: r.get("id", 0)):
        print(f"    {row}")


def demo_replay(pipeline: Pipeline, log: EventLog, projection: ProjectionConsumer) -> None:
    print_section("3. Replay — rebuild projection from offset 0")

    # Reset projection state to simulate a fresh rebuild
    projection._projection.clear()
    projection._events_processed = 0

    replayed = pipeline.replay(from_offset=0, topic="orders")
    print(f"  Replayed events: {replayed}")
    print(f"\n  Rebuilt projection ({projection.count()} rows):")
    for row in sorted(projection.all(), key=lambda r: r.get("id", 0)):
        print(f"    {row}")


def demo_schema_evolution(registry: SchemaRegistry) -> None:
    print_section("4. Schema Evolution — register and evolve schemas")

    v1 = {
        "type": "object",
        "properties": {
            "id":     {"type": "integer"},
            "item":   {"type": "string"},
            "amount": {"type": "number"},
        },
        "required": ["id", "item", "amount"],
    }
    sv1 = registry.register("orders", v1)
    print(f"  Registered schema v{sv1.version} for 'orders'")

    # FULL-compatible evolution: add optional field
    v2 = {
        "type": "object",
        "properties": {
            "id":       {"type": "integer"},
            "item":     {"type": "string"},
            "amount":   {"type": "number"},
            "currency": {"type": "string"},   # new optional field
        },
        "required": ["id", "item", "amount"],
    }
    sv2 = registry.register("orders", v2)
    print(f"  Registered schema v{sv2.version} for 'orders' (added optional 'currency' field)")

    print(f"  Latest schema version: {registry.get_latest('orders').version}")
    print(f"  Total versions registered: {len(registry.all_versions('orders'))}")

    # Demonstrate incompatible evolution being rejected
    v3_bad = {
        "type": "object",
        "properties": {
            "id":       {"type": "integer"},
            "currency": {"type": "string"},
        },
        "required": ["id", "currency"],  # 'item' and 'amount' dropped from required
    }
    try:
        registry.register("orders", v3_bad)
        print("  ERROR: breaking schema should have been rejected!")
    except SchemaEvolutionError as exc:
        print(f"  Breaking schema correctly rejected: {exc}")


def main() -> None:
    print("Change Data Capture Pipeline — Demo")

    # ------------------------------------------------------------------ #
    # Set up simulated change events                                       #
    # ------------------------------------------------------------------ #
    stream = SimulatedDBStream(
        events=[
            ChangeEvent(operation=Operation.INSERT, table="orders", before=None,
                        after={"id": 1, "item": "Widget A", "amount": 9.99}, lsn="1"),
            ChangeEvent(operation=Operation.INSERT, table="orders", before=None,
                        after={"id": 2, "item": "Widget B", "amount": 19.99}, lsn="2"),
            ChangeEvent(operation=Operation.INSERT, table="orders", before=None,
                        after={"id": 3, "item": "Widget C", "amount": 4.99}, lsn="3"),
            ChangeEvent(operation=Operation.UPDATE, table="orders",
                        before={"id": 2, "item": "Widget B", "amount": 19.99},
                        after={"id": 2, "item": "Widget B", "amount": 24.99}, lsn="4"),
            ChangeEvent(operation=Operation.DELETE, table="orders",
                        before={"id": 3, "item": "Widget C", "amount": 4.99},
                        after=None, lsn="5"),
        ]
    )

    log = EventLog()
    projection = ProjectionConsumer("order_projection", log, table="orders")
    registry = SchemaRegistry(default_compatibility=Compatibility.FULL)

    pipeline = Pipeline(stream=stream, log=log, consumers=[projection])

    demo_db_event_stream(pipeline, log)
    demo_consumers(pipeline, projection)
    demo_replay(pipeline, log, projection)
    demo_schema_evolution(registry)

    print("\n" + "=" * 60)
    print("  Demo complete.")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
