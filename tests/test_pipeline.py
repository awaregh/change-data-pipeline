"""Tests for Pipeline orchestration."""

import pytest

from cdc.db_stream import ChangeEvent, Operation, SimulatedDBStream
from cdc.event_log import EventLog
from cdc.pipeline import Pipeline
from cdc.schema_registry import SchemaRegistry, Compatibility
from cdc.consumer import Consumer
from cdc.event_log import Event


class CollectingConsumer(Consumer):
    def __init__(self, name: str, log: EventLog) -> None:
        super().__init__(name, log)
        self.collected: list[Event] = []

    def process(self, event: Event) -> None:
        self.collected.append(event)


def make_stream(*operations) -> SimulatedDBStream:
    events = []
    for i, (op, table, after, before) in enumerate(operations):
        events.append(
            ChangeEvent(operation=op, table=table, before=before, after=after, lsn=str(i))
        )
    return SimulatedDBStream(events=events)


class TestPipelineIngest:
    def test_ingest_writes_to_log(self):
        stream = make_stream(
            (Operation.INSERT, "orders", {"id": 1, "item": "A"}, None),
            (Operation.INSERT, "orders", {"id": 2, "item": "B"}, None),
        )
        log = EventLog()
        pipeline = Pipeline(stream=stream, log=log)
        count = pipeline.ingest()
        assert count == 2
        assert log.size() == 2

    def test_ingest_topic_is_table_name(self):
        stream = make_stream(
            (Operation.INSERT, "users", {"id": 1}, None),
        )
        log = EventLog()
        pipeline = Pipeline(stream=stream, log=log)
        pipeline.ingest()
        assert log.get(0).topic == "users"

    def test_ingest_with_topic_prefix(self):
        stream = make_stream((Operation.INSERT, "orders", {"id": 1}, None))
        log = EventLog()
        pipeline = Pipeline(stream=stream, log=log, topic_prefix="db.")
        pipeline.ingest()
        assert log.get(0).topic == "db.orders"

    def test_total_ingested_accumulates(self):
        stream = SimulatedDBStream(events=[
            ChangeEvent(Operation.INSERT, "t", None, {"id": 1}),
        ])
        log = EventLog()
        pipeline = Pipeline(stream=stream, log=log)
        pipeline.ingest()
        assert pipeline.total_ingested == 1

    def test_payload_contains_operation(self):
        stream = make_stream(
            (Operation.UPDATE, "orders", {"id": 1, "v": 2}, {"id": 1, "v": 1}),
        )
        log = EventLog()
        Pipeline(stream=stream, log=log).ingest()
        event = log.get(0)
        assert event.payload["operation"] == "UPDATE"


class TestPipelineRunConsumers:
    def test_run_consumers_processes_events(self):
        stream = make_stream(
            (Operation.INSERT, "orders", {"id": 1}, None),
            (Operation.INSERT, "orders", {"id": 2}, None),
        )
        log = EventLog()
        c = CollectingConsumer("c", log)
        pipeline = Pipeline(stream=stream, log=log, consumers=[c])
        pipeline.run_once()
        assert len(c.collected) == 2

    def test_add_consumer_after_init(self):
        stream = make_stream((Operation.INSERT, "t", {"id": 1}, None))
        log = EventLog()
        pipeline = Pipeline(stream=stream, log=log)
        c = CollectingConsumer("c", log)
        pipeline.add_consumer(c)
        pipeline.run_once()
        assert len(c.collected) == 1

    def test_run_consumers_returns_counts(self):
        stream = make_stream(
            (Operation.INSERT, "orders", {"id": 1}, None),
        )
        log = EventLog()
        c = CollectingConsumer("c", log)
        pipeline = Pipeline(stream=stream, log=log, consumers=[c])
        counts = pipeline.run_once()
        assert counts["c"] == 1


class TestPipelineReplay:
    def test_replay_from_offset_zero(self):
        stream = make_stream(
            (Operation.INSERT, "orders", {"id": 1}, None),
            (Operation.INSERT, "orders", {"id": 2}, None),
            (Operation.INSERT, "orders", {"id": 3}, None),
        )
        log = EventLog()
        c = CollectingConsumer("c", log)
        pipeline = Pipeline(stream=stream, log=log, consumers=[c])
        pipeline.run_once()
        committed_before = c.committed_offset

        # Replay should not move committed offset
        c.collected.clear()
        pipeline.replay(from_offset=0)
        assert c.committed_offset == committed_before
        assert len(c.collected) == 3

    def test_replay_from_mid_offset(self):
        stream = SimulatedDBStream(events=[
            ChangeEvent(Operation.INSERT, "t", None, {"id": i}) for i in range(5)
        ])
        log = EventLog()
        c = CollectingConsumer("c", log)
        pipeline = Pipeline(stream=stream, log=log, consumers=[c])
        pipeline.run_once()

        c.collected.clear()
        counts = pipeline.replay(from_offset=3)
        assert counts["c"] == 2


class TestPipelineSchemaRegistry:
    def test_pipeline_accepts_schema_registry(self):
        stream = make_stream((Operation.INSERT, "orders", {"id": 1, "item": "A", "amount": 1.0}, None))
        log = EventLog()
        registry = SchemaRegistry(default_compatibility=Compatibility.FULL)
        pipeline = Pipeline(stream=stream, log=log, schema_registry=registry)
        # Ingest should succeed even without registered schemas
        assert pipeline.ingest() == 1
