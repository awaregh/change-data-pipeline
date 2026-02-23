"""Tests for the ProjectionConsumer demo consumer."""

import pytest

from cdc.db_stream import ChangeEvent, Operation, SimulatedDBStream
from cdc.event_log import EventLog
from cdc.pipeline import Pipeline
from demo.projection_consumer import ProjectionConsumer


def make_pipeline_with_projection(table: str = "orders"):
    log = EventLog()
    projection = ProjectionConsumer("proj", log, table=table)
    return log, projection


class TestProjectionConsumer:
    def test_insert_adds_row(self):
        log, proj = make_pipeline_with_projection()
        log.append("orders", "1", {"operation": "INSERT", "table": "orders",
                                    "before": None, "after": {"id": 1, "item": "Widget"},
                                    "lsn": "1", "timestamp": 0.0, "schema_version": 1})
        proj.run(topic="orders")
        assert proj.count() == 1
        assert proj.get(1) == {"id": 1, "item": "Widget"}

    def test_update_modifies_row(self):
        log, proj = make_pipeline_with_projection()
        log.append("orders", "1", {"operation": "INSERT", "table": "orders",
                                    "before": None, "after": {"id": 1, "amount": 9.99},
                                    "lsn": "1", "timestamp": 0.0, "schema_version": 1})
        log.append("orders", "1", {"operation": "UPDATE", "table": "orders",
                                    "before": {"id": 1, "amount": 9.99},
                                    "after": {"id": 1, "amount": 14.99},
                                    "lsn": "2", "timestamp": 0.0, "schema_version": 1})
        proj.run(topic="orders")
        assert proj.count() == 1
        assert proj.get(1)["amount"] == 14.99

    def test_delete_removes_row(self):
        log, proj = make_pipeline_with_projection()
        log.append("orders", "1", {"operation": "INSERT", "table": "orders",
                                    "before": None, "after": {"id": 1, "item": "Widget"},
                                    "lsn": "1", "timestamp": 0.0, "schema_version": 1})
        log.append("orders", "1", {"operation": "DELETE", "table": "orders",
                                    "before": {"id": 1, "item": "Widget"}, "after": None,
                                    "lsn": "2", "timestamp": 0.0, "schema_version": 1})
        proj.run(topic="orders")
        assert proj.count() == 0
        assert proj.get(1) is None

    def test_only_processes_matching_topic(self):
        log, proj = make_pipeline_with_projection(table="orders")
        log.append("users", "u1", {"operation": "INSERT", "table": "users",
                                    "before": None, "after": {"id": 1},
                                    "lsn": "1", "timestamp": 0.0, "schema_version": 1})
        log.append("orders", "1", {"operation": "INSERT", "table": "orders",
                                    "before": None, "after": {"id": 1, "item": "Widget"},
                                    "lsn": "2", "timestamp": 0.0, "schema_version": 1})
        proj.run()
        # The user event will be ignored (wrong topic)
        assert proj.count() == 1

    def test_all_returns_all_rows(self):
        log, proj = make_pipeline_with_projection()
        for i in range(3):
            log.append("orders", str(i), {"operation": "INSERT", "table": "orders",
                                           "before": None, "after": {"id": i, "item": f"Item {i}"},
                                           "lsn": str(i), "timestamp": 0.0, "schema_version": 1})
        proj.run(topic="orders")
        assert len(proj.all()) == 3

    def test_events_processed_counter(self):
        log, proj = make_pipeline_with_projection()
        log.append("orders", "1", {"operation": "INSERT", "table": "orders",
                                    "before": None, "after": {"id": 1},
                                    "lsn": "1", "timestamp": 0.0, "schema_version": 1})
        log.append("orders", "1", {"operation": "UPDATE", "table": "orders",
                                    "before": {"id": 1}, "after": {"id": 1, "v": 2},
                                    "lsn": "2", "timestamp": 0.0, "schema_version": 1})
        proj.run(topic="orders")
        assert proj.events_processed == 2

    def test_end_to_end_with_pipeline(self):
        """Full pipeline integration: stream → log → projection."""
        stream = SimulatedDBStream(events=[
            ChangeEvent(Operation.INSERT, "orders", None, {"id": 1, "item": "A"}, lsn="1"),
            ChangeEvent(Operation.INSERT, "orders", None, {"id": 2, "item": "B"}, lsn="2"),
            ChangeEvent(Operation.UPDATE, "orders",
                        {"id": 1, "item": "A"}, {"id": 1, "item": "A++"}, lsn="3"),
            ChangeEvent(Operation.DELETE, "orders",
                        {"id": 2, "item": "B"}, None, lsn="4"),
        ])
        log = EventLog()
        proj = ProjectionConsumer("proj", log, table="orders")
        pipeline = Pipeline(stream=stream, log=log, consumers=[proj])
        pipeline.run_once(topic="orders")

        assert proj.count() == 1
        assert proj.get(1)["item"] == "A++"
        assert proj.get(2) is None
