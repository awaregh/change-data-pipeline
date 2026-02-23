"""Tests for DBStream (SimulatedDBStream and ChangeEvent)."""

import pytest

from cdc.db_stream import ChangeEvent, Operation, SimulatedDBStream


class TestChangeEvent:
    def test_key_from_after(self):
        event = ChangeEvent(
            operation=Operation.INSERT,
            table="orders",
            before=None,
            after={"id": 42, "item": "Widget"},
        )
        assert event.key == "42"

    def test_key_from_before_on_delete(self):
        event = ChangeEvent(
            operation=Operation.DELETE,
            table="orders",
            before={"id": 7, "item": "Widget"},
            after=None,
        )
        assert event.key == "7"

    def test_key_empty_when_no_row(self):
        event = ChangeEvent(operation=Operation.TRUNCATE, table="orders", before=None, after=None)
        assert event.key == ""

    def test_to_dict_contains_operation(self):
        event = ChangeEvent(operation=Operation.UPDATE, table="t", before={"id": 1}, after={"id": 1, "v": 2})
        d = event.to_dict()
        assert d["operation"] == "UPDATE"
        assert d["table"] == "t"
        assert d["after"] == {"id": 1, "v": 2}


class TestSimulatedDBStream:
    def _make_insert(self, pk: int) -> ChangeEvent:
        return ChangeEvent(
            operation=Operation.INSERT,
            table="orders",
            before=None,
            after={"id": pk, "item": f"Item {pk}"},
            lsn=str(pk),
        )

    def test_streams_all_events(self):
        events = [self._make_insert(i) for i in range(3)]
        stream = SimulatedDBStream(events=events)
        result = list(stream.stream())
        assert len(result) == 3

    def test_stream_order_preserved(self):
        events = [self._make_insert(i) for i in range(5)]
        stream = SimulatedDBStream(events=events)
        result = list(stream.stream())
        lsns = [e.lsn for e in result]
        assert lsns == ["0", "1", "2", "3", "4"]

    def test_empty_stream(self):
        stream = SimulatedDBStream()
        assert list(stream.stream()) == []

    def test_add_event(self):
        stream = SimulatedDBStream()
        stream.add(self._make_insert(99))
        result = list(stream.stream())
        assert len(result) == 1
        assert result[0].after["id"] == 99

    def test_operations(self):
        events = [
            ChangeEvent(Operation.INSERT, "t", None, {"id": 1}),
            ChangeEvent(Operation.UPDATE, "t", {"id": 1}, {"id": 1, "v": 2}),
            ChangeEvent(Operation.DELETE, "t", {"id": 1}, None),
        ]
        stream = SimulatedDBStream(events=events)
        result = list(stream.stream())
        assert result[0].operation == Operation.INSERT
        assert result[1].operation == Operation.UPDATE
        assert result[2].operation == Operation.DELETE
