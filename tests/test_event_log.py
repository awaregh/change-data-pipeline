"""Tests for EventLog."""

import json
import tempfile
from pathlib import Path

import pytest

from cdc.event_log import Event, EventLog


def make_log() -> EventLog:
    return EventLog()


class TestEventLogBasics:
    def test_append_returns_event(self):
        log = make_log()
        event = log.append("orders", "1", {"op": "INSERT"})
        assert isinstance(event, Event)
        assert event.offset == 0
        assert event.topic == "orders"
        assert event.key == "1"

    def test_offsets_are_sequential(self):
        log = make_log()
        e0 = log.append("t", "a", {})
        e1 = log.append("t", "b", {})
        e2 = log.append("t", "c", {})
        assert [e.offset for e in [e0, e1, e2]] == [0, 1, 2]

    def test_size(self):
        log = make_log()
        assert log.size() == 0
        log.append("t", "k", {})
        assert log.size() == 1

    def test_latest_offset_empty(self):
        log = make_log()
        assert log.latest_offset() == -1

    def test_latest_offset(self):
        log = make_log()
        log.append("t", "k", {})
        log.append("t", "k", {})
        assert log.latest_offset() == 1

    def test_get(self):
        log = make_log()
        log.append("t", "k", {"val": 42})
        event = log.get(0)
        assert event.payload == {"val": 42}


class TestEventLogReplay:
    def test_replay_all(self):
        log = make_log()
        log.append("t", "a", {"n": 1})
        log.append("t", "b", {"n": 2})
        log.append("t", "c", {"n": 3})
        events = list(log.replay())
        assert len(events) == 3
        assert [e.payload["n"] for e in events] == [1, 2, 3]

    def test_replay_from_offset(self):
        log = make_log()
        for i in range(5):
            log.append("t", str(i), {"n": i})
        events = list(log.replay(from_offset=3))
        assert [e.payload["n"] for e in events] == [3, 4]

    def test_replay_topic_filter(self):
        log = make_log()
        log.append("orders", "1", {"n": 1})
        log.append("users", "u1", {"n": 2})
        log.append("orders", "2", {"n": 3})
        orders = list(log.replay(topic="orders"))
        assert len(orders) == 2
        assert all(e.topic == "orders" for e in orders)

    def test_replay_empty_log(self):
        log = make_log()
        assert list(log.replay()) == []

    def test_replay_from_beyond_end(self):
        log = make_log()
        log.append("t", "k", {})
        assert list(log.replay(from_offset=100)) == []


class TestEventLogSubscribe:
    def test_subscriber_called_on_append(self):
        log = make_log()
        received: list[Event] = []
        log.subscribe(received.append)
        log.append("t", "k", {"x": 1})
        assert len(received) == 1
        assert received[0].payload == {"x": 1}

    def test_multiple_subscribers(self):
        log = make_log()
        a: list[Event] = []
        b: list[Event] = []
        log.subscribe(a.append)
        log.subscribe(b.append)
        log.append("t", "k", {})
        assert len(a) == 1
        assert len(b) == 1


class TestEventLogPersistence:
    def test_round_trip_to_disk(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "events.jsonl"

            log1 = EventLog(path=path)
            log1.append("orders", "1", {"amount": 9.99})
            log1.append("orders", "2", {"amount": 19.99})

            # Load from disk into a new EventLog instance
            log2 = EventLog(path=path)
            assert log2.size() == 2
            events = list(log2.replay())
            assert events[0].payload == {"amount": 9.99}
            assert events[1].payload == {"amount": 19.99}

    def test_file_format_is_jsonl(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "events.jsonl"
            log = EventLog(path=path)
            log.append("t", "k", {"val": 1})

            lines = path.read_text().strip().splitlines()
            assert len(lines) == 1
            data = json.loads(lines[0])
            assert data["topic"] == "t"
            assert data["payload"] == {"val": 1}


class TestEventSerialization:
    def test_to_dict_round_trip(self):
        e = Event(offset=0, topic="t", key="k", payload={"a": 1})
        d = e.to_dict()
        e2 = Event.from_dict(d)
        assert e2.offset == 0
        assert e2.topic == "t"
        assert e2.payload == {"a": 1}
