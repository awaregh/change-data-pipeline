"""Tests for Consumer and ConsumerGroup."""

from typing import Any

import pytest

from cdc.consumer import Consumer, ConsumerGroup
from cdc.event_log import Event, EventLog


class CollectingConsumer(Consumer):
    """Test consumer that collects all events."""

    def __init__(self, name: str, log: EventLog) -> None:
        super().__init__(name, log)
        self.collected: list[Event] = []

    def process(self, event: Event) -> None:
        self.collected.append(event)


class TestConsumerOffsets:
    def test_initial_offset_is_minus_one(self):
        log = EventLog()
        c = CollectingConsumer("c", log)
        assert c.committed_offset == -1

    def test_run_advances_committed_offset(self):
        log = EventLog()
        log.append("t", "1", {"n": 1})
        log.append("t", "2", {"n": 2})
        c = CollectingConsumer("c", log)
        c.run()
        assert c.committed_offset == 1

    def test_run_only_processes_new_events(self):
        log = EventLog()
        log.append("t", "1", {})
        log.append("t", "2", {})
        c = CollectingConsumer("c", log)
        c.run()
        assert len(c.collected) == 2

        # New event added; second run should only pick it up
        log.append("t", "3", {})
        c.run()
        assert len(c.collected) == 3
        assert c.collected[-1].offset == 2

    def test_commit_advances_offset(self):
        log = EventLog()
        c = CollectingConsumer("c", log)
        c.commit(5)
        assert c.committed_offset == 5

    def test_commit_does_not_go_backwards(self):
        log = EventLog()
        c = CollectingConsumer("c", log)
        c.commit(10)
        c.commit(3)
        assert c.committed_offset == 10

    def test_seek_resets_offset(self):
        log = EventLog()
        log.append("t", "1", {})
        log.append("t", "2", {})
        log.append("t", "3", {})
        c = CollectingConsumer("c", log)
        c.run()
        assert c.committed_offset == 2

        c.seek(1)  # next run starts from offset 1
        c.run()
        # Should have re-processed events at offsets 1 and 2
        assert len(c.collected) == 5


class TestConsumerTopicFilter:
    def test_topic_filter(self):
        log = EventLog()
        log.append("orders", "1", {"op": "INSERT"})
        log.append("users", "u1", {"op": "INSERT"})
        log.append("orders", "2", {"op": "INSERT"})

        c = CollectingConsumer("c", log)
        c.run(topic="orders")
        assert len(c.collected) == 2
        assert all(e.topic == "orders" for e in c.collected)

    def test_no_topic_filter_processes_all(self):
        log = EventLog()
        log.append("orders", "1", {})
        log.append("users", "u1", {})
        c = CollectingConsumer("c", log)
        c.run()
        assert len(c.collected) == 2


class TestConsumerReplay:
    def test_replay_from_does_not_advance_committed_offset(self):
        log = EventLog()
        log.append("t", "1", {})
        log.append("t", "2", {})
        log.append("t", "3", {})
        c = CollectingConsumer("c", log)
        c.run()
        committed_before = c.committed_offset

        c.collected.clear()
        c.replay_from(0)

        assert c.committed_offset == committed_before
        assert len(c.collected) == 3

    def test_replay_from_mid_offset(self):
        log = EventLog()
        for i in range(5):
            log.append("t", str(i), {"n": i})
        c = CollectingConsumer("c", log)
        c.replay_from(3)
        assert len(c.collected) == 2
        assert [e.offset for e in c.collected] == [3, 4]


class TestConsumerGroup:
    def test_run_all_consumers(self):
        log = EventLog()
        log.append("t", "1", {})
        log.append("t", "2", {})

        c1 = CollectingConsumer("c1", log)
        c2 = CollectingConsumer("c2", log)
        group = ConsumerGroup([c1, c2])
        counts = group.run()

        assert counts["c1"] == 2
        assert counts["c2"] == 2
        assert len(c1.collected) == 2
        assert len(c2.collected) == 2

    def test_add_consumer(self):
        log = EventLog()
        group = ConsumerGroup()
        c = CollectingConsumer("c", log)
        group.add(c)
        log.append("t", "1", {})
        counts = group.run()
        assert counts["c"] == 1

    def test_replay_from_group(self):
        log = EventLog()
        for i in range(4):
            log.append("t", str(i), {})

        c1 = CollectingConsumer("c1", log)
        group = ConsumerGroup([c1])
        group.run()  # process all

        c1.collected.clear()
        counts = group.replay_from(2)
        assert counts["c1"] == 2
        assert [e.offset for e in c1.collected] == [2, 3]
