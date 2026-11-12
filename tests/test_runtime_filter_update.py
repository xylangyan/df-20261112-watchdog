"""Runtime updates of path-matching handler filters.

Covers changing a subscription's matching rules at runtime without
unscheduling and re-scheduling: the underlying emitter is shared and kept
alive, another handler on the same watch sees no gap, invalid updates leave
the previous rules working, moved events keep their matching semantics, and
the choice of whether already-queued events use old or new rules is explicit
and testable through :class:`QueuedEventPolicy`.
"""

from __future__ import annotations

import contextlib
import re
import time
from pathlib import Path
from time import sleep

import pytest

from watchdog.events import (
    FileCreatedEvent,
    FileMovedEvent,
    FileSystemEventHandler,
    PatternMatchingEventHandler,
    QueuedEventPolicy,
    RegexMatchingEventHandler,
)
from watchdog.observers.api import BaseObserver, EventEmitter, ObservedWatch
from watchdog.observers.polling import PollingObserver

WATCH_PATH = "/watched"
OLD_PATTERN = "**/*.tmp"
NEW_PATTERN = "**/*.dat"


class RecordingHandler(PatternMatchingEventHandler):
    """Pattern-matching handler recording every dispatched event source path."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.received: list[str] = []

    def on_any_event(self, event) -> None:  # noqa: ANN001
        self.received.append(event.src_path)


class RegexRecordingHandler(RegexMatchingEventHandler):
    """Regex-matching handler recording every dispatched event source path."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.received: list[str] = []

    def on_any_event(self, event) -> None:  # noqa: ANN001
        self.received.append(event.src_path)


def _drive(observer: BaseObserver, count: int) -> None:
    """Dispatch ``count`` queued events synchronously."""
    for _ in range(count):
        observer.dispatch_events(observer.event_queue)


@pytest.fixture
def observer() -> BaseObserver:
    obs = BaseObserver(EventEmitter)
    yield obs
    obs.stop()
    with contextlib.suppress(RuntimeError):
        obs.join()


def _two_handlers(observer: BaseObserver) -> tuple[RecordingHandler, RecordingHandler, ObservedWatch, EventEmitter]:
    handler_a = RecordingHandler(patterns=[OLD_PATTERN])
    handler_b = RecordingHandler(patterns=[OLD_PATTERN])
    watch = observer.schedule(handler_a, WATCH_PATH, recursive=True)
    observer.add_handler_for_watch(handler_b, watch)
    (emitter,) = observer.emitters
    return handler_a, handler_b, watch, emitter


def test_queued_event_policy_enum_values() -> None:
    assert QueuedEventPolicy("old_rules") is QueuedEventPolicy.OLD_RULES
    assert QueuedEventPolicy("new_rules") is QueuedEventPolicy.NEW_RULES
    handler = PatternMatchingEventHandler(patterns=[OLD_PATTERN])
    assert handler.queued_event_policy is QueuedEventPolicy.OLD_RULES


def test_rule_snapshot_freezes_rules_at_queue_time() -> None:
    """A snapshot attached when an event is queued keeps the rules from that
    moment, so an OLD_RULES handler evaluates the event against them even
    after its live rules have moved on; NEW_RULES ignores the snapshot."""
    handler = RecordingHandler(patterns=[OLD_PATTERN])
    event = FileCreatedEvent(f"{WATCH_PATH}/old.tmp")
    handler.attach_rule_snapshot(event)

    handler.update_patterns(patterns=[NEW_PATTERN], queued_events=QueuedEventPolicy.OLD_RULES)
    handler.dispatch(event)
    assert handler.received == [f"{WATCH_PATH}/old.tmp"]

    handler.received.clear()
    handler.update_patterns(patterns=[NEW_PATTERN], queued_events=QueuedEventPolicy.NEW_RULES)
    handler.dispatch(event)
    assert handler.received == []


def test_update_patterns_switches_rules_without_touching_subscription(observer: BaseObserver) -> None:
    handler_a, handler_b, watch, emitter = _two_handlers(observer)

    observer.update_handler_filter(handler_a, watch, patterns=[NEW_PATTERN])

    assert handler_a.patterns == [NEW_PATTERN]
    assert handler_b.patterns == [OLD_PATTERN]
    # The subscription and its shared emitter stay in place: no unschedule gap.
    assert observer.emitters == {emitter}
    assert watch in observer._handlers  # noqa: SLF001


def test_update_is_rejected_for_unscheduled_handler(observer: BaseObserver) -> None:
    handler_a, _handler_b, watch, _emitter = _two_handlers(observer)
    stranger = RecordingHandler(patterns=[OLD_PATTERN])

    with pytest.raises(KeyError):
        observer.update_handler_filter(stranger, watch, patterns=[NEW_PATTERN])
    assert handler_a.patterns == [OLD_PATTERN]


def test_update_rejects_handler_without_runtime_rules(observer: BaseObserver) -> None:
    plain = FileSystemEventHandler()
    watch = observer.schedule(plain, WATCH_PATH, recursive=True)
    with pytest.raises(TypeError):
        observer.update_handler_filter(plain, watch, patterns=[NEW_PATTERN])  # type: ignore[arg-type]


def test_invalid_update_keeps_previous_rules_working(observer: BaseObserver) -> None:
    handler_a, _handler_b, watch, emitter = _two_handlers(observer)

    with pytest.raises(ValueError, match="conflicting patterns"):
        observer.update_handler_filter(
            handler_a,
            watch,
            patterns=["*.dat"],
            ignore_patterns=["*.dat"],
        )

    # Original subscription still works with the old rules.
    assert handler_a.patterns == [OLD_PATTERN]
    assert handler_a.ignore_patterns is None
    emitter.queue_event(FileCreatedEvent(f"{WATCH_PATH}/still.tmp"))
    _drive(observer, 1)
    assert handler_a.received == [f"{WATCH_PATH}/still.tmp"]


def test_invalid_queued_event_policy_keeps_previous_rules(observer: BaseObserver) -> None:
    handler_a, _handler_b, watch, _emitter = _two_handlers(observer)

    with pytest.raises(ValueError, match="bogus"):
        observer.update_handler_filter(handler_a, watch, patterns=[NEW_PATTERN], queued_events="bogus")

    assert handler_a.patterns == [OLD_PATTERN]
    assert handler_a.queued_event_policy is QueuedEventPolicy.OLD_RULES


def test_regex_invalid_update_rolls_back(observer: BaseObserver) -> None:
    handler = RegexRecordingHandler(regexes=[r".*\.tmp"])
    watch = observer.schedule(handler, WATCH_PATH, recursive=True)

    with pytest.raises(re.error):
        observer.update_handler_filter(handler, watch, regexes=["[unterminated"])

    assert [r.pattern for r in handler.regexes] == [r".*\.tmp"]
    (emitter,) = observer.emitters
    emitter.queue_event(FileCreatedEvent(f"{WATCH_PATH}/rollback.tmp"))
    _drive(observer, 1)
    assert handler.received == [f"{WATCH_PATH}/rollback.tmp"]


@pytest.mark.parametrize(
    ("policy", "expected_a", "expected_b"),
    [
        (
            QueuedEventPolicy.OLD_RULES,
            [f"{WATCH_PATH}/old.tmp", f"{WATCH_PATH}/new.dat"],
            [f"{WATCH_PATH}/old.tmp"],
        ),
        (
            QueuedEventPolicy.NEW_RULES,
            [f"{WATCH_PATH}/new.dat"],
            [f"{WATCH_PATH}/old.tmp"],
        ),
        ("old_rules", [f"{WATCH_PATH}/old.tmp", f"{WATCH_PATH}/new.dat"], [f"{WATCH_PATH}/old.tmp"]),
        ("new_rules", [f"{WATCH_PATH}/new.dat"], [f"{WATCH_PATH}/old.tmp"]),
    ],
)
def test_queued_old_event_uses_selected_policy(
    observer: BaseObserver,
    policy: QueuedEventPolicy,
    expected_a: list[str],
    expected_b: list[str],
) -> None:
    """Pad one old event, switch A's rules, then queue a new event.

    A receives according to the selected policy; B never shows a gap caused
    by A's switch because B keeps the old rules the whole time.
    """
    handler_a, handler_b, watch, emitter = _two_handlers(observer)

    # An event produced (and queued) under the old rules.
    emitter.queue_event(FileCreatedEvent(f"{WATCH_PATH}/old.tmp"))

    # Switch only A's filters at runtime; B and the emitter are untouched.
    observer.update_handler_filter(handler_a, watch, patterns=[NEW_PATTERN], queued_events=policy)

    # An event produced after the switch, matching only the new rules.
    emitter.queue_event(FileCreatedEvent(f"{WATCH_PATH}/new.dat"))

    _drive(observer, 2)

    assert handler_a.received == expected_a
    assert handler_b.received == expected_b


def test_moved_event_keeps_existing_matching_semantics(observer: BaseObserver) -> None:
    handler = RecordingHandler(patterns=["**/*.txt"], ignore_patterns=["**/*.bak"])
    watch = observer.schedule(handler, WATCH_PATH, recursive=True)
    (emitter,) = observer.emitters

    # A move touching an ignored path is dropped under the default ANY policy.
    emitter.queue_event(FileMovedEvent(f"{WATCH_PATH}/a.txt", f"{WATCH_PATH}/a.bak"))
    # A rename between two included paths is delivered; src_path is recorded.
    emitter.queue_event(FileMovedEvent(f"{WATCH_PATH}/b.txt", f"{WATCH_PATH}/c.txt"))
    _drive(observer, 2)
    assert handler.received == [f"{WATCH_PATH}/b.txt"]

    # After the runtime update the same move semantics apply with the new rules.
    observer.update_handler_filter(handler, watch, patterns=["**/*.dat"], ignore_patterns=["**/*.bak"])
    emitter.queue_event(FileMovedEvent(f"{WATCH_PATH}/d.dat", f"{WATCH_PATH}/e.dat"))
    emitter.queue_event(FileMovedEvent(f"{WATCH_PATH}/f.dat", f"{WATCH_PATH}/g.bak"))
    _drive(observer, 2)
    assert handler.received == [f"{WATCH_PATH}/b.txt", f"{WATCH_PATH}/d.dat"]


def test_shared_emitter_survives_update_and_keeps_dispatching(observer: BaseObserver) -> None:
    handler_a, handler_b, watch, emitter = _two_handlers(observer)

    observer.update_handler_filter(handler_a, watch, patterns=[NEW_PATTERN])
    assert observer.emitters == {emitter}
    assert emitter in observer.emitters

    # Both handlers still receive events produced after the update when they match.
    emitter.queue_event(FileCreatedEvent(f"{WATCH_PATH}/common.tmp"))
    _drive(observer, 1)
    assert handler_a.received == []  # now only *.dat
    assert handler_b.received == [f"{WATCH_PATH}/common.tmp"]  # unchanged, no gap


def test_polling_observer_runtime_filter_update(tmp_path: Path) -> None:
    """End-to-end check through the polling observer against a real directory."""
    directory = str(tmp_path)
    observer = PollingObserver(timeout=0.05)
    handler_a = RecordingHandler(patterns=["**/*.tmp"])
    handler_b = RecordingHandler(patterns=["**/*.tmp"])
    watch = observer.schedule(handler_a, directory, recursive=True)
    observer.add_handler_for_watch(handler_b, watch)
    observer.start()
    try:
        (tmp_path / "before.tmp").write_text("x")
        _wait_until(lambda: f"{directory}/before.tmp" in handler_b.received)

        # Switch A from temporary artifacts to the formal file extension.
        observer.update_handler_filter(handler_a, watch, patterns=["**/*.dat"])
        (tmp_path / "after.dat").write_text("y")
        _wait_until(lambda: f"{directory}/after.dat" in handler_a.received)
    finally:
        observer.stop()
        observer.join(5)

    # A kept receiving the old *.tmp event it had already caught, then moved on
    # to *.dat; B kept watching *.tmp throughout, so A's switch caused no gap.
    assert f"{directory}/before.tmp" in handler_a.received
    assert f"{directory}/after.dat" in handler_a.received
    assert f"{directory}/before.tmp" in handler_b.received
    assert f"{directory}/after.dat" not in handler_b.received


def _wait_until(predicate, timeout: float = 5.0) -> None:  # noqa: ANN001
    deadline = time.monotonic() + timeout
    while not predicate() and time.monotonic() < deadline:
        sleep(0.02)
    assert predicate(), "condition not met before timeout"
