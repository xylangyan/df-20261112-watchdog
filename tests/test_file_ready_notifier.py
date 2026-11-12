from __future__ import annotations


import time

import pytest

from watchdog.events import (
    EVENT_TYPE_READY,
    DirCreatedEvent,
    FileCreatedEvent,
    FileDeletedEvent,
    FileModifiedEvent,
    FileMovedEvent,
    FileReadyEvent,
    FileSystemEventHandler,
)
from watchdog.utils.file_ready_notifier import FileReadyNotifier


class ManualClock:
    """A monotonic clock that only moves when ``advance()`` is called."""

    def __init__(self, start: float = 0.0) -> None:
        self._now = start

    def monotonic(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds


class ScriptedStat:
    """A stat replacement holding mutable metadata per path.

    Calling :meth:`set` again with the same path models a new write (or a
    re-created file when the inode changes); :meth:`remove` models deletion
    or rename. Every stat call reads the current state, like the real one.
    """

    def __init__(self) -> None:
        self._states: dict[bytes | str, tuple[int, int, int, int]] = {}
        self._calls: list[bytes | str] = []

    def set(
        self,
        path: bytes | str,
        *,
        size: int,
        mtime_ns: int,
        inode: int = 1,
        is_regular: bool = True,
    ) -> None:
        mode = 0o100000 | 0o644 if is_regular else 0o040000 | 0o755
        self._states[path] = (size, mtime_ns, inode, mode)

    def remove(self, path: bytes | str) -> None:
        self._states.pop(path, None)

    def stat(self, path: bytes | str) -> tuple[int, int, int, int]:
        self._calls.append(path)
        try:
            return self._states[path]
        except KeyError as error:
            raise FileNotFoundError(path) from error

    @property
    def calls(self) -> list[bytes | str]:
        return self._calls


class ScriptedNotifier(FileReadyNotifier):
    """FileReadyNotifier using an explicit clock and stat sequence."""

    def __init__(
        self,
        clock: ManualClock,
        stat: ScriptedStat,
        handler: FileSystemEventHandler,
        settle_interval: float,
    ) -> None:
        self._clock = clock
        self._scripted_stat = stat
        super().__init__(handler, settle_interval, poll_interval=1.0)

    def _now(self) -> float:
        return self._clock.monotonic()

    def _stat_signature(self, path: bytes | str) -> tuple[int, int, int, int] | None:
        try:
            return self._scripted_stat.stat(path)
        except (FileNotFoundError, NotADirectoryError):
            return None


class RecordingHandler(FileSystemEventHandler):
    def __init__(self) -> None:
        self.events: list[object] = []
        self.ready: list[tuple[object, float]] = []
        self.clock: ManualClock | None = None

    def on_any_event(self, event):
        self.events.append(event)

    def on_ready(self, event: FileReadyEvent) -> None:
        assert event.event_type == EVENT_TYPE_READY
        now = self.clock.monotonic() if self.clock is not None else 0.0
        self.ready.append((event.src_path, now))


def make_notifier(
    settle_interval: float = 5.0,
) -> tuple[ManualClock, ScriptedStat, RecordingHandler, ScriptedNotifier]:
    clock = ManualClock()
    stat = ScriptedStat()
    handler = RecordingHandler()
    handler.clock = clock
    notifier = ScriptedNotifier(clock, stat, handler, settle_interval)
    return clock, stat, handler, notifier


def test_two_files_ready_independently_in_write_order():
    """A big file still being written must not delay a small settled file."""
    clock, stat, handler, notifier = make_notifier(settle_interval=5.0)
    stat.set("big", size=10, mtime_ns=100)
    stat.set("small", size=10, mtime_ns=100)

    notifier.dispatch(FileModifiedEvent("big"))
    notifier.dispatch(FileModifiedEvent("small"))

    # The big file keeps growing one second later; the small one does not.
    clock.advance(1)
    stat.set("big", size=100, mtime_ns=200)
    notifier.dispatch(FileModifiedEvent("big"))

    # Both were first reported at t=0, so the small one settles at t=5 and
    # the big one only at t=6 (it changed at t=1).
    clock.advance(4)
    assert notifier.pending_paths() == {"big", "small"}
    notifier._poll_and_notify()  # noqa: SLF001
    assert handler.ready == [("small", 5)]

    clock.advance(1)
    notifier._poll_and_notify()  # noqa: SLF001
    assert handler.ready == [("small", 5), ("big", 6)]

    # Original events are still forwarded unchanged, before readiness.
    assert [type(event).__name__ for event in handler.events] == [
        "FileModifiedEvent",
        "FileModifiedEvent",
        "FileModifiedEvent",
        "FileReadyEvent",
        "FileReadyEvent",
    ]


def test_large_file_change_restarts_only_its_own_timer():
    clock, stat, handler, notifier = make_notifier(settle_interval=5.0)
    stat.set("small", size=10, mtime_ns=100)
    stat.set("big", size=10, mtime_ns=100)
    notifier.handle_event(FileModifiedEvent("small"))
    notifier.handle_event(FileModifiedEvent("big"))

    clock.advance(2)
    stat.set("big", size=200, mtime_ns=200)
    notifier.handle_event(FileModifiedEvent("big"))  # big deadline -> t=7

    clock.advance(2)  # t=4
    stat.set("big", size=300, mtime_ns=300)
    notifier.handle_event(FileModifiedEvent("big"))  # big deadline -> t=9

    clock.advance(1)  # t=5
    notifier._poll_and_notify()  # noqa: SLF001
    assert handler.ready == [("small", 5)]

    clock.advance(2)  # t=7, big's previous deadline: nothing yet
    notifier._poll_and_notify()  # noqa: SLF001
    assert handler.ready == [("small", 5)]

    clock.advance(2)  # t=9, big settles five seconds after its last report
    notifier._poll_and_notify()  # noqa: SLF001
    assert handler.ready == [("small", 5), ("big", 9)]


def test_same_path_recreated_starts_a_new_timer():
    clock, stat, handler, notifier = make_notifier(settle_interval=5.0)
    stat.set("input", size=1, mtime_ns=100, inode=7)
    notifier.handle_event(FileModifiedEvent("input"))
    clock.advance(2)

    # Same path, new file: even identical size and mtime must not be accepted
    # as evidence that the previous incarnation settled.
    stat.set("input", size=1, mtime_ns=100, inode=8)
    notifier.handle_event(FileCreatedEvent("input"))

    clock.advance(3)  # t=5, the old deadline
    notifier._poll_and_notify()  # noqa: SLF001
    assert handler.ready == []

    clock.advance(2)  # t=7, five seconds after the re-creation
    notifier._poll_and_notify()  # noqa: SLF001
    assert handler.ready == [("input", 7)]


def test_delete_while_waiting_is_silent():
    clock, stat, handler, notifier = make_notifier(settle_interval=5.0)
    stat.set("gone", size=1, mtime_ns=100)
    notifier.handle_event(FileModifiedEvent("gone"))
    stat.remove("gone")

    clock.advance(6)
    notifier._poll_and_notify()  # noqa: SLF001
    assert handler.ready == []
    assert notifier.pending_paths() == set()


def test_rename_while_waiting_restarts_the_timer_at_the_new_path():
    clock, stat, handler, notifier = make_notifier(settle_interval=5.0)
    stat.set("old", size=1, mtime_ns=100)
    notifier.handle_event(FileModifiedEvent("old"))

    clock.advance(2)
    stat.remove("old")
    stat.set("new", size=1, mtime_ns=100)
    notifier.dispatch(FileMovedEvent("old", "new"))
    # The old path is dropped; the destination starts a fresh interval.
    assert notifier.pending_paths() == {"new"}

    clock.advance(4)  # t=6, before the destination's deadline
    notifier._poll_and_notify()  # noqa: SLF001
    assert handler.ready == []

    clock.advance(1)  # t=7, five seconds after the move
    notifier._poll_and_notify()  # noqa: SLF001
    assert handler.ready == [("new", 7)]
    # The move itself was forwarded to the wrapped handler.
    assert handler.events[-2].event_type == "moved"


def test_rename_away_while_waiting_is_silent():
    clock, stat, handler, notifier = make_notifier(settle_interval=5.0)
    stat.set("old", size=1, mtime_ns=100)
    notifier.handle_event(FileModifiedEvent("old"))

    clock.advance(2)
    stat.remove("old")
    notifier.dispatch(FileMovedEvent("old", "elsewhere"))
    assert notifier.pending_paths() == set()

    clock.advance(4)
    notifier._poll_and_notify()  # noqa: SLF001
    assert handler.ready == []


def test_deleted_event_forgets_a_pending_path():
    clock, stat, handler, notifier = make_notifier(settle_interval=5.0)
    stat.set("temp", size=1, mtime_ns=100)
    notifier.handle_event(FileModifiedEvent("temp"))
    # The delete event cancels the check even if stat still resolves briefly.
    notifier.dispatch(FileDeletedEvent("temp"))
    assert notifier.pending_paths() == set()

    clock.advance(6)
    notifier._poll_and_notify()  # noqa: SLF001
    assert handler.ready == []
    assert handler.events[-1].event_type == "deleted"


def test_stop_drops_pending_files_without_late_notification():
    clock, stat, handler, notifier = make_notifier(settle_interval=5.0)
    stat.set("late", size=1, mtime_ns=100)
    notifier.handle_event(FileModifiedEvent("late"))
    clock.advance(6)

    notifier.stop()
    notifier._poll_and_notify()  # noqa: SLF001

    assert handler.ready == []
    assert notifier.pending_paths() == set()


def test_change_between_check_and_delivery_restarts_the_timer():
    clock, stat, handler, notifier = make_notifier(settle_interval=5.0)
    stat.set("racy", size=1, mtime_ns=100)
    notifier.handle_event(FileModifiedEvent("racy"))
    clock.advance(5)

    # Make the delivery re-check observe a different signature than the poll.
    original_poll = notifier._poll_ready  # noqa: SLF001

    def poll_then_change():
        ready = original_poll()
        stat.remove("racy")
        stat.set("racy", size=2, mtime_ns=200)
        return ready

    notifier._poll_ready = poll_then_change  # noqa: SLF001
    notifier._poll_and_notify()  # noqa: SLF001
    assert handler.ready == []
    assert notifier.pending_paths() == {"racy"}

    # It is reported only after the new metadata settles.
    notifier._poll_ready = original_poll  # noqa: SLF001
    clock.advance(5)
    notifier._poll_and_notify()  # noqa: SLF001
    assert handler.ready == [("racy", 10)]


def test_scheduled_on_observer_it_reports_ready_file_through_dispatch(tmp_path):
    from watchdog.observers.polling import PollingObserver

    handler = RecordingHandler()
    notifier = FileReadyNotifier(handler, settle_interval=0.2, poll_interval=0.05)
    observer = PollingObserver(timeout=0.1)
    observer.schedule(notifier, str(tmp_path), recursive=False)

    notifier.start()
    observer.start()
    try:
        target = tmp_path / "table.csv"
        with target.open("w", encoding="utf-8") as stream:
            stream.write("a,b\n")
            stream.flush()
            time.sleep(0.05)
            stream.write("1,2\n")

        deadline = time.monotonic() + 5
        while not handler.ready and time.monotonic() < deadline:
            time.sleep(0.02)
        assert [path for path, _ in handler.ready] == [str(target)]
    finally:
        observer.stop()
        notifier.stop()
        observer.join()
        notifier.join()


def test_events_after_stop_are_ignored():
    clock, stat, handler, notifier = make_notifier(settle_interval=5.0)
    stat.set("late", size=1, mtime_ns=100)
    notifier.handle_event(FileModifiedEvent("late"))
    notifier.stop()

    notifier.handle_event(FileModifiedEvent("late"))
    notifier._poll_and_notify()  # noqa: SLF001
    assert handler.ready == []
    assert notifier.pending_paths() == set()


def test_directories_are_not_tracked():
    clock, stat, handler, notifier = make_notifier(settle_interval=5.0)
    stat.set("adir", size=4096, mtime_ns=100, is_regular=False)
    notifier.handle_event(DirCreatedEvent("adir"))
    assert notifier.pending_paths() == set()

    clock.advance(6)
    notifier._poll_and_notify()  # noqa: SLF001
    assert handler.ready == []


def test_invalid_arguments():
    handler = RecordingHandler()
    with pytest.raises(ValueError, match="settle_interval must be non-negative"):
        FileReadyNotifier(handler, settle_interval=-1)
    with pytest.raises(ValueError, match="poll_interval must be positive"):
        FileReadyNotifier(handler, settle_interval=1, poll_interval=0)


def test_ready_event_is_not_re_tracked():
    clock, stat, handler, notifier = make_notifier(settle_interval=5.0)
    stat.set("input", size=1, mtime_ns=100)
    notifier.handle_event(FileReadyEvent("input", is_synthetic=True))
    assert notifier.pending_paths() == set()
