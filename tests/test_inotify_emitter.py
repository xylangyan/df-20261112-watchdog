from __future__ import annotations

import errno
import os
import time
from queue import Queue
from threading import Event, Lock
from typing import TYPE_CHECKING

import pytest

from watchdog.utils import platform

if not platform.is_linux():
    pytest.skip("GNU/Linux only.", allow_module_level=True)

from watchdog.events import DirCreatedEvent, FileCreatedEvent, FileDeletedEvent, FileMovedEvent, FileSystemEventHandler
from watchdog.observers.inotify import InotifyObserver, InotifyWatchGroup
from watchdog.observers.inotify_c import InotifyConstants, InotifyEvent, InotifyFD, Mask
from watchdog.observers.inotify_move_event_grouper import InotifyMoveEventGrouper

if TYPE_CHECKING:
    from pathlib import Path


def test_observer_does_not_initialize_shared_inotify_fd(monkeypatch):
    # monkeypatch restores the session fixture's shared instance after the test.
    monkeypatch.setattr(InotifyFD, "_instance", None)
    observer = InotifyObserver()
    try:
        assert InotifyFD._instance is None  # noqa: SLF001
    finally:
        observer.stop()


@pytest.mark.parametrize("running", [False, True])
def test_empty_observers_do_not_allocate_inotify_resources(monkeypatch, running):
    def unexpected_allocation():
        raise AssertionError("An observer without watches must not allocate an inotify instance")

    monkeypatch.setattr(InotifyFD, "_create_inotify_fd", staticmethod(unexpected_allocation))
    observers = []
    try:
        for _ in range(5):
            observer = InotifyObserver()
            observers.append(observer)
            if running:
                observer.start()
    finally:
        for observer in observers:
            observer.stop()
            if observer.is_alive():
                observer.join(timeout=5)


def test_observers_have_independent_inotify_fds(tmp_path):
    observers = []
    try:
        observers.extend(InotifyObserver() for _ in range(2))
        for observer in observers:
            observer.schedule(FileSystemEventHandler(), tmp_path)

        first, second = (observer._inotify_fd for observer in observers)  # noqa: SLF001
        assert first is not second
        assert first._inotify_fd != second._inotify_fd  # noqa: SLF001
        assert first.is_alive()
        assert second.is_alive()

        observers[0].stop()
        assert not first.is_alive()
        assert second.is_alive()
    finally:
        for observer in observers:
            observer.stop()
        assert all(not observer._inotify_fd.is_alive() for observer in observers)  # noqa: SLF001


@pytest.mark.parametrize("generate_full_events", [True, False])
def test_observer_delivers_created_events(tmp_path, generate_full_events):
    events = Queue()

    class Handler(FileSystemEventHandler):
        def on_created(self, event):
            events.put(event)

    observer = InotifyObserver(generate_full_events=generate_full_events)
    directories = [tmp_path / "first", tmp_path / "second"]
    try:
        for directory in directories:
            directory.mkdir()
            observer.schedule(Handler(), directory)
        assert all(emitter._inotify_fd is observer._inotify_fd for emitter in observer.emitters)  # noqa: SLF001
        observer.start()

        for directory in directories:
            marker = directory / "marker"
            marker.touch()
            assert events.get(timeout=5) == FileCreatedEvent(str(marker))
    finally:
        observer.stop()
        if observer.is_alive():
            observer.join(timeout=5)
        assert not observer.is_alive()
        assert not observer._inotify_fd.is_alive()  # noqa: SLF001


@pytest.mark.parametrize("recursive", [True, False])
@pytest.mark.parametrize("running", [True, False])
def test_missing_root_raises_and_can_be_retried(tmp_path, recursive, running):
    events = Queue()

    class Handler(FileSystemEventHandler):
        def on_any_event(self, event):
            events.put(event)

    handler = Handler()
    directory = tmp_path / "root"
    directory.mkdir()
    observer = InotifyObserver()
    try:
        if running:
            observer.start()
        else:
            observer.schedule(handler, directory, recursive=recursive, event_filter=[FileDeletedEvent])

        # The directory must exist when the kernel watch is registered, even
        # if it existed when the watch was scheduled before observer.start().
        directory.rmdir()
        if running:
            with pytest.raises(FileNotFoundError) as exc:
                observer.schedule(handler, directory, recursive=recursive, event_filter=[FileDeletedEvent])
        else:
            with pytest.raises(FileNotFoundError) as exc:
                observer.start()
        assert exc.value.errno == errno.ENOENT
        assert not observer.emitters

        directory.mkdir()
        marker = directory / "marker"
        marker.touch()
        observer.schedule(handler, directory, recursive=recursive, event_filter=[FileDeletedEvent])
        if not running:
            observer.start()
        marker.unlink()
        assert events.get(timeout=5) == FileDeletedEvent(str(marker))
    finally:
        observer.stop()
        if observer.is_alive():
            observer.join(timeout=5)
        assert not observer.is_alive()


@pytest.mark.parametrize("error", [errno.ENOENT, errno.EACCES])
@pytest.mark.parametrize("create_after_start", [True, False])
def test_recursive_watch_ignores_subdirectory_errors(tmp_path, monkeypatch, error, create_after_start):
    events = Queue()

    class Handler(FileSystemEventHandler):
        def on_any_event(self, event):
            events.put(event)

    skipped = tmp_path / "skipped"
    watched = tmp_path / "watched"
    ready = tmp_path / "ready"
    ready.touch()
    if not create_after_start:
        skipped.mkdir()
        watched.mkdir()

    observer = InotifyObserver()
    observer.schedule(Handler(), tmp_path, recursive=True, event_filter=[DirCreatedEvent, FileDeletedEvent])
    inotify_fd = observer._inotify_fd  # noqa: SLF001
    add_callback = inotify_fd.add_callback
    registration_attempted = Event()

    def add_callback_with_error(path, *args, **kwargs):
        if path == os.fsencode(skipped):
            registration_attempted.set()
            raise OSError(error, os.strerror(error), path)
        return add_callback(path, *args, **kwargs)

    monkeypatch.setattr(inotify_fd, "add_callback", add_callback_with_error)
    try:
        observer.start()
        if create_after_start:
            skipped.mkdir()
            watched.mkdir()
            assert events.get(timeout=5) == DirCreatedEvent(str(skipped))
            assert events.get(timeout=5) == DirCreatedEvent(str(watched))

        # Receiving the root event waits for both subdirectory registrations.
        ready.unlink()
        assert events.get(timeout=5) == FileDeletedEvent(str(ready))
        assert registration_attempted.is_set()

        marker = watched / "marker"
        marker.touch()
        marker.unlink()
        assert events.get(timeout=5) == FileDeletedEvent(str(marker))
    finally:
        observer.stop()
        if observer.is_alive():
            observer.join(timeout=5)
        assert not observer.is_alive()


def test_new_directory_filled_while_its_watches_are_added(tmp_path, monkeypatch):
    events = Queue()

    class Handler(FileSystemEventHandler):
        def on_any_event(self, event):
            events.put(event)

    tree = tmp_path / "tree"
    late = tree / "late"
    add_all_callbacks = InotifyWatchGroup._add_all_callbacks  # noqa: SLF001

    def add_all_callbacks_then_copy_more(self, path):
        add_all_callbacks(self, path)
        if path == os.fsencode(tree):
            # Like `cp -r` still copying: this appears after the watches for the new tree were added
            (late / "nested").mkdir(parents=True)
            (late / "file").touch()

    monkeypatch.setattr(InotifyWatchGroup, "_add_all_callbacks", add_all_callbacks_then_copy_more)
    observer = InotifyObserver()
    observer.schedule(
        Handler(), tmp_path, recursive=True, event_filter=[DirCreatedEvent, FileCreatedEvent, FileDeletedEvent]
    )
    try:
        observer.start()
        tree.mkdir()

        expected = {DirCreatedEvent(str(late / "nested")), FileCreatedEvent(str(late / "file"))}
        while expected:
            expected.discard(events.get(timeout=5))

        # The late directory is watched too
        marker = late / "nested" / "marker"
        marker.touch()
        marker.unlink()
        while events.get(timeout=5) != FileDeletedEvent(str(marker)):
            pass
    finally:
        observer.stop()
        observer.join(timeout=5)
        assert not observer.is_alive()


@pytest.mark.parametrize("recursive", [True, False])
@pytest.mark.parametrize("create_after_start", [True, False])
def test_filtered_events_in_subdirectory(tmp_path, recursive, create_after_start):
    events = Queue()

    class Handler(FileSystemEventHandler):
        def on_any_event(self, event):
            events.put(event)

    directory = tmp_path / "subdirectory"
    ready = tmp_path / "ready"
    finished = tmp_path / "finished"
    ready.touch()
    finished.touch()
    if not create_after_start:
        directory.mkdir()

    observer = InotifyObserver()
    observer.schedule(
        Handler(),
        str(tmp_path),
        recursive=recursive,
        event_filter=[FileMovedEvent, FileDeletedEvent],
    )
    try:
        observer.start()
        if create_after_start:
            directory.mkdir()

        # The root deletion follows mkdir on the same inotify descriptor, so
        # receiving it also waits for registration of the new directory watch.
        ready.unlink()
        assert events.get(timeout=5) == FileDeletedEvent(str(ready))

        source = directory / "source"
        destination = directory / "destination"
        source.touch()
        source.rename(destination)
        destination.unlink()
        finished.unlink()

        if recursive:
            assert events.get(timeout=5) == FileMovedEvent(str(source), str(destination))
            assert events.get(timeout=5) == FileDeletedEvent(str(destination))
        # This second root event waits for the preceding operations and also
        # checks that the filter excluded their create/modify/open/close events.
        assert events.get(timeout=5) == FileDeletedEvent(str(finished))
        assert events.empty()
    finally:
        observer.stop()
        observer.join(timeout=5)
        assert not observer.is_alive()


class _InjectedEvents:
    """Collects dispatched events and renders them as an acceptance table."""

    def __init__(self) -> None:
        self.rows: list[tuple[str, str, str, str]] = []
        self._lock = Lock()

    def handler(self, subscription: str) -> FileSystemEventHandler:
        collector = self

        class Handler(FileSystemEventHandler):
            def on_any_event(self, event):
                row = (subscription, event.event_type, os.fsdecode(event.src_path), os.fsdecode(event.dest_path))
                with collector._lock:
                    collector.rows.append(row)

        return Handler()

    def count(self) -> int:
        with self._lock:
            return len(self.rows)

    def wait(self, expected: int, timeout: float = 5) -> None:
        deadline = time.monotonic() + timeout
        while self.count() < expected and time.monotonic() < deadline:
            time.sleep(0.05)
        # Unmatched move halves are held for the grouper delay: wait for extras too.
        time.sleep(InotifyMoveEventGrouper.delay + 0.1)


def _events_table(rows: list[tuple[str, str, str, str]], base: Path) -> list[tuple[str, str, str, str]]:
    """The received rows, with the temporary directory folded into `<base>`."""
    return [
        (subscription, kind, src.replace(str(base), "<base>"), dest.replace(str(base), "<base>"))
        for subscription, kind, src, dest in rows
    ]


def _format_table(rows: list[tuple[str, str, str, str]]) -> str:
    """Render received events as the acceptance table, submitted sorted."""
    header = f"{'subscription':<16} {'event':<9} {'path':<32} {'dest_path'}"
    lines = [header, "-" * len(header)]
    lines.extend(f"{subscription:<16} {kind:<9} {path:<32} {dest}" for subscription, kind, path, dest in sorted(rows))
    return "\n".join(line.rstrip() for line in lines)


def _watch_group(observer: InotifyObserver, path: Path) -> InotifyWatchGroup:
    emitters = {emitter.watch.raw_path: emitter for emitter in observer.emitters}
    return emitters[str(path)]._inotify  # noqa: SLF001


def _raw_move(group: InotifyWatchGroup, path: Path, name: str, *, moved_from: bool) -> InotifyEvent:
    """A raw inotify event for `path/name`, as the kernel reports it."""
    mask = InotifyConstants.IN_MOVED_FROM if moved_from else InotifyConstants.IN_MOVED_TO
    wd = group._active_callbacks_by_path[os.fsencode(str(path))]  # noqa: SLF001
    return InotifyEvent(wd, Mask(mask), 42, os.fsencode(name))


@pytest.mark.parametrize("arrival", ["from_then_to", "to_then_from"])
def test_move_between_two_subscriptions_in_both_arrival_orders(tmp_path, arrival):
    """A downloader stages files in one directory and atomically moves them to
    another. The raw inotify halves are injected in both arrival orders; each
    subscription must report the paths it watches, and the acceptance table
    must not depend on the order the two halves arrived in.
    """
    base = tmp_path / "data"
    staging = base / "staging"
    ready = base / "ready"
    staging.mkdir(parents=True)
    ready.mkdir()

    events = _InjectedEvents()
    observer = InotifyObserver()
    observer.schedule(events.handler("staging"), staging)
    observer.schedule(events.handler("ready"), ready)
    observer.schedule(events.handler("parent"), base, recursive=True)
    try:
        observer.start()

        moved_from = _raw_move(_watch_group(observer, staging), staging, "recording.mp4", moved_from=True)
        moved_to = _raw_move(_watch_group(observer, ready), ready, "recording.mp4", moved_from=False)
        inotify_fd = observer._inotify_fd  # noqa: SLF001
        for event in [moved_from, moved_to] if arrival == "from_then_to" else [moved_to, moved_from]:
            inotify_fd.handle_event(event)

        events.wait(7)

        found = _events_table(events.rows, base)
        assert _format_table(found) == (
            "subscription     event     path                             dest_path\n"
            "---------------------------------------------------------------------\n"
            "parent           modified  <base>/ready\n"
            "parent           modified  <base>/staging\n"
            "parent           moved     <base>/staging/recording.mp4     <base>/ready/recording.mp4\n"
            "ready            created   <base>/ready/recording.mp4\n"
            "ready            modified  <base>/ready\n"
            "staging          deleted   <base>/staging/recording.mp4\n"
            "staging          modified  <base>/staging"
        )
    finally:
        observer.stop()
        if observer.is_alive():
            observer.join(timeout=5)


@pytest.mark.parametrize(
    ("subscription", "directory", "expected"),
    [
        (
            "source only",
            "staging",
            (
                "subscription     event     path                             dest_path\n"
                "---------------------------------------------------------------------\n"
                "source only      deleted   <base>/staging/recording.mp4\n"
                "source only      modified  <base>/staging"
            ),
        ),
        (
            "destination only",
            "ready",
            (
                "subscription     event     path                             dest_path\n"
                "---------------------------------------------------------------------\n"
                "destination only created   <base>/ready/recording.mp4\n"
                "destination only modified  <base>/ready"
            ),
        ),
    ],
)
def test_move_with_only_one_side_subscribed(tmp_path, subscription, directory, expected):
    """Subscribing to only one of the two directories keeps the semantics of
    that side: the move crossed its boundary, so it is a delete or a create."""
    base = tmp_path / "data"
    (base / "staging").mkdir(parents=True)
    (base / "ready").mkdir()
    side = base / directory

    events = _InjectedEvents()
    observer = InotifyObserver()
    observer.schedule(events.handler(subscription), side)
    try:
        observer.start()

        group = _watch_group(observer, side)
        inotify_fd = observer._inotify_fd  # noqa: SLF001
        # Only the half that this subscription's watch receives reaches it.
        inotify_fd.handle_event(_raw_move(group, side, "recording.mp4", moved_from=directory == "staging"))

        events.wait(2)
        assert _format_table(_events_table(events.rows, base)) == expected
    finally:
        observer.stop()
        if observer.is_alive():
            observer.join(timeout=5)


@pytest.mark.parametrize("arrival", ["from_then_to", "to_then_from"])
def test_rename_within_one_subscription(tmp_path, arrival):
    """A rename inside a watched directory is still one move event."""
    directory = tmp_path / "staging"
    directory.mkdir()

    events = _InjectedEvents()
    observer = InotifyObserver()
    observer.schedule(events.handler("staging"), directory)
    try:
        observer.start()

        group = _watch_group(observer, directory)
        inotify_fd = observer._inotify_fd  # noqa: SLF001
        moved_from = _raw_move(group, directory, "a.tmp", moved_from=True)
        moved_to = _raw_move(group, directory, "a", moved_from=False)
        for event in [moved_from, moved_to] if arrival == "from_then_to" else [moved_to, moved_from]:
            inotify_fd.handle_event(event)

        events.wait(2)

        assert _format_table(_events_table(events.rows, tmp_path)) == (
            "subscription     event     path                             dest_path\n"
            "---------------------------------------------------------------------\n"
            "staging          modified  <base>/staging\n"
            "staging          moved     <base>/staging/a.tmp             <base>/staging/a"
        )
    finally:
        observer.stop()
        if observer.is_alive():
            observer.join(timeout=5)
