"""Tests for snapshot-based recovery after an event queue overflow.

The recovery path is driven with a fake event source instead of the kernel:
the source emits an overflow signal, and a snapshot factory hook lets a test
inject a live event exactly while the rescan is reading the tree. A
re-runnable recording handler captures which stage every event landed in.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from watchdog.events import (
    DirCreatedEvent,
    FileCreatedEvent,
    FileDeletedEvent,
    FileModifiedEvent,
    FileSystemEvent,
    FileSystemEventHandler,
    RecoveryBeginEvent,
    RecoveryEndEvent,
    ResyncRequiredEvent,
)
from watchdog.observers.api import ObservedWatch
from watchdog.utils.dirsnapshot import DirectorySnapshot, DirectorySnapshotDiff, SnapshotUnstableError
from watchdog.utils.recovery import SnapshotRecoveryCoordinator, diff_to_events

from .shell import mkdir, rm, touch

STAGE_LIVE = "live"
STAGE_RECOVERY = "recovery"
STAGE_AFTER_RECOVERY = "after_recovery"


@dataclass
class RecordedEvent:
    """One entry in the handler's re-runnable event log."""

    stage: str
    kind: str
    src_path: str
    dest_path: str = ""
    is_synthetic: bool = False


class RecordingHandler(FileSystemEventHandler):
    """Logs each event with the stage it was dispatched in.

    The log only contains deterministic data (stage, event class, relative
    paths, synthetic flag), so re-running the same script yields the same
    record. The base class no-op hooks keep default observers compatible:
    dispatching recovery markers never raises.
    """

    def __init__(self, root: str) -> None:
        super().__init__()
        self.root = root
        self.log: list[RecordedEvent] = []
        self._stage = STAGE_LIVE

    def set_stage(self, stage: str) -> None:
        self._stage = stage

    def _relative(self, path: bytes | str) -> str:
        return os.path.relpath(os.fsdecode(path), self.root) if path else ""

    def dispatch(self, event: FileSystemEvent) -> None:
        self.log.append(
            RecordedEvent(
                stage=self._stage,
                kind=type(event).__name__,
                src_path=self._relative(event.src_path),
                dest_path=self._relative(event.dest_path),
                is_synthetic=event.is_synthetic,
            )
        )
        super().dispatch(event)


class FakeEventSource:
    """Scripted stand-in for a platform emitter (e.g. inotify).

    Tests append plain file system events or an overflow marker. The driver
    replays them in order; the marker is what the real watcher would send when
    its queue overflows.
    """

    @dataclass
    class Overflow:
        """Marker telling the driver to invoke overflow recovery."""

    def __init__(self) -> None:
        self.script: list[object] = []

    def event(self, event: FileSystemEvent) -> None:
        self.script.append(event)

    def overflow(self) -> None:
        self.script.append(self.Overflow())

    def next(self) -> object:
        return self.script.pop(0)

    def __bool__(self) -> bool:
        return bool(self.script)


def _coordinator(p, *, max_attempts: int = 3, on_snapshot_read=None):
    """Build a coordinator around a stability-checked snapshot factory.

    ``on_snapshot_read`` runs at the start of every snapshot read, which is
    how tests insert a live event while the rescan is in progress.
    """

    watch = ObservedWatch(p(""), recursive=True)

    def snapshot_factory() -> DirectorySnapshot:
        if on_snapshot_read is not None:
            on_snapshot_read()
        return DirectorySnapshot(p(""), recursive=True, stable=True)

    return SnapshotRecoveryCoordinator(watch, snapshot_factory=snapshot_factory, max_attempts=max_attempts)


def _drive(source: FakeEventSource, coordinator: SnapshotRecoveryCoordinator, handler: RecordingHandler):
    """Replay the fake source through the coordinator into the handler.

    Plain events are handed to the coordinator, which buffers them while a
    recovery is in progress and otherwise passes them straight through. An
    overflow marker runs a recovery whose whole output is dispatched in the
    ``recovery`` stage; buffered live events come out after the end marker.
    """
    while source:
        item = source.next()
        if isinstance(item, FakeEventSource.Overflow):
            # The coordinator owns the boundary: markers, compensation events
            # and the buffered live events all leave through the same call.
            handler.set_stage(STAGE_RECOVERY)

            def dispatch(event: FileSystemEvent) -> None:
                if isinstance(event, RecoveryEndEvent):
                    handler.dispatch(event)
                    handler.set_stage(STAGE_AFTER_RECOVERY)
                else:
                    handler.dispatch(event)

            coordinator.dispatch_events(dispatch)
        else:
            buffered = coordinator.handle_live_event(item)
            if not buffered:
                handler.dispatch(item)


def _stages(log: list[RecordedEvent], kind: str) -> list[str]:
    return [record.stage for record in log if record.kind == kind]


def _synthetic(log: list[RecordedEvent], kind: str) -> list[bool]:
    return [record.is_synthetic for record in log if record.kind == kind]


def test_overflow_recovers_missed_files_from_snapshot_diff(p):
    """An overflow with no mid-scan event produces only compensation events."""
    touch(p("present.txt"))
    coordinator = _coordinator(p)
    coordinator.update_known_state(DirectorySnapshot(p(""), recursive=True))

    # These changes are never observed by the fake source: they only exist on
    # disk when the recovery rescan runs.
    touch(p("created.txt"))
    rm(p("present.txt"))
    mkdir(p("newdir"))

    source = FakeEventSource()
    source.overflow()
    handler = RecordingHandler(p(""))
    _drive(source, coordinator, handler)

    kinds = [record.kind for record in handler.log]
    assert kinds[0] == "RecoveryBeginEvent"
    assert kinds[-1] == "RecoveryEndEvent"
    assert all(record.stage == STAGE_RECOVERY for record in handler.log)
    assert all(record.is_synthetic for record in handler.log)

    by_path = {record.src_path: record.kind for record in handler.log}
    assert by_path["present.txt"] == "FileDeletedEvent"
    assert by_path["created.txt"] == "FileCreatedEvent"
    assert by_path["newdir"] == "DirCreatedEvent"


def test_live_event_during_rescan_lands_after_recovery_end(p):
    """A live event injected while the snapshot is read is buffered, not lost."""
    touch(p("stable.txt"))

    live_event = FileCreatedEvent(p("mid_scan.txt"))

    def inject_live_event() -> None:
        # Runs inside the snapshot factory, i.e. while the rescan is reading.
        assert coordinator.is_recovering
        assert coordinator.handle_live_event(live_event)

    coordinator = _coordinator(p, on_snapshot_read=inject_live_event)
    coordinator.update_known_state(DirectorySnapshot(p(""), recursive=True))

    source = FakeEventSource()
    source.overflow()
    handler = RecordingHandler(p(""))
    _drive(source, coordinator, handler)

    kinds = [record.kind for record in handler.log]
    assert kinds[-1] == "FileCreatedEvent"
    assert handler.log[-1].src_path == "mid_scan.txt"
    assert _stages(handler.log, "FileCreatedEvent") == [STAGE_AFTER_RECOVERY]
    assert _synthetic(handler.log, "FileCreatedEvent") == [False]
    # The clear boundary is exactly one end marker right before the live event.
    assert handler.log[-2].kind == "RecoveryEndEvent"
    assert handler.log[-2].stage == STAGE_RECOVERY


def test_compensation_and_mid_scan_event_land_in_distinct_stages(p):
    """One overflow produces both a missed-file compensation and a mid-scan event.

    A single re-runnable record must show the missed file in the recovery
    stage as a synthetic compensation event, and the live event injected while
    the snapshot is read in the after-recovery stage as a normal event.
    """
    touch(p("present.txt"))

    live_event = FileCreatedEvent(p("mid_scan.txt"))

    def inject_live_event() -> None:
        assert coordinator.handle_live_event(live_event)

    coordinator = _coordinator(p, on_snapshot_read=inject_live_event)
    coordinator.update_known_state(DirectorySnapshot(p(""), recursive=True))

    # A change the fake source never observed: only the rescan can find it.
    rm(p("present.txt"))

    source = FakeEventSource()
    source.overflow()
    handler = RecordingHandler(p(""))
    _drive(source, coordinator, handler)

    kinds = [record.kind for record in handler.log]
    # Deleting present.txt also modifies its parent directory, so the
    # compensation stream contains both events before the end marker.
    assert kinds == [
        "RecoveryBeginEvent",
        "FileDeletedEvent",
        "DirModifiedEvent",
        "RecoveryEndEvent",
        "FileCreatedEvent",
    ]
    missed = next(record for record in handler.log if record.src_path == "present.txt")
    assert missed.stage == STAGE_RECOVERY
    assert missed.is_synthetic
    mid_scan = next(record for record in handler.log if record.src_path == "mid_scan.txt")
    assert mid_scan.stage == STAGE_AFTER_RECOVERY
    assert not mid_scan.is_synthetic

    # Replay the same script in an isolated directory: the record is identical.
    rerun_root = os.path.join(p(""), "rerun")
    mkdir(rerun_root)
    touch(os.path.join(rerun_root, "present.txt"))
    rerun_live = FileCreatedEvent(os.path.join(rerun_root, "mid_scan.txt"))
    rerun_watch = ObservedWatch(rerun_root, recursive=True)

    def rerun_factory() -> DirectorySnapshot:
        coordinator.handle_live_event(rerun_live)
        return DirectorySnapshot(rerun_root, recursive=True, stable=True)

    coordinator = SnapshotRecoveryCoordinator(rerun_watch, snapshot_factory=rerun_factory)
    coordinator.update_known_state(DirectorySnapshot(rerun_root, recursive=True))
    rm(os.path.join(rerun_root, "present.txt"))

    rerun_source = FakeEventSource()
    rerun_source.overflow()
    rerun_handler = RecordingHandler(rerun_root)
    _drive(rerun_source, coordinator, rerun_handler)

    assert [record.kind for record in rerun_handler.log] == kinds
    assert [
        (record.stage, record.is_synthetic) for record in rerun_handler.log
    ] == [(record.stage, record.is_synthetic) for record in handler.log]


def test_unstable_snapshot_emits_resync_notification(p):
    """If the tree never settles, callers get an explicit resync notification."""

    def always_unstable() -> DirectorySnapshot:
        raise SnapshotUnstableError("still changing")

    watch = ObservedWatch(p(""), recursive=True)
    coordinator = SnapshotRecoveryCoordinator(watch, snapshot_factory=always_unstable, max_attempts=2)
    known = DirectorySnapshot(p(""), recursive=True)
    coordinator.update_known_state(known)

    events, new_snapshot = coordinator.recover_from_overflow()

    assert new_snapshot is None
    assert [type(event) for event in events] == [ResyncRequiredEvent]
    assert events[0].is_synthetic
    # The pre-overflow state is preserved for a later retry.
    assert coordinator._known_snapshot is known  # noqa: SLF001


def test_recovery_markers_are_compatible_with_default_handler(p):
    """The plain base handler must accept every recovery marker without errors."""
    handler = FileSystemEventHandler()
    for event in (
        RecoveryBeginEvent(p("")),
        RecoveryEndEvent(p("")),
        ResyncRequiredEvent(p("")),
    ):
        handler.dispatch(event)  # must not raise


def test_recorded_log_is_deterministic_across_runs(p):
    """Re-running the same fake source script yields the same event record.

    Each run replays in its own subdirectory so no file system state leaks
    from one run into the next.
    """

    def run_once(run: str) -> list[RecordedEvent]:
        root = os.path.join(p(""), run)
        mkdir(root)
        touch(os.path.join(root, "stable.txt"))

        live_event = FileCreatedEvent(os.path.join(root, "mid_scan.txt"))

        def inject() -> None:
            coordinator.handle_live_event(live_event)

        watch = ObservedWatch(root, recursive=True)

        def snapshot_factory() -> DirectorySnapshot:
            inject()
            return DirectorySnapshot(root, recursive=True, stable=True)

        coordinator = SnapshotRecoveryCoordinator(watch, snapshot_factory=snapshot_factory)
        coordinator.update_known_state(DirectorySnapshot(root, recursive=True))
        touch(os.path.join(root, "missed.txt"))

        source = FakeEventSource()
        source.event(FileModifiedEvent(os.path.join(root, "stable.txt")))
        source.overflow()
        handler = RecordingHandler(root)
        _drive(source, coordinator, handler)
        return handler.log

    first = run_once("run1")
    second = run_once("run2")
    assert first == second


def test_recovery_stream_dispatches_through_base_observer(p):
    """The coordinator's output survives the default observer dispatch path.

    ``BaseObserver.dispatch_events`` must hand every marker and compensation
    event to the registered handler unchanged, so default observers keep
    working without knowing about recovery.
    """
    from watchdog.observers.api import BaseObserver, EventEmitter

    touch(p("stable.txt"))
    coordinator = _coordinator(p)
    coordinator.update_known_state(DirectorySnapshot(p(""), recursive=True))
    touch(p("missed.txt"))

    source = FakeEventSource()
    source.overflow()
    recovery_events, _ = coordinator.recover_from_overflow()
    assert recovery_events

    observer = BaseObserver(EventEmitter)
    handler = RecordingHandler(p(""))
    watch = observer.schedule(handler, p(""), recursive=True)
    for event in recovery_events:
        observer.event_queue.put((event, watch))
        observer.dispatch_events(observer.event_queue)

    assert [record.kind for record in handler.log] == [type(event).__name__ for event in recovery_events]
    assert all(record.is_synthetic for record in handler.log)
    assert all(record.stage == STAGE_LIVE for record in handler.log)
    observer.unschedule_all()


def test_diff_to_events_marks_everything_synthetic(p):
    """Unit check for the diff conversion used by the coordinator."""
    ref = DirectorySnapshot(p(""))
    mkdir(p("d"))
    touch(p("f"))
    diff = DirectorySnapshotDiff(ref, DirectorySnapshot(p("")))

    events = diff_to_events(diff)
    assert events
    assert all(event.is_synthetic for event in events)
    kinds = {type(event) for event in events}
    assert DirCreatedEvent in kinds
    assert FileCreatedEvent in kinds

