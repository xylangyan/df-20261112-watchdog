""":module: watchdog.utils.recovery
:synopsis: Snapshot-based recovery after an event queue overflow.

When a watcher reports that its queue overflowed (e.g. inotify's
``IN_Q_OVERFLOW``) some events were dropped and can never be delivered. This
module reconciles the watch with the file system using directory snapshots:

* the *last known state* is the most recent snapshot that was still
  consistent with what had been dispatched so far;
* the *rescanned state* is a fresh, stability-checked snapshot;
* the diff between the two produces compensation events, each marked
  ``is_synthetic=True`` and bracketed by :class:`RecoveryBeginEvent` and
  :class:`RecoveryEndEvent`;
* if no consistent snapshot can be obtained, a
  :class:`ResyncRequiredEvent` is emitted instead.

Live events that arrive while the rescan is in progress are held back behind
the :class:`RecoveryEndEvent` marker, so normal dispatch resumes from a clear
boundary.

Classes
-------
.. autoclass:: SnapshotRecoveryCoordinator
   :members:
   :show-inheritance:

"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from watchdog.events import (
    DirAttribEvent,
    DirCreatedEvent,
    DirDeletedEvent,
    DirModifiedEvent,
    DirMovedEvent,
    FileAttribEvent,
    FileCreatedEvent,
    FileDeletedEvent,
    FileModifiedEvent,
    FileMovedEvent,
    FileSystemEvent,
    RecoveryBeginEvent,
    RecoveryEndEvent,
    ResyncRequiredEvent,
)
from watchdog.utils.dirsnapshot import (
    DirectorySnapshot,
    DirectorySnapshotDiff,
    EmptyDirectorySnapshot,
    SnapshotUnstableError,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from watchdog.observers.api import ObservedWatch

#: Maximum number of stability-checked rescans before giving up and asking
#: for a full resynchronization.
DEFAULT_MAX_ATTEMPTS = 3


def diff_to_events(diff: DirectorySnapshotDiff) -> list[FileSystemEvent]:
    """Convert a snapshot diff into compensation events.

    Ordering mirrors :class:`watchdog.observers.polling.PollingEmitter` so the
    produced stream looks like ordinary polled events.
    """
    events: list[FileSystemEvent] = []

    for src_path in diff.files_deleted:
        events.append(FileDeletedEvent(src_path, is_synthetic=True))
    for src_path in diff.files_modified:
        events.append(FileModifiedEvent(src_path, is_synthetic=True))
    for src_path in diff.files_created:
        events.append(FileCreatedEvent(src_path, is_synthetic=True))
    for src_path, dest_path in diff.files_moved:
        events.append(FileMovedEvent(src_path, dest_path, is_synthetic=True))
    for src_path in diff.files_attrib:
        events.append(FileAttribEvent(src_path, is_synthetic=True))

    for src_path in diff.dirs_deleted:
        events.append(DirDeletedEvent(src_path, is_synthetic=True))
    for src_path in diff.dirs_modified:
        events.append(DirModifiedEvent(src_path, is_synthetic=True))
    for src_path in diff.dirs_created:
        events.append(DirCreatedEvent(src_path, is_synthetic=True))
    for src_path, dest_path in diff.dirs_moved:
        events.append(DirMovedEvent(src_path, dest_path, is_synthetic=True))
    for src_path in diff.dirs_attrib:
        events.append(DirAttribEvent(src_path, is_synthetic=True))

    return events


class SnapshotRecoveryCoordinator:
    """Coordinates snapshot-based recovery after an overflow.

    One coordinator is used per watch. It holds the last *known state*
    snapshot and, on overflow, rescans the tree to compute the compensation
    events. While a recovery is in progress, live events are buffered; they
    are flushed after the :class:`RecoveryEndEvent` so consumers see a clear
    boundary between compensation and normal dispatch.

    :param watch:
        The watch being recovered. Its ``path`` and ``is_recursive`` select
        the snapshot scope.
    :type watch:
        :class:`watchdog.observers.api.ObservedWatch`
    :param snapshot_factory:
        Callable returning a fresh :class:`DirectorySnapshot` of the watched
        tree. Defaults to a plain recursive/non-recursive snapshot of the
        watch path.
    :param max_attempts:
        Number of stability-checked rescans to attempt before emitting a
        :class:`ResyncRequiredEvent`.
    """

    def __init__(
        self,
        watch: ObservedWatch,
        *,
        snapshot_factory: Callable[[], DirectorySnapshot] | None = None,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    ) -> None:
        self._watch = watch
        if snapshot_factory is None:
            snapshot_factory = lambda: DirectorySnapshot(
                watch.path,
                recursive=watch.is_recursive,
                stable=True,
            )
        self._snapshot_factory = snapshot_factory
        if max_attempts < 1:
            msg = "max_attempts must be >= 1"
            raise ValueError(msg)
        self._max_attempts = max_attempts
        self._lock = threading.Lock()
        self._known_snapshot: DirectorySnapshot = EmptyDirectorySnapshot()
        self._recovering = False
        self._pending_live_events: list[FileSystemEvent] = []

    @property
    def watch(self) -> ObservedWatch:
        """The watch this coordinator is responsible for."""
        return self._watch

    @property
    def is_recovering(self) -> bool:
        """``True`` while a recovery rescan is in progress."""
        with self._lock:
            return self._recovering

    def update_known_state(self, snapshot: DirectorySnapshot) -> None:
        """Replace the last known state snapshot.

        Call this whenever the watcher has dispatched everything that had
        been observed up to ``snapshot`` (e.g. after a successful poll).
        No-op while a recovery is in progress so the recovery keeps using
        the pre-overflow state as its reference.
        """
        with self._lock:
            if self._recovering:
                return
            self._known_snapshot = snapshot

    def take_snapshot(self) -> DirectorySnapshot:
        """Take a stability-checked snapshot, retrying until the tree settles.

        :raises SnapshotUnstableError:
            If the tree keeps changing for ``max_attempts`` rescans.
        """
        last_error: SnapshotUnstableError | None = None
        for _ in range(self._max_attempts):
            try:
                snapshot = self._snapshot_factory()
            except SnapshotUnstableError as exc:
                last_error = exc
                continue
            if snapshot.stable:
                return snapshot
            last_error = SnapshotUnstableError("snapshot was not stability-checked")
        assert last_error is not None
        raise last_error

    def handle_live_event(self, event: FileSystemEvent) -> bool:
        """Buffer a live event while a recovery is in progress.

        :returns:
            ``True`` if the event was buffered (recovery in progress, the
            caller must not dispatch it); ``False`` if normal dispatch should
            proceed immediately.
        """
        with self._lock:
            if not self._recovering:
                return False
            self._pending_live_events.append(event)
            return True

    def dispatch_events(
        self,
        dispatch: Callable[[FileSystemEvent], None],
    ) -> DirectorySnapshot | None:
        """Run a recovery cycle and dispatch its output through ``dispatch``.

        This is the boundary-enforcing entry point:

        * every recovery marker and compensation event is passed to
          ``dispatch`` in order;
        * a :class:`ResyncRequiredEvent` is passed instead when no consistent
          snapshot could be taken;
        * live events buffered during the rescan are then passed to
          ``dispatch`` after the end marker, so normal dispatch resumes at a
          clear point.

        :param dispatch:
            Callable that receives each event in submission order (typically
            an observer's queue ``put`` or a handler's ``dispatch``).
        :returns:
            The new known-state snapshot, or ``None`` when a resync is
            required.
        """
        events, new_snapshot = self.recover_from_overflow()
        for event in events:
            dispatch(event)
        return new_snapshot

    def recover_from_overflow(self) -> tuple[list[FileSystemEvent], DirectorySnapshot | None]:
        """Run one full recovery cycle after an overflow is reported.

        Returns a tuple of ``(events, new_snapshot)``:

        * on success, ``events`` starts with :class:`RecoveryBeginEvent`,
          contains the synthetic compensation events and ends with
          :class:`RecoveryEndEvent`; ``new_snapshot`` becomes the new known
          state.
        * on failure, ``events`` contains :class:`ResyncRequiredEvent` only
          and ``new_snapshot`` is ``None``; the previous known state is kept.

        Live events received during the rescan are appended after the end
        marker. The whole sequence must be dispatched in order.
        """
        with self._lock:
            if self._recovering:
                msg = "recovery already in progress"
                raise RuntimeError(msg)
            self._recovering = True
            known_snapshot = self._known_snapshot

        events: list[FileSystemEvent] = []
        new_snapshot: DirectorySnapshot | None = None
        try:
            try:
                new_snapshot = self.take_snapshot()
            except SnapshotUnstableError:
                events = [ResyncRequiredEvent(self._watch.path)]
                new_snapshot = None
            else:
                diff = DirectorySnapshotDiff(known_snapshot, new_snapshot)
                events = [
                    RecoveryBeginEvent(self._watch.path),
                    *diff_to_events(diff),
                    RecoveryEndEvent(self._watch.path),
                ]
        finally:
            with self._lock:
                self._recovering = False
                if new_snapshot is not None:
                    self._known_snapshot = new_snapshot
                events.extend(self._pending_live_events)
                self._pending_live_events.clear()
        return events, new_snapshot
