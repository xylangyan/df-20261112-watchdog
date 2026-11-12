""":module: watchdog.utils.file_ready_notifier
:synopsis: Optional per-file readiness notifications for event dispatch.

Some producers write files in place. A watcher that reacts to the first
modification event can then read a half-written file. :class:`FileReadyNotifier`
is both an event handler and a background thread: schedule it on an observer,
start it, and it forwards a :class:`~watchdog.events.FileReadyEvent` to its
inner event handler once a reported file's size and modification time have
remained unchanged for a settle interval.

Ready checks are tracked per file, so a small file that has settled is
reported while a large file in the same directory is still being written.
"""

from __future__ import annotations

import dataclasses
import os
import stat
import threading
import time
from typing import TYPE_CHECKING

from watchdog.events import (
    EVENT_TYPE_DELETED,
    EVENT_TYPE_MOVED,
    EVENT_TYPE_READY,
    FileReadyEvent,
    FileSystemEventHandler,
)
from watchdog.utils import BaseThread

if TYPE_CHECKING:
    from watchdog.events import FileSystemEvent
    from watchdog.utils import Seconds

_StatSignature = tuple[int, int, int]
"""(size, mtime_ns, inode), enough to tell a re-created file from the previous one."""


@dataclasses.dataclass
class _PendingFile:
    """Metadata and deadline of a file waiting to settle."""

    path: bytes | str
    signature: _StatSignature
    due: float
    """:func:`time.monotonic` value at which the file becomes ready."""
    watch_path: bytes | str | None = None
    """The watch root carried by the event, used by path-matching handlers."""


class FileReadyNotifier(FileSystemEventHandler, BaseThread):
    """Optional per-file readiness notifications for event dispatch.

    This handler forwards every event to ``event_handler`` unchanged. It
    additionally monitors regular files through :func:`os.stat` and forwards
    a :class:`~watchdog.events.FileReadyEvent` to ``event_handler`` once the
    file's size and modification time have stopped changing for
    ``settle_interval``. The ready event carries the last reported path of the
    file; ``dest_path`` is empty.

    Files are tracked independently: a small file settling does not wait for
    a large file reported at the same time, and an event for an already
    pending path only restarts that path's timer.

    A pending file that is deleted or renamed away before it settles
    produces no notification. A move to a watched destination starts a fresh
    settle interval there. If a file is deleted (or renamed away) and
    re-created at the same path, the new file starts a new settle interval:
    the previous inode is never accepted as evidence that the new file is
    ready.

    Files are polled every ``poll_interval`` while at least one file is
    pending. A notifier with nothing pending waits without waking up, so an
    idle notifier consumes no CPU.

    The notifier has to be started explicitly, before or after scheduling it
    on an observer. Stopping it drops all pending files: files that had not
    settled at that point are never reported.

    :param event_handler:
        The handler receiving the original events and the readiness events.
    :param settle_interval:
        How long the size and modification time must remain unchanged before a
        file is reported ready. ``0`` reports a file on the next poll.
    :type settle_interval:
        ``Seconds``
    :param poll_interval:
        Minimum delay between two metadata polls while files are pending.
    :type poll_interval:
        ``Seconds``

    Example::

        handler = MyHandler()
        ready = FileReadyNotifier(handler, settle_interval=2)
        observer.schedule(ready, "ready", recursive=False)
        ready.start()
        observer.start()
        ...
        ready.stop()
        ready.join()
    """

    def __init__(
        self,
        event_handler: FileSystemEventHandler,
        settle_interval: Seconds,
        *,
        poll_interval: Seconds = 0.1,
    ) -> None:
        FileSystemEventHandler.__init__(self)
        BaseThread.__init__(self)
        if settle_interval < 0:
            error = "settle_interval must be non-negative."
            raise ValueError(error)
        if poll_interval <= 0:
            error = "poll_interval must be positive."
            raise ValueError(error)
        self.event_handler = event_handler
        self.settle_interval = settle_interval
        self.poll_interval = poll_interval

        self._cond = threading.Condition()
        self._pending: dict[bytes | str, _PendingFile] = {}

    def dispatch(self, event: FileSystemEvent) -> None:
        """Forward *event* and start monitoring it if it is a regular file."""
        self.event_handler.dispatch(event)
        self.handle_event(event)

    def handle_event(self, event: FileSystemEvent) -> None:
        """Report a file to monitor.

        Directory events, synthetic events, and readiness events are ignored.
        The timer of an already pending path is restarted: the path becomes
        ready ``settle_interval`` after its last reported change. Deleted
        events cancel the check of their path; moved events cancel the source
        path and start a fresh check for the destination path.

        :param event:
            The most recent event reported for the file.
        """
        if not self.should_keep_running() or event.is_directory or event.is_synthetic:
            return

        if event.event_type == EVENT_TYPE_READY:
            return
        if event.event_type == EVENT_TYPE_DELETED:
            # The file is gone by the time the event is reported: cancel any
            # pending check instead of risking a late notification.
            self._forget(event.src_path)
            return

        watch_path = getattr(event, "watch_path", None)
        if event.event_type == EVENT_TYPE_MOVED and event.dest_path:
            self._forget(event.src_path)
            self._track_path(event.dest_path, watch_path)
            return

        self._track_path(event.src_path, watch_path)

    def _track_path(self, path: bytes | str, watch_path: bytes | str | None) -> None:
        stat_result = self._stat_signature(path)
        if stat_result is None or not stat.S_ISREG(stat_result[3]):
            # Missing or no longer a regular file: forget a previous
            # incarnation so the next one settles from scratch.
            self._forget(path)
            return
        self._track(path, stat_result[:3], watch_path)

    def stop(self) -> None:
        """Stop the notifier without reporting files that are not ready yet."""
        with self._cond:
            super().stop()
            self._pending.clear()
            self._cond.notify()

    def pending_paths(self) -> set[bytes | str]:
        """Return the paths currently waiting to settle."""
        with self._cond:
            return set(self._pending)

    def _now(self) -> float:
        """Current monotonic time; overridable for tests."""
        return time.monotonic()

    def _stat_signature(self, path: bytes | str) -> tuple[int, int, int, int] | None:
        """Return ``(size, mtime_ns, inode, mode)`` for *path*, or None if missing."""
        try:
            result = os.stat(path)
        except (FileNotFoundError, NotADirectoryError):
            return None
        return result.st_size, result.st_mtime_ns, result.st_ino, result.st_mode

    def _forget(self, path: bytes | str) -> None:
        with self._cond:
            self._pending.pop(path, None)
            self._cond.notify()

    def _track(self, path: bytes | str, signature: _StatSignature, watch_path: bytes | str | None) -> None:
        pending = _PendingFile(
            path=path,
            signature=signature,
            due=self._now() + self.settle_interval,
            watch_path=watch_path,
        )
        with self._cond:
            self._pending[path] = pending
            self._cond.notify()

    def _poll_ready(self) -> list[_PendingFile]:
        """Check every pending file, with the lock held, and return ready ones."""
        now = self._now()
        ready: list[_PendingFile] = []
        for path, pending in list(self._pending.items()):
            stat_result = self._stat_signature(path)
            if stat_result is None:
                # Deleted or renamed while waiting: silent, as documented.
                del self._pending[path]
                continue
            signature = stat_result[:3]
            if signature != pending.signature:
                # Still being written, or replaced by a new file: start over.
                self._pending[path] = dataclasses.replace(
                    pending,
                    signature=signature,
                    due=now + self.settle_interval,
                )
                continue
            if now >= pending.due:
                del self._pending[path]
                ready.append(pending)
        return ready

    def _next_delay(self) -> float | None:
        """How long the run loop may sleep, with the lock held.

        Returns None when nothing is pending: the loop then waits until
        :meth:`handle_event` or :meth:`stop` wakes it up.
        """
        if not self._pending:
            return None
        upcoming = min(pending.due for pending in self._pending.values())
        return max(0.0, min(upcoming - self._now(), self.poll_interval))

    def _poll_and_notify(self) -> None:
        """Check the pending files once and deliver the ready ones."""
        if not self.should_keep_running():
            # Stopping drops pending files: no late notification.
            return
        with self._cond:
            ready = self._poll_ready()

        # Notifications are delivered without the lock: the handler is user
        # code and may call back into this notifier.
        for pending in ready:
            if not self.should_keep_running():
                return
            stat_result = self._stat_signature(pending.path)
            if stat_result is None or stat_result[:3] != pending.signature:
                # Changed (or vanished) between the check and delivery:
                # restart its interval instead of reporting stale data.
                if self.should_keep_running():
                    self._track_path(pending.path, pending.watch_path)
                continue
            self._notify(pending)

    def run(self) -> None:
        """Poll pending files and notify those that stopped changing."""
        while self.should_keep_running():
            with self._cond:
                delay = self._next_delay()
                if delay is None or delay > 0:
                    self._cond.wait(timeout=delay)
                    continue
            self._poll_and_notify()

        # Files still pending are intentionally dropped: shutting down must
        # not deliver a late readiness notification.

    def _notify(self, pending: _PendingFile) -> None:
        """Dispatch a readiness event for a settled file."""
        ready_event = FileReadyEvent(pending.path, is_synthetic=True)
        if pending.watch_path is not None:
            ready_event.watch_path = pending.watch_path
        self.event_handler.dispatch(ready_event)
