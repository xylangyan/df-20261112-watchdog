from __future__ import annotations

import contextlib
import os
import queue
import threading
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING

from watchdog.events import FileSystemMovedEvent, PathMatchingEventHandler, attach_rule_snapshots
from watchdog.utils import BaseThread
from watchdog.utils.bricks import SkipRepeatsQueue

if TYPE_CHECKING:
    import types
    from collections.abc import Callable
    from typing import Any, Self

    from watchdog.events import FileSystemEvent, FileSystemEventHandler, QueuedEventPolicy
    from watchdog.utils import Seconds

DEFAULT_EMITTER_TIMEOUT: Seconds = 1.0
DEFAULT_OBSERVER_TIMEOUT: Seconds = 1.0


class EventQueue(SkipRepeatsQueue):
    """Thread-safe event queue based on a special queue that skips adding
    the same event (:class:`watchdog.events.FileSystemEvent`) multiple times
    consecutively. Thus avoiding dispatching multiple event handling
    calls when multiple identical events are produced quicker than an observer
    can consume them.

    :param maxsize:
        Maximum number of items allowed in the queue. If <= 0, the queue size is infinite.
    :type maxsize:
        ``int``
    """


class ObservedWatch:
    """An scheduled watch.

    :param path:
        Path string.
    :type path:
        ``str`` or :class:`pathlib.Path`
    :param recursive:
        ``True`` if watch should include subdirectories; ``False`` otherwise.
    :type recursive:
        ``bool``
    :param event_filter:
        Optional collection of :class:`watchdog.events.FileSystemEvent` to watch.
    :type event_filter:
        Iterable[:class:`watchdog.events.FileSystemEvent`] | None
    :param follow_symlink:
        ``True`` if symlinks are followed; ``False`` otherwise.
    :type follow_symlink:
        ``bool``
    """

    def __init__(
        self,
        path: str | Path,
        *,
        recursive: bool,
        event_filter: list[type[FileSystemEvent]] | None = None,
        follow_symlink: bool = False,
    ):
        raw_path = str(path) if isinstance(path, Path) else path
        self.raw_path = raw_path
        self.follow_symlink = follow_symlink
        self.event_filter = frozenset(event_filter) if event_filter is not None else None

        self.is_file = os.path.isfile(raw_path)
        self.resolved_path = (
            os.path.realpath(os.path.abspath(raw_path)) if follow_symlink else os.path.abspath(raw_path)
        )

        if self.is_file:
            self.path = os.path.dirname(self.resolved_path)
            self.is_recursive = False
        else:
            self.path = raw_path
            self.is_recursive = recursive

    @property
    def key(self) -> tuple[str, bool, frozenset[type[FileSystemEvent]] | None, bool]:
        """A tuple key identifying the watch (raw_path, recursive, event_filter, follow_symlink)."""
        return self.raw_path, self.is_recursive, self.event_filter, self.follow_symlink

    def __eq__(self, watch: object) -> bool:
        if not isinstance(watch, ObservedWatch):
            return NotImplemented
        return self.key == watch.key

    def __ne__(self, watch: object) -> bool:
        if not isinstance(watch, ObservedWatch):
            return NotImplemented
        return self.key != watch.key

    def __hash__(self) -> int:
        return hash(self.key)

    def __repr__(self) -> str:
        if self.event_filter is not None:
            event_filter_str = "|".join(sorted(_cls.__name__ for _cls in self.event_filter))
            event_filter_str = f", event_filter={event_filter_str}"
        else:
            event_filter_str = ""
        return f"<{type(self).__name__}: path={self.path!r}, is_recursive={self.is_recursive}{event_filter_str}>"


# Observer classes
class EventEmitter(BaseThread):
    """Producer thread base class subclassed by event emitters
    that generate events and populate a queue with them.

    :param event_queue:
        The event queue to populate with generated events.
    :type event_queue:
        :class:`EventQueue`
    :param watch:
        The watch to observe and produce events for.
    :type watch:
        :class:`ObservedWatch`
    :param timeout:
        Timeout between successive attempts at reading events.
    :type timeout:
        ``Seconds``
    :param event_filter:
        Collection of event types to emit, or None for no filtering (default).
    :type event_filter:
        Iterable[:class:`watchdog.events.FileSystemEvent`] | None
    """

    def __init__(
        self,
        event_queue: EventQueue,
        watch: ObservedWatch,
        *,
        timeout: Seconds = DEFAULT_EMITTER_TIMEOUT,
        event_filter: list[type[FileSystemEvent]] | None = None,
    ) -> None:
        super().__init__()
        # bool subclasses int; timeout=True would silently become 1.0s
        if isinstance(timeout, bool):
            error = f"timeout must be a float, not bool (got {timeout!r})"
            raise TypeError(error)
        self._event_queue = event_queue
        self._watch = watch
        self._timeout = timeout
        self._event_filter = frozenset(event_filter) if event_filter is not None else None
        # Injected by BaseObserver after construction; returns the handlers of
        # this emitter's watch so rule snapshots can be frozen at queue time.
        self._handlers_provider: Callable[[ObservedWatch], set[FileSystemEventHandler]] | None = None

    @property
    def timeout(self) -> Seconds:
        """Blocking timeout for reading events."""
        return self._timeout

    @property
    def watch(self) -> ObservedWatch:
        """The watch associated with this emitter."""
        return self._watch

    def queue_event(self, event: FileSystemEvent) -> None:
        """Queues a single event.

        :param event:
            Event to be queued.
        :type event:
            An instance of :class:`watchdog.events.FileSystemEvent`
            or a subclass.
        """
        if self._event_filter is None or any(isinstance(event, cls) for cls in self._event_filter):
            # Freeze the current matching rules on the event before it is
            # queued, so runtime filter updates can keep applying old rules
            # to already-queued events (QueuedEventPolicy.OLD_RULES).
            if self._handlers_provider is not None:
                attach_rule_snapshots(event, self._handlers_provider(self._watch))
            self._event_queue.put((event, self.watch))

    def queue_events(self, timeout: Seconds) -> None:
        """Override this method to populate the event queue with events
        per interval period.

        :param timeout:
            Timeout between successive attempts at
            reading events.
        :type timeout:
            ``Seconds``
        """

    def run(self) -> None:
        while self.should_keep_running():
            self.queue_events(self.timeout)


class EventDispatcher(BaseThread):
    """Consumer thread base class subclassed by event observer threads
    that dispatch events from an event queue to appropriate event handlers.

    :param timeout:
        Timeout value passed to emitters
        constructions in the child class BaseObserver.
    :type timeout:
        ``Seconds``
    """

    stop_event = object()
    """Event inserted into the queue to signal a requested stop."""

    def __init__(self, *, timeout: Seconds = DEFAULT_OBSERVER_TIMEOUT) -> None:
        super().__init__()
        # bool subclasses int; timeout=True would silently become 1.0s
        if isinstance(timeout, bool):
            error = f"timeout must be a float, not bool (got {timeout!r})"
            raise TypeError(error)
        self._event_queue = EventQueue()
        self._timeout = timeout

    @property
    def timeout(self) -> Seconds:
        """Timeout value to construct emitters with."""
        return self._timeout

    def stop(self) -> None:
        BaseThread.stop(self)
        with contextlib.suppress(queue.Full):
            self.event_queue.put_nowait(EventDispatcher.stop_event)

    @property
    def event_queue(self) -> EventQueue:
        """The event queue which is populated with file system events
        by emitters and from which events are dispatched by a dispatcher
        thread.
        """
        return self._event_queue

    def dispatch_events(self, event_queue: EventQueue) -> None:
        """Override this method to consume events from an event queue, blocking
        on the queue for the specified timeout before raising :class:`queue.Empty`.

        :param event_queue:
            Event queue to populate with one set of events.
        :type event_queue:
            :class:`EventQueue`
        :raises:
            :class:`queue.Empty`
        """

    def run(self) -> None:
        while self.should_keep_running():
            try:
                self.dispatch_events(self.event_queue)
            except queue.Empty:
                continue


class BaseObserver(EventDispatcher):
    """Base observer.

    :param emitter_class:
        The class of the emitter to use (e.g., a subclass of :class:`EventEmitter`).
    :type emitter_class:
        ``type[EventEmitter]``
    :param timeout:
        The timeout for the observer to wait on events.
    :type timeout:
        ``Seconds``
    :param kwargs:
        Extra keyword arguments forwarded to every emitter this observer
        constructs, for platform-specific emitter options.
    """

    def __init__(
        self,
        emitter_class: type[EventEmitter],
        *,
        timeout: Seconds = DEFAULT_OBSERVER_TIMEOUT,
        **kwargs: Any,
    ) -> None:
        super().__init__(timeout=timeout)
        self._emitter_class = emitter_class
        self._emitter_kwargs = kwargs
        self._lock = threading.RLock()
        self._watches: set[ObservedWatch] = set()
        self._handlers: defaultdict[ObservedWatch, set[FileSystemEventHandler]] = defaultdict(set)
        self._emitters: set[EventEmitter] = set()
        self._emitter_for_watch: dict[ObservedWatch, EventEmitter] = {}

    def _add_emitter(self, emitter: EventEmitter) -> None:
        self._emitter_for_watch[emitter.watch] = emitter
        self._emitters.add(emitter)

    def _remove_emitter(self, emitter: EventEmitter) -> None:
        del self._emitter_for_watch[emitter.watch]
        self._emitters.remove(emitter)
        emitter.stop()
        with contextlib.suppress(RuntimeError):
            emitter.join()

    def _clear_emitters(self) -> None:
        for emitter in self._emitters:
            emitter.stop()
        for emitter in self._emitters:
            with contextlib.suppress(RuntimeError):
                emitter.join()
        self._emitters.clear()
        self._emitter_for_watch.clear()

    def _add_handler_for_watch(self, event_handler: FileSystemEventHandler, watch: ObservedWatch) -> None:
        self._handlers[watch].add(event_handler)

    def _remove_handlers_for_watch(self, watch: ObservedWatch) -> None:
        del self._handlers[watch]

    def _handlers_for_watch(self, watch: ObservedWatch) -> set[FileSystemEventHandler]:
        # Snapshot the handler set: emitters call this from their own thread
        # while queueing events, and set membership must remain stable while
        # rule snapshots are attached. .get() avoids resurrecting removed watches.
        return set(self._handlers.get(watch, ()))

    @property
    def emitters(self) -> set[EventEmitter]:
        """Returns event emitter created by this observer."""
        return self._emitters

    def start(self) -> None:
        """Starts the observer thread and all scheduled emitters under synchronization lock.

        Overrides :meth:`watchdog.utils.BaseThread.start` to guarantee atomicity between
        observer startup and concurrent :meth:`schedule` invocations (prevents #1044).
        """
        with self._lock:
            super().start()

    def on_thread_start(self) -> None:
        """Start all scheduled emitters before the observer thread begins dispatching."""
        with self._lock:
            for emitter in self._emitters.copy():
                try:
                    emitter.start()
                except Exception:
                    self._remove_emitter(emitter)
                    raise

    def __enter__(self) -> Self:
        """Context manager entry. Starts the observer if not already running."""
        if not self.is_alive():
            self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        """Context manager exit. Stops the observer and waits for it to finish."""
        if self.is_alive():
            self.stop()
            self.join()

    def schedule(
        self,
        event_handler: FileSystemEventHandler,
        path: str | Path,
        *,
        recursive: bool = False,
        event_filter: list[type[FileSystemEvent]] | None = None,
        follow_symlink: bool = False,
    ) -> ObservedWatch:
        """Schedules watching a path and calls appropriate methods specified
        in the given event handler in response to file system events.

        :param event_handler:
            An event handler instance that has appropriate event handling
            methods which will be called by the observer in response to
            file system events.
        :type event_handler:
            :class:`watchdog.events.FileSystemEventHandler` or a subclass
        :param path:
            Directory or file path that will be monitored.
        :type path:
            ``str`` or :class:`pathlib.Path`
        :param recursive:
            ``True`` if events will be emitted for sub-directories
            traversed recursively; ``False`` otherwise.
        :type recursive:
            ``bool``
        :param event_filter:
            Collection of event types to emit, or None for no filtering (default).
        :type event_filter:
            Iterable[:class:`watchdog.events.FileSystemEvent`] | None
        :param follow_symlink:
            ``True`` if symlinks should be followed; ``False`` otherwise.
        :type follow_symlink:
            ``bool``
        :return:
            An :class:`ObservedWatch` object instance representing
            a watch.
        """
        with self._lock:
            watch = ObservedWatch(path, recursive=recursive, event_filter=event_filter, follow_symlink=follow_symlink)

            # If we don't have an emitter for this watch already, create it.
            if watch not in self._emitter_for_watch:
                emitter = self._emitter_class(
                    self.event_queue,
                    watch,
                    timeout=self.timeout,
                    event_filter=event_filter,
                    **self._emitter_kwargs,
                )
                emitter._handlers_provider = self._handlers_for_watch  # noqa: SLF001
                if self.is_alive():
                    emitter.start()
                self._add_emitter(emitter)
            self._add_handler_for_watch(event_handler, watch)
            self._watches.add(watch)
        return watch

    def add_handler_for_watch(self, event_handler: FileSystemEventHandler, watch: ObservedWatch) -> None:
        """Adds a handler for the given watch.

        :param event_handler:
            An event handler instance that has appropriate event handling
            methods which will be called by the observer in response to
            file system events.
        :type event_handler:
            :class:`watchdog.events.FileSystemEventHandler` or a subclass
        :param watch:
            The watch to add a handler for.
        :type watch:
            An instance of :class:`ObservedWatch` or a subclass of
            :class:`ObservedWatch`
        """
        with self._lock:
            self._add_handler_for_watch(event_handler, watch)

    def remove_handler_for_watch(self, event_handler: FileSystemEventHandler, watch: ObservedWatch) -> None:
        """Removes a handler for the given watch.

        :param event_handler:
            An event handler instance that has appropriate event handling
            methods which will be called by the observer in response to
            file system events.
        :type event_handler:
            :class:`watchdog.events.FileSystemEventHandler` or a subclass
        :param watch:
            The watch to remove a handler for.
        :type watch:
            An instance of :class:`ObservedWatch` or a subclass of
            :class:`ObservedWatch`
        """
        with self._lock:
            # Use .get() rather than indexing: _handlers is a defaultdict, so
            # indexing it here would resurrect a watch that unschedule() already
            # removed, leaving a permanent empty-set entry behind.
            self._handlers.get(watch, set()).remove(event_handler)

    def unschedule(self, watch: ObservedWatch) -> None:
        """Unschedules a watch.

        :param watch:
            The watch to unschedule.
        :type watch:
            An instance of :class:`ObservedWatch` or a subclass of
            :class:`ObservedWatch`
        """
        with self._lock:
            emitter = self._emitter_for_watch[watch]
            del self._handlers[watch]
            self._remove_emitter(emitter)
            self._watches.remove(watch)

    def update_handler_filter(
        self,
        event_handler: PathMatchingEventHandler,
        watch: ObservedWatch,
        *,
        queued_events: QueuedEventPolicy | str = "old_rules",
        **rules: Any,
    ) -> None:
        """Update the matching rules of a scheduled handler at runtime.

        Unlike ``unschedule()`` followed by ``schedule()``, this keeps the
        subscription and its underlying emitter alive, so no events are lost
        during the switch and other handlers sharing the same watch are not
        affected. The update is atomic: if the new rules are invalid, the
        exception propagates and the previous rules keep working unchanged.

        :param event_handler:
            The scheduled handler whose rules are updated. It must be a
            :class:`~watchdog.events.PathMatchingEventHandler` subclass
            (e.g. :class:`~watchdog.events.PatternMatchingEventHandler` or
            :class:`~watchdog.events.RegexMatchingEventHandler`) currently
            registered for ``watch``.
        :param watch:
            The watch the handler is scheduled for.
        :type watch:
            An instance of :class:`ObservedWatch` or a subclass of
            :class:`ObservedWatch`
        :param queued_events:
            Whether events already queued but not yet dispatched are matched
            against the old or the new rules. Accepts a
            :class:`~watchdog.events.QueuedEventPolicy` or the strings
            ``"old_rules"`` (default) and ``"new_rules"``.
        :param rules:
            The new matching rules, forwarded to the handler's update method
            (``patterns``/``ignore_patterns`` or ``regexes``/``ignore_regexes``).
        :raises KeyError:
            If the handler is not scheduled for ``watch``.
        :raises TypeError:
            If the handler does not support runtime rule updates.
        :raises ValueError:
            If the new rules are invalid; the previous rules remain active.
        """
        with self._lock:
            # Use .get() rather than indexing: _handlers is a defaultdict, so
            # indexing it here would resurrect a watch that unschedule() removed.
            handlers = self._handlers.get(watch, set())
            if event_handler not in handlers:
                error = f"{event_handler!r} is not scheduled for {watch!r}"
                raise KeyError(error)
            update = getattr(event_handler, "update_patterns", None) or getattr(event_handler, "update_regexes", None)
            if update is None:
                error = f"{type(event_handler).__name__} does not support runtime rule updates"
                raise TypeError(error)
            update(queued_events=queued_events, **rules)

    def unschedule_all(self) -> None:
        """Unschedules all watches and detaches all associated event handlers."""
        with self._lock:
            self._handlers.clear()
            self._clear_emitters()
            self._watches.clear()

    def on_thread_stop(self) -> None:
        self.unschedule_all()

    def dispatch_events(self, event_queue: EventQueue) -> None:
        entry = event_queue.get(block=True)
        if entry is EventDispatcher.stop_event:
            event_queue.task_done()
            return

        event, watch = entry

        if watch.is_file:
            target = watch.resolved_path
            src_path = (
                os.path.realpath(os.path.abspath(event.src_path))
                if watch.follow_symlink
                else os.path.abspath(event.src_path)
            )
            dest_path = None
            if isinstance(event, FileSystemMovedEvent) and event.dest_path:
                dest_path = (
                    os.path.realpath(os.path.abspath(event.dest_path))
                    if watch.follow_symlink
                    else os.path.abspath(event.dest_path)
                )
            if src_path != target and dest_path != target:
                event_queue.task_done()
                return

        # To allow unschedule/stop and safe removal of event handlers
        # within event handlers itself, check if the handler is still
        # registered before dispatching.
        # Use .get() rather than indexing: _handlers is a defaultdict, so
        # indexing it here would resurrect a watch that unschedule() removed
        # but whose events were already queued.
        # Handlers are dispatched without holding self._lock to avoid blocking
        # concurrent calls to schedule(), unschedule(), or stop().
        with self._lock:
            handlers = list(self._handlers.get(watch, ()))

        for handler in handlers:
            with self._lock:
                is_active = handler in self._handlers.get(watch, ())
            if is_active:
                event.watch_path = watch.path
                handler.dispatch(event)
        event_queue.task_done()
