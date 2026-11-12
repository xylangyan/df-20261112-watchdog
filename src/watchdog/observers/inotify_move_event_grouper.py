""":module: watchdog.observers.inotify_buffer
:synopsis: queue-like class for ``Inotify`` to group move events.
:author: thomas.amland@gmail.com (Thomas Amland)
:author: Mickaël Schoentgen <contact@tiger-222.fr>
:author: Joachim Coenen <joachimcoenen@icloud.com>
:platforms: linux 2.6.13+
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, NamedTuple, cast

from watchdog.utils.delayed_queue import DelayedQueue

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TypeAlias

    from watchdog.observers.inotify_c import InotifyEvent
    from watchdog.utils import Seconds

logger = logging.getLogger(__name__)


class PathedInotifyEvent(NamedTuple):
    """An InotifyEvent and its full source path"""

    ev: InotifyEvent
    path: bytes


GroupedInotifyEvent: TypeAlias = PathedInotifyEvent | tuple[PathedInotifyEvent, PathedInotifyEvent]
MovePair: TypeAlias = tuple[PathedInotifyEvent, PathedInotifyEvent]


class InotifyMoveEventGrouper:
    """A queue-like class for `Inotify` that holds IN_MOVED_FROM and
    IN_MOVED_TO events for `delay`. During this time, both halves of a move
    are paired by cookie, regardless of the order in which they arrived.
    """

    delay: Seconds = 0.5

    def __init__(
        self,
        on_move_paired: Callable[[MovePair], None] | None = None,
        on_move_unpaired: Callable[[PathedInotifyEvent], None] | None = None,
    ) -> None:
        self._queue: DelayedQueue[GroupedInotifyEvent] = DelayedQueue(self.delay)
        self._on_move_paired = on_move_paired
        self._on_move_unpaired = on_move_unpaired

    def read_event(self) -> GroupedInotifyEvent | None:
        """Returns a single event or a tuple of from/to events in case of a
        paired move event. If this buffer has been closed, raise the Closed
        exception.
        """
        event = self._queue.get()
        # A lone half was returned once its partner can no longer arrive.
        if isinstance(event, PathedInotifyEvent) and event.ev.is_move and self._on_move_unpaired is not None:
            self._on_move_unpaired(event)
        return event

    def put_event(self, event: PathedInotifyEvent) -> None:
        """Add an event to the `queue`. When adding one half of a move, remove
        a previously added matching other half and add them back to the queue
        as a (from, to) tuple. Unmatched halves are delayed so a late partner
        can still be paired.
        """
        logger.debug("in-event %s", event)
        # Delay unmatched move events so a half arriving out of order can
        # still be paired with its partner.
        should_delay = event.ev.is_move

        grouped_event = self._group_move_event(event) if event.ev.is_move else event

        self._queue.put(grouped_event, delay=should_delay)

    def _group_move_event(self, event: PathedInotifyEvent) -> GroupedInotifyEvent:
        """Group any matching move events by checking if the matching other
        half is in the delay queue already and removing it. The returned tuple
        is always ordered (moved_from, moved_to).
        """
        cookie = event.ev.cookie

        def matching_other_half(queued: GroupedInotifyEvent) -> bool:
            return (
                isinstance(queued, PathedInotifyEvent)
                and queued.ev.is_move
                and queued.ev.is_moved_from != event.ev.is_moved_from
                and queued.ev.cookie == cookie
            )

        # Check if the other half is in the delay queue already
        other_event = cast("PathedInotifyEvent", self._queue.remove(matching_other_half))
        if other_event is None:
            logger.debug("could not find matching move event")
            return event

        from_event, to_event = (event, other_event) if event.ev.is_moved_from else (other_event, event)
        pair = (from_event, to_event)
        if self._on_move_paired is not None:
            self._on_move_paired(pair)
        return pair

    def discard_pending_events(self) -> None:
        """Discard events whose ordering is no longer trustworthy."""
        self._queue.clear()

    def close(self) -> None:
        """closes the queue"""
        self._queue.close()
