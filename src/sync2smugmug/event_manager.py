import asyncio
import functools
import logging
from collections import defaultdict
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from typing import Any

from sync2smugmug import events

logger = logging.getLogger(__name__)

# Alias for the event listener callable
EventHandler = Callable[[Any, bool], Coroutine]


@dataclass
class EventsTracker:
    """
    Handles async events triggered during sync.
    All events will be executed asynchronously (allowing more than one handler to register for an event)
    """

    event_handlers: dict[str, set[EventHandler]] = field(default_factory=lambda: defaultdict(set))
    tasks = []

    # Keep track of event types fired (for summary print-out)
    event_count_by_type: dict = field(default_factory=lambda: defaultdict(int))
    total_submitted: int = 0
    total_processed: int = 0


the_events_tracker: EventsTracker = EventsTracker()
_concurrency_limiter = asyncio.Semaphore(10)


async def fire_event(event: str, event_data: events.EventData, dry_run: bool):
    """
    Log an event for async processing.
    """
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"!---- Event fired: {event} - {event_data} ----!")

    # Call all listeners asynchronously (keep track of tasks, so we can wait on them later)
    async_task = asyncio.create_task(handle_event(event=event, event_data=event_data, dry_run=dry_run))
    the_events_tracker.tasks.append(async_task)

    # Update bookkeeping
    the_events_tracker.total_submitted += 1
    the_events_tracker.event_count_by_type[event] += 1


async def handle_event(event: str, event_data: events.EventData, dry_run: bool):
    """
    Called asynchronously (via asyncio.create_task) to handle an event. This will call all call back each of the
    registered handlers with the event data.

    Concurrency (how many events can be processed at the same time) is limited here so there is no risk for a deadlock
    in lower level actions.
    """
    async with _concurrency_limiter:
        handlers = the_events_tracker.event_handlers.get(event) or []

        # Allow each of the event_handlers to process the event
        for handler in handlers:
            await handler(event_data, dry_run)

        the_events_tracker.total_processed += 1


def subscribe(*event_tags):
    """
    Decorator to subscribe an event handler to one or more events
    """

    def wrapper(event_handler: EventHandler):
        @functools.wraps(event_handler)
        async def wrapped(event_data, dry_run: bool) -> bool:
            # Delegate to the event_handler to process the event
            return await event_handler(event_data, dry_run)

        # Register each of the handlers under this event tag
        for event_tag in event_tags:
            the_events_tracker.event_handlers[event_tag].add(wrapped)

        return wrapped

    return wrapper


async def join():
    """
    Wait until all events submitted are processed.

    Since events can (and often are) be fired from within other event handlers, we will continue waiting until the
    queue is finally empty. This is done by repeatedly calling 'gather' with a slice from the queue - until the queue
    is exhausted.
    """
    #
    while len(the_events_tracker.tasks) > 0:
        slice_size = min(len(the_events_tracker.tasks), 100)

        # Take a piece of the events
        a_slice = the_events_tracker.tasks[:slice_size]
        the_events_tracker.tasks = the_events_tracker.tasks[slice_size:]

        await asyncio.gather(*a_slice)
