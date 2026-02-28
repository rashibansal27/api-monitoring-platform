"""
Abstract base classes for the ingestion layer.

BaseIngestor   — contract every data source must implement
EventBus       — abstract publish interface (mock today, Kafka tomorrow)
"""

import asyncio
import logging
from abc import ABC, abstractmethod

from common.schemas import LogEvent

logger = logging.getLogger(__name__)


class BaseIngestor(ABC):
    """
    Every ingestor fetches raw data from one source, converts it to
    a list of LogEvents, and publishes them onto the EventBus.
    """

    def __init__(self, bus: "EventBus") -> None:
        self._bus = bus

    @abstractmethod
    async def fetch(self) -> list[dict]:
        """
        Pull raw records from the source.
        Returns a list of source-native dicts (ES hits, Prometheus samples, etc.)
        """

    @abstractmethod
    async def normalize(self, raw_records: list[dict]) -> list[LogEvent]:
        """
        Convert source-native records into unified LogEvent objects.
        Must never raise — log and skip malformed records.
        """

    async def run(self) -> int:
        """
        Orchestrates fetch → normalize → publish.
        Returns the number of events successfully published.
        """
        try:
            raw = await self.fetch()
        except Exception:
            logger.exception("%s.fetch() failed", self.__class__.__name__)
            return 0

        events = await self.normalize(raw)
        published = 0
        for event in events:
            try:
                await self._bus.publish(event)
                published += 1
            except Exception:
                logger.exception("Failed to publish event %s", event.event_id)

        logger.info(
            "%s: fetched=%d normalized=%d published=%d",
            self.__class__.__name__,
            len(raw),
            len(events),
            published,
        )
        return published


# ---------------------------------------------------------------------------
# EventBus
# ---------------------------------------------------------------------------

class EventBus(ABC):
    """
    Abstract publish/subscribe interface.
    Downstream consumers (classification, metrics) subscribe to receive events.
    """

    @abstractmethod
    async def publish(self, event: LogEvent) -> None:
        """Publish a single LogEvent to all registered subscribers."""

    @abstractmethod
    async def subscribe(self, handler) -> None:
        """
        Register an async handler: async def handler(event: LogEvent) -> None
        Called for every published event.
        """


class InMemoryEventBus(EventBus):
    """
    asyncio.Queue-backed event bus for local development and testing.
    Replaces Kafka with zero external dependencies.

    Usage:
        bus = InMemoryEventBus()
        await bus.subscribe(my_handler)
        await bus.publish(event)          # my_handler(event) called immediately
    """

    def __init__(self) -> None:
        self._handlers: list = []
        self._queue: asyncio.Queue[LogEvent] = asyncio.Queue()
        self._running = False

    async def publish(self, event: LogEvent) -> None:
        await self._queue.put(event)

    async def subscribe(self, handler) -> None:
        self._handlers.append(handler)

    async def start(self) -> None:
        """
        Start the dispatch loop.
        Call once during application startup.
        """
        self._running = True
        asyncio.create_task(self._dispatch_loop())
        logger.info("InMemoryEventBus started")

    async def stop(self) -> None:
        self._running = False

    async def _dispatch_loop(self) -> None:
        while self._running:
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            for handler in self._handlers:
                try:
                    await handler(event)
                except Exception:
                    logger.exception("Handler %s raised for event %s", handler, event.event_id)
            self._queue.task_done()
