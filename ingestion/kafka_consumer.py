"""
Real Kafka consumer using aiokafka.

Activated when KAFKA_USE_MOCK=false in .env.
Implements the same interface as MockKafkaConsumer so the rest of the
system is completely agnostic to which backend is running.

Requires: pip install aiokafka
"""

import json
import logging

from common.schemas import LogEvent
from config.settings import get_settings
from ingestion.base import EventBus
from ingestion.normalizer import normalize_kafka_payload

logger = logging.getLogger(__name__)


class KafkaConsumer:
    """
    Consumes messages from configured Kafka topics and publishes
    normalised LogEvents onto the EventBus.
    """

    def __init__(self, bus: EventBus) -> None:
        self._bus = bus
        self._settings = get_settings().kafka
        self._consumer = None
        self._running = False

    async def start(self) -> None:
        try:
            # Import deferred — aiokafka is optional
            from aiokafka import AIOKafkaConsumer  # type: ignore[import]
        except ImportError:
            logger.error(
                "aiokafka not installed. Run: pip install aiokafka  "
                "or set KAFKA_USE_MOCK=true"
            )
            raise

        self._consumer = AIOKafkaConsumer(
            *self._settings.topics,
            bootstrap_servers=self._settings.bootstrap_servers,
            group_id=self._settings.group_id,
            auto_offset_reset=self._settings.auto_offset_reset,
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        await self._consumer.start()
        self._running = True

        import asyncio
        asyncio.create_task(self._consume_loop())
        logger.info(
            "KafkaConsumer started → %s topics=%s",
            self._settings.bootstrap_servers,
            self._settings.topics,
        )

    async def stop(self) -> None:
        self._running = False
        if self._consumer:
            await self._consumer.stop()

    async def _consume_loop(self) -> None:
        async for msg in self._consumer:
            if not self._running:
                break
            try:
                payload: dict = msg.value
                event: LogEvent | None = normalize_kafka_payload(payload)
                if event is not None:
                    await self._bus.publish(event)
            except Exception:
                logger.exception(
                    "KafkaConsumer: failed processing topic=%s partition=%d offset=%d",
                    msg.topic, msg.partition, msg.offset,
                )
