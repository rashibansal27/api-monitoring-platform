"""
Metrics aggregator.

Subscribes to ClassifiedEvent stream, buckets events into 1-minute windows,
and flushes completed windows as MetricPoint objects.

Window lifecycle:
  - Events arrive continuously from the classification pipeline
  - Each event is bucketed to floor(timestamp, window_seconds)
  - A window is "complete" when its window_start is older than 1 window ago
  - The periodic flush loop runs every PRIMARY_AGGREGATION_WINDOW seconds
  - Completed windows → MetricPoint → MetricsWriter

Concurrency:
  All operations run on the asyncio event loop — no threading needed.
  The _windows dict is accessed only from async handlers and the flush loop,
  both on the same loop.

Scope:
  This aggregator produces PLATFORM-WIDE (api_name only, no merchant_id) metrics.
  Per-merchant aggregation is handled by ClientProfiler.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Awaitable

from common.constants import PRIMARY_AGGREGATION_WINDOW
from common.enums import ErrorType
from common.schemas import ClassifiedEvent, MetricPoint
from metrics.latency import LatencyBuffer, compute_percentiles

logger = logging.getLogger(__name__)

# Type alias for downstream MetricPoint handlers
MetricPointHandler = Callable[[MetricPoint], Awaitable[None]]


# ---------------------------------------------------------------------------
# Per-window accumulator (internal)
# ---------------------------------------------------------------------------

@dataclass
class _WindowAccumulator:
    """Mutable state for one (api_name, window_start) bucket."""
    api_name: str
    window_start: datetime
    window_seconds: int

    total: int = 0
    successes: int = 0
    tech_errors: int = 0
    biz_errors: int = 0
    amounts: list[float] = field(default_factory=list)
    latency_buf: LatencyBuffer = field(init=False)

    def __post_init__(self) -> None:
        self.latency_buf = LatencyBuffer(
            api_name=self.api_name,
            window_start=self.window_start,
        )

    def ingest(self, event: ClassifiedEvent) -> None:
        self.total += 1
        self.latency_buf.add(event.response_time_ms)

        if event.error_type == ErrorType.TECHNICAL:
            self.tech_errors += 1
        elif event.error_type == ErrorType.BUSINESS:
            self.biz_errors += 1
        else:
            self.successes += 1

        if event.transaction_amount:
            self.amounts.append(event.transaction_amount)

    def to_metric_point(self) -> MetricPoint:
        perc = self.latency_buf.flush()
        total = self.total or 1          # Avoid /0

        total_amount = sum(self.amounts)
        avg_amount = total_amount / len(self.amounts) if self.amounts else 0.0
        max_amount = max(self.amounts) if self.amounts else 0.0

        return MetricPoint(
            window_start=self.window_start,
            window_seconds=self.window_seconds,
            api_name=self.api_name,
            merchant_id=None,
            total_requests=self.total,
            success_count=self.successes,
            technical_error_count=self.tech_errors,
            business_error_count=self.biz_errors,
            tps=round(self.total / self.window_seconds, 4),
            success_rate=round(self.successes / total, 6),
            technical_error_rate=round(self.tech_errors / total, 6),
            business_error_rate=round(self.biz_errors / total, 6),
            avg_latency_ms=round(perc.mean_ms, 2),
            p50_latency_ms=round(perc.p50_ms, 2),
            p95_latency_ms=round(perc.p95_ms, 2),
            p99_latency_ms=round(perc.p99_ms, 2),
            total_txn_amount=round(total_amount, 2),
            avg_txn_amount=round(avg_amount, 2),
            max_txn_amount=round(max_amount, 2),
        )


# ---------------------------------------------------------------------------
# MetricsAggregator
# ---------------------------------------------------------------------------

class MetricsAggregator:
    """
    Stateful aggregator that converts a stream of ClassifiedEvents
    into a stream of MetricPoints via fixed-size time windows.
    """

    def __init__(self, window_seconds: int = PRIMARY_AGGREGATION_WINDOW) -> None:
        self._window_seconds = window_seconds
        self._handlers: list[MetricPointHandler] = []
        # (api_name, window_start_iso) → _WindowAccumulator
        self._windows: dict[tuple[str, str], _WindowAccumulator] = {}
        self._flush_task: asyncio.Task | None = None

    def add_handler(self, handler: MetricPointHandler) -> None:
        """Register a downstream handler to receive flushed MetricPoints."""
        self._handlers.append(handler)

    async def start(self) -> None:
        self._flush_task = asyncio.create_task(self._flush_loop())
        logger.info(
            "MetricsAggregator started (window=%ds)", self._window_seconds
        )

    async def stop(self) -> None:
        if self._flush_task:
            self._flush_task.cancel()
        # Flush remaining open windows before shutdown
        await self._flush_all(force=True)

    # ------------------------------------------------------------------
    # EventBus handler — called for every ClassifiedEvent
    # ------------------------------------------------------------------

    async def handle(self, event: ClassifiedEvent) -> None:
        window_start = self._floor_timestamp(event.timestamp)
        key = (event.api_name, window_start.isoformat())

        if key not in self._windows:
            self._windows[key] = _WindowAccumulator(
                api_name=event.api_name,
                window_start=window_start,
                window_seconds=self._window_seconds,
            )

        self._windows[key].ingest(event)

    # ------------------------------------------------------------------
    # Flush logic
    # ------------------------------------------------------------------

    async def _flush_loop(self) -> None:
        while True:
            await asyncio.sleep(self._window_seconds)
            try:
                await self._flush_all(force=False)
            except Exception:
                logger.exception("MetricsAggregator flush_loop error")

    async def _flush_all(self, force: bool = False) -> None:
        """
        Flush all windows that have closed.
        A window is closed when window_start + window_seconds < now.
        force=True flushes all windows (used during shutdown).
        """
        now = datetime.now(timezone.utc)
        to_flush: list[tuple[str, str]] = []

        for key, acc in self._windows.items():
            # Window end = window_start + window_seconds
            window_end_ts = acc.window_start.timestamp() + acc.window_seconds
            if force or now.timestamp() >= window_end_ts:
                to_flush.append(key)

        for key in to_flush:
            acc = self._windows.pop(key)
            if acc.total == 0:
                continue
            metric = acc.to_metric_point()
            await self._publish(metric)

        if to_flush:
            logger.debug("Flushed %d metric windows", len(to_flush))

    async def _publish(self, metric: MetricPoint) -> None:
        for handler in self._handlers:
            try:
                await handler(metric)
            except Exception:
                logger.exception(
                    "MetricsAggregator handler %s raised for %s @ %s",
                    handler, metric.api_name, metric.window_start,
                )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _floor_timestamp(self, ts: datetime) -> datetime:
        """Floor timestamp to the nearest window boundary."""
        epoch = ts.timestamp()
        floored = (epoch // self._window_seconds) * self._window_seconds
        return datetime.fromtimestamp(floored, tz=timezone.utc)

    def open_window_count(self) -> int:
        """Number of currently open (unflushed) windows — useful for health checks."""
        return len(self._windows)

    def get_open_windows_summary(self) -> list[dict]:
        """Returns a list of open window summaries for debugging."""
        return [
            {
                "api_name": acc.api_name,
                "window_start": acc.window_start.isoformat(),
                "total": acc.total,
                "tech_errors": acc.tech_errors,
                "biz_errors": acc.biz_errors,
            }
            for acc in self._windows.values()
        ]


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------
aggregator = MetricsAggregator()
