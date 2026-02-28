"""
Client (merchant) behaviour profiler.

Produces ClientSnapshot objects per merchant per time window.
Each snapshot captures transaction volume, failure ratio, financial patterns,
geographic distribution, time-of-day usage, and burst/silence flags.

Burst detection:
  If txn_frequency_per_min > BURST_MULTIPLIER * baseline → burst
Silence detection:
  If txn_frequency_per_min < SILENCE_FRACTION * baseline AND baseline > threshold → silence

Baselines for burst/silence are checked against Redis-cached rolling averages.
If no baseline exists yet, burst/silence flags remain False.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Awaitable

from common.constants import (
    BURST_MULTIPLIER,
    MIN_BASELINE_TPS_FOR_SILENCE,
    PRIMARY_AGGREGATION_WINDOW,
    SILENCE_FRACTION,
)
from common.enums import ErrorType
from common.schemas import ClassifiedEvent, ClientSnapshot
from metrics.geo_tracker import build_geo_profile

logger = logging.getLogger(__name__)

ClientSnapshotHandler = Callable[[ClientSnapshot], Awaitable[None]]


# ---------------------------------------------------------------------------
# Per-merchant per-window accumulator (internal)
# ---------------------------------------------------------------------------

@dataclass
class _ClientAccumulator:
    merchant_id: str
    window_start: datetime
    window_seconds: int

    total: int = 0
    failures: int = 0
    amounts: list[float] = field(default_factory=list)
    country_codes: list[str] = field(default_factory=list)
    city_codes: list[str] = field(default_factory=list)
    hour_counts: dict[int, int] = field(default_factory=lambda: defaultdict(int))
    api_counts: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def ingest(self, event: ClassifiedEvent) -> None:
        self.total += 1
        self.api_counts[event.api_name] += 1
        self.hour_counts[event.timestamp.hour] += 1

        if event.error_type in (ErrorType.TECHNICAL, ErrorType.BUSINESS):
            self.failures += 1

        if event.transaction_amount:
            self.amounts.append(event.transaction_amount)
        if event.request_geo_country:
            self.country_codes.append(event.request_geo_country)
        if event.request_geo_city:
            self.city_codes.append(event.request_geo_city)

    def to_snapshot(
        self,
        baseline_tps: float | None = None,
        known_countries: set[str] | None = None,
    ) -> ClientSnapshot:
        total = self.total or 1
        freq = self.total / (self.window_seconds / 60.0)   # txn per minute
        failure_ratio = round(self.failures / total, 6)

        total_amount = sum(self.amounts)
        avg_amount = total_amount / len(self.amounts) if self.amounts else 0.0
        max_amount = max(self.amounts) if self.amounts else 0.0

        geo = build_geo_profile(
            merchant_id=self.merchant_id,
            country_codes=self.country_codes,
            city_codes=self.city_codes,
            known_countries=known_countries,
        )

        # Burst / silence detection
        burst = False
        silence = False
        if baseline_tps is not None:
            if freq > BURST_MULTIPLIER * baseline_tps:
                burst = True
            elif (
                baseline_tps > MIN_BASELINE_TPS_FOR_SILENCE
                and freq < SILENCE_FRACTION * baseline_tps
            ):
                silence = True

        return ClientSnapshot(
            merchant_id=self.merchant_id,
            window_start=self.window_start,
            window_seconds=self.window_seconds,
            txn_volume=self.total,
            txn_frequency_per_min=round(freq, 4),
            failure_ratio=failure_ratio,
            avg_txn_amount=round(avg_amount, 2),
            max_txn_amount=round(max_amount, 2),
            total_txn_amount=round(total_amount, 2),
            unique_countries=geo.unique_countries,
            unique_cities=geo.unique_cities,
            geo_entropy=geo.geo_entropy,
            hour_distribution=dict(self.hour_counts),
            burst_detected=burst,
            silence_detected=silence,
            api_breakdown=dict(self.api_counts),
        )


# ---------------------------------------------------------------------------
# ClientProfiler
# ---------------------------------------------------------------------------

class ClientProfiler:
    """
    Subscribes to ClassifiedEvent stream.
    Maintains per-merchant per-window accumulators.
    Flushes ClientSnapshots every window_seconds.
    """

    def __init__(self, window_seconds: int = PRIMARY_AGGREGATION_WINDOW) -> None:
        self._window_seconds = window_seconds
        self._handlers: list[ClientSnapshotHandler] = []
        # (merchant_id, window_start_iso) → _ClientAccumulator
        self._windows: dict[tuple[str, str], _ClientAccumulator] = {}
        self._flush_task: asyncio.Task | None = None

        # In-memory baseline TPS per merchant (seeded from Redis on first access)
        # merchant_id → rolling avg txn_frequency_per_min
        self._baseline_freq: dict[str, float] = {}

    def add_handler(self, handler: ClientSnapshotHandler) -> None:
        self._handlers.append(handler)

    async def start(self) -> None:
        self._flush_task = asyncio.create_task(self._flush_loop())
        logger.info("ClientProfiler started (window=%ds)", self._window_seconds)

    async def stop(self) -> None:
        if self._flush_task:
            self._flush_task.cancel()
        await self._flush_all(force=True)

    # ------------------------------------------------------------------
    # EventBus handler
    # ------------------------------------------------------------------

    async def handle(self, event: ClassifiedEvent) -> None:
        if not event.merchant_id:
            return

        window_start = self._floor_timestamp(event.timestamp)
        key = (event.merchant_id, window_start.isoformat())

        if key not in self._windows:
            self._windows[key] = _ClientAccumulator(
                merchant_id=event.merchant_id,
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
                logger.exception("ClientProfiler flush_loop error")

    async def _flush_all(self, force: bool = False) -> None:
        now = datetime.now(timezone.utc)
        to_flush: list[tuple[str, str]] = []

        for key, acc in self._windows.items():
            window_end = acc.window_start.timestamp() + acc.window_seconds
            if force or now.timestamp() >= window_end:
                to_flush.append(key)

        for key in to_flush:
            acc = self._windows.pop(key)
            if acc.total == 0:
                continue

            baseline_tps = self._baseline_freq.get(acc.merchant_id)
            snapshot = acc.to_snapshot(baseline_tps=baseline_tps)

            # Update in-memory baseline with EWMA (α=0.1)
            freq = snapshot.txn_frequency_per_min
            if acc.merchant_id in self._baseline_freq:
                alpha = 0.1
                self._baseline_freq[acc.merchant_id] = (
                    alpha * freq + (1 - alpha) * self._baseline_freq[acc.merchant_id]
                )
            else:
                self._baseline_freq[acc.merchant_id] = freq

            await self._publish(snapshot)

        if to_flush:
            logger.debug("ClientProfiler flushed %d merchant windows", len(to_flush))

    async def _publish(self, snapshot: ClientSnapshot) -> None:
        for handler in self._handlers:
            try:
                await handler(snapshot)
            except Exception:
                logger.exception(
                    "ClientProfiler handler %s raised for merchant %s",
                    handler, snapshot.merchant_id,
                )

    def _floor_timestamp(self, ts: datetime) -> datetime:
        epoch = ts.timestamp()
        floored = (epoch // self._window_seconds) * self._window_seconds
        return datetime.fromtimestamp(floored, tz=timezone.utc)

    def active_merchant_count(self) -> int:
        return len({k[0] for k in self._windows})


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------
client_profiler = ClientProfiler()
