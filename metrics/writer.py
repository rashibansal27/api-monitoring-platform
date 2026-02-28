"""
MetricsWriter — async dual-write to TimescaleDB and Redis.

Receives MetricPoint and ClientSnapshot objects from the aggregators
and persists them:
  - TimescaleDB → durable long-term storage (7+ days retention)
  - Redis        → real-time sliding window counters for fast API reads

Write strategy:
  - TimescaleDB writes are batched (BATCH_SIZE or FLUSH_INTERVAL_S, whichever first)
  - Redis writes are immediate (low-cost ZADD / LPUSH operations)
  - On TimescaleDB failure: log error + continue (metrics not lost from Redis for 7 days)
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timezone

from common.enums import ErrorType
from common.schemas import ClientSnapshot, MetricPoint
from storage.redis_client import (
    get_redis,
    push_latency,
    record_error,
    record_request,
    biz_err_key,
    tech_err_key,
)

logger = logging.getLogger(__name__)

_BATCH_SIZE = 50            # Flush to TimescaleDB after this many MetricPoints
_FLUSH_INTERVAL_S = 10      # Or after this many seconds, whichever is sooner


class MetricsWriter:
    """
    Dual-write handler for MetricPoints and ClientSnapshots.

    Usage:
        writer = MetricsWriter()
        await writer.start()
        aggregator.add_handler(writer.handle_metric_point)
        client_profiler.add_handler(writer.handle_client_snapshot)
    """

    def __init__(self) -> None:
        self._metric_batch: list[MetricPoint] = []
        self._snapshot_batch: list[ClientSnapshot] = []
        self._flush_task: asyncio.Task | None = None
        self._redis = get_redis()

    async def start(self) -> None:
        self._flush_task = asyncio.create_task(self._flush_loop())
        logger.info("MetricsWriter started")

    async def stop(self) -> None:
        if self._flush_task:
            self._flush_task.cancel()
        await self._flush_batches()

    # ------------------------------------------------------------------
    # Incoming handlers
    # ------------------------------------------------------------------

    async def handle_metric_point(self, metric: MetricPoint) -> None:
        """Called by MetricsAggregator for each flushed MetricPoint."""
        # Immediate Redis write for real-time API reads
        await self._write_metric_to_redis(metric)

        # Batch for TimescaleDB
        self._metric_batch.append(metric)
        if len(self._metric_batch) >= _BATCH_SIZE:
            await self._flush_metric_batch()

    async def handle_client_snapshot(self, snapshot: ClientSnapshot) -> None:
        """Called by ClientProfiler for each flushed ClientSnapshot."""
        self._snapshot_batch.append(snapshot)
        if len(self._snapshot_batch) >= _BATCH_SIZE:
            await self._flush_snapshot_batch()

    # ------------------------------------------------------------------
    # Redis writes (immediate, lightweight)
    # ------------------------------------------------------------------

    async def _write_metric_to_redis(self, metric: MetricPoint) -> None:
        try:
            ts = metric.window_start.replace(tzinfo=timezone.utc).timestamp()
            window = metric.window_seconds
            api = metric.api_name

            r = self._redis
            # Record request count in sliding window ZSET
            # Use metric_id as the member (unique per window)
            await record_request(r, api, ts, metric.metric_id, window)

            # Record technical errors
            for i in range(metric.technical_error_count):
                await record_error(
                    r, tech_err_key(api, window), ts,
                    f"{metric.metric_id}:t{i}", window,
                )

            # Record business errors
            for i in range(metric.business_error_count):
                await record_error(
                    r, biz_err_key(api, window), ts,
                    f"{metric.metric_id}:b{i}", window,
                )

            # Push p95 latency to rolling buffer (representative of window)
            if metric.p95_latency_ms > 0:
                await push_latency(r, api, metric.p95_latency_ms)

        except Exception:
            logger.exception("Redis write failed for MetricPoint %s", metric.metric_id)

    # ------------------------------------------------------------------
    # TimescaleDB batch writes
    # ------------------------------------------------------------------

    async def _flush_loop(self) -> None:
        while True:
            await asyncio.sleep(_FLUSH_INTERVAL_S)
            await self._flush_batches()

    async def _flush_batches(self) -> None:
        await self._flush_metric_batch()
        await self._flush_snapshot_batch()

    async def _flush_metric_batch(self) -> None:
        if not self._metric_batch:
            return

        batch = self._metric_batch[:]
        self._metric_batch.clear()

        try:
            await self._write_metrics_to_db(batch)
            logger.debug("Wrote %d MetricPoints to TimescaleDB", len(batch))
        except Exception:
            logger.exception(
                "TimescaleDB write failed for %d MetricPoints — data preserved in Redis",
                len(batch),
            )

    async def _flush_snapshot_batch(self) -> None:
        if not self._snapshot_batch:
            return

        batch = self._snapshot_batch[:]
        self._snapshot_batch.clear()

        try:
            await self._write_snapshots_to_db(batch)
            logger.debug("Wrote %d ClientSnapshots to TimescaleDB", len(batch))
        except Exception:
            logger.exception(
                "TimescaleDB write failed for %d ClientSnapshots",
                len(batch),
            )

    async def _write_metrics_to_db(self, batch: list[MetricPoint]) -> None:
        from storage.database import get_session
        from storage.models import MetricPointORM

        async with get_session() as session:
            orm_objects = [
                MetricPointORM(
                    id=m.metric_id,
                    window_start=m.window_start,
                    window_seconds=m.window_seconds,
                    api_name=m.api_name,
                    merchant_id=m.merchant_id,
                    total_requests=m.total_requests,
                    success_count=m.success_count,
                    technical_error_count=m.technical_error_count,
                    business_error_count=m.business_error_count,
                    tps=m.tps,
                    success_rate=m.success_rate,
                    technical_error_rate=m.technical_error_rate,
                    business_error_rate=m.business_error_rate,
                    avg_latency_ms=m.avg_latency_ms,
                    p50_latency_ms=m.p50_latency_ms,
                    p95_latency_ms=m.p95_latency_ms,
                    p99_latency_ms=m.p99_latency_ms,
                    total_txn_amount=m.total_txn_amount,
                    avg_txn_amount=m.avg_txn_amount,
                    max_txn_amount=m.max_txn_amount,
                )
                for m in batch
            ]
            session.add_all(orm_objects)

    async def _write_snapshots_to_db(self, batch: list[ClientSnapshot]) -> None:
        from storage.database import get_session
        from storage.models import ClientSnapshotORM

        async with get_session() as session:
            orm_objects = [
                ClientSnapshotORM(
                    id=s.snapshot_id,
                    merchant_id=s.merchant_id,
                    window_start=s.window_start,
                    window_seconds=s.window_seconds,
                    txn_volume=s.txn_volume,
                    txn_frequency_per_min=s.txn_frequency_per_min,
                    failure_ratio=s.failure_ratio,
                    avg_txn_amount=s.avg_txn_amount,
                    max_txn_amount=s.max_txn_amount,
                    total_txn_amount=s.total_txn_amount,
                    unique_countries=s.unique_countries,
                    unique_cities=s.unique_cities,
                    geo_entropy=s.geo_entropy,
                    hour_distribution={str(h): c for h, c in s.hour_distribution.items()},
                    burst_detected=s.burst_detected,
                    silence_detected=s.silence_detected,
                    api_breakdown=s.api_breakdown,
                )
                for s in batch
            ]
            session.add_all(orm_objects)


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------
writer = MetricsWriter()
