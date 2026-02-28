"""
TimescaleDB query helpers.

All read-path queries used by the API routes live here.
Keeps SQL concerns isolated from route handlers.

Strategy:
  - MetricPoints are pre-aggregated at 1-min granularity by the metrics engine.
  - For coarser granularities (5-min, 1-hour) we use time_bucket (TimescaleDB)
    with a fallback to Python-level re-aggregation when time_bucket is unavailable.
  - All functions are async and use the shared SQLAlchemy engine.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import desc, func, select, text

from storage.database import get_session
from storage.models import AlertORM, ClientSnapshotORM, MetricPointORM

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Metric time-series queries
# ---------------------------------------------------------------------------

async def get_metric_series(
    api_name: str,
    metric_attr: str,           # ORM column name: "tps", "technical_error_rate", …
    from_dt: datetime,
    to_dt: datetime,
    limit: int = 1440,          # max 1440 → 24h at 1-min resolution
) -> list[tuple[datetime, float]]:
    """
    Return (window_start, value) pairs for a single metric over a time range.
    Platform-wide (merchant_id IS NULL) rows only.
    """
    col = getattr(MetricPointORM, metric_attr, None)
    if col is None:
        logger.error("get_metric_series: unknown metric_attr=%s", metric_attr)
        return []

    try:
        async with get_session() as session:
            result = await session.execute(
                select(MetricPointORM.window_start, col)
                .where(
                    MetricPointORM.api_name == api_name,
                    MetricPointORM.merchant_id.is_(None),
                    MetricPointORM.window_start >= from_dt,
                    MetricPointORM.window_start <= to_dt,
                )
                .order_by(MetricPointORM.window_start.asc())
                .limit(limit)
            )
            return [(row[0], float(row[1] or 0.0)) for row in result.all()]
    except Exception:
        logger.exception("get_metric_series failed api=%s metric=%s", api_name, metric_attr)
        return []


async def get_latest_metric(
    api_name: str,
    merchant_id: Optional[str] = None,
) -> Optional[MetricPointORM]:
    """Return the most recent MetricPoint for an API (optionally scoped to a merchant)."""
    try:
        async with get_session() as session:
            q = (
                select(MetricPointORM)
                .where(MetricPointORM.api_name == api_name)
                .order_by(desc(MetricPointORM.window_start))
                .limit(1)
            )
            if merchant_id is not None:
                q = q.where(MetricPointORM.merchant_id == merchant_id)
            else:
                q = q.where(MetricPointORM.merchant_id.is_(None))

            result = await session.execute(q)
            return result.scalar_one_or_none()
    except Exception:
        logger.exception("get_latest_metric failed api=%s", api_name)
        return None


async def get_all_api_names(since_hours: int = 1) -> list[str]:
    """Return distinct api_name values seen in the last N hours."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)
    try:
        async with get_session() as session:
            result = await session.execute(
                select(MetricPointORM.api_name)
                .where(MetricPointORM.window_start >= cutoff)
                .distinct()
            )
            return [row[0] for row in result.all()]
    except Exception:
        logger.exception("get_all_api_names failed")
        return []


# ---------------------------------------------------------------------------
# Aggregated summaries
# ---------------------------------------------------------------------------

async def get_windowed_averages(
    api_name: str,
    metric_attr: str,
    hours: int = 24,
) -> dict[str, float]:
    """
    Return mean, min, max, latest for a metric over the last N hours.
    Used by the health endpoint and baseline display.
    """
    from_dt = datetime.now(timezone.utc) - timedelta(hours=hours)
    col = getattr(MetricPointORM, metric_attr, None)
    if col is None:
        return {"mean": 0.0, "min": 0.0, "max": 0.0, "latest": 0.0}

    try:
        async with get_session() as session:
            result = await session.execute(
                select(
                    func.avg(col).label("mean"),
                    func.min(col).label("min"),
                    func.max(col).label("max"),
                )
                .where(
                    MetricPointORM.api_name == api_name,
                    MetricPointORM.merchant_id.is_(None),
                    MetricPointORM.window_start >= from_dt,
                )
            )
            row = result.one_or_none()
            if not row or row.mean is None:
                return {"mean": 0.0, "min": 0.0, "max": 0.0, "latest": 0.0}

            latest_row = await get_latest_metric(api_name)
            latest_val = float(getattr(latest_row, metric_attr, 0.0) or 0.0) if latest_row else 0.0

            return {
                "mean":   round(float(row.mean or 0.0), 6),
                "min":    round(float(row.min or 0.0), 6),
                "max":    round(float(row.max or 0.0), 6),
                "latest": round(latest_val, 6),
            }
    except Exception:
        logger.exception("get_windowed_averages failed api=%s metric=%s", api_name, metric_attr)
        return {"mean": 0.0, "min": 0.0, "max": 0.0, "latest": 0.0}


# ---------------------------------------------------------------------------
# Error breakdown
# ---------------------------------------------------------------------------

async def get_error_rate_series(
    api_name: str,
    error_type: str,            # "technical" | "business"
    from_dt: datetime,
    to_dt: datetime,
    limit: int = 1440,
) -> list[tuple[datetime, float]]:
    """Convenience wrapper for error-rate time series."""
    attr = (
        "technical_error_rate" if error_type == "technical"
        else "business_error_rate"
    )
    return await get_metric_series(api_name, attr, from_dt, to_dt, limit)


async def get_top_failing_merchants(
    limit: int = 10,
    since_hours: int = 1,
) -> list[dict]:
    """
    Return top merchants ranked by failure_ratio descending.
    Uses the most recent ClientSnapshot per merchant.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)
    try:
        async with get_session() as session:
            # Latest snapshot per merchant (subquery)
            latest_subq = (
                select(
                    ClientSnapshotORM.merchant_id,
                    func.max(ClientSnapshotORM.window_start).label("latest_window"),
                )
                .where(ClientSnapshotORM.window_start >= cutoff)
                .group_by(ClientSnapshotORM.merchant_id)
                .subquery()
            )

            result = await session.execute(
                select(ClientSnapshotORM)
                .join(
                    latest_subq,
                    (ClientSnapshotORM.merchant_id == latest_subq.c.merchant_id)
                    & (ClientSnapshotORM.window_start == latest_subq.c.latest_window),
                )
                .order_by(desc(ClientSnapshotORM.failure_ratio))
                .limit(limit)
            )
            rows = result.scalars().all()
            return [
                {
                    "merchant_id":      r.merchant_id,
                    "failure_ratio":    round(r.failure_ratio, 4),
                    "txn_volume":       r.txn_volume,
                    "burst_detected":   r.burst_detected,
                    "silence_detected": r.silence_detected,
                    "avg_txn_amount":   round(r.avg_txn_amount, 2),
                    "window_start":     r.window_start.isoformat(),
                }
                for r in rows
            ]
    except Exception:
        logger.exception("get_top_failing_merchants failed")
        return []


async def get_latest_client_snapshot(
    merchant_id: str,
) -> Optional[ClientSnapshotORM]:
    try:
        async with get_session() as session:
            result = await session.execute(
                select(ClientSnapshotORM)
                .where(ClientSnapshotORM.merchant_id == merchant_id)
                .order_by(desc(ClientSnapshotORM.window_start))
                .limit(1)
            )
            return result.scalar_one_or_none()
    except Exception:
        logger.exception("get_latest_client_snapshot failed merchant=%s", merchant_id)
        return None


async def get_client_metric_series(
    merchant_id: str,
    metric_attr: str,
    from_dt: datetime,
    to_dt: datetime,
    limit: int = 1440,
) -> list[tuple[datetime, float]]:
    """Time series for a single client-level metric."""
    col = getattr(ClientSnapshotORM, metric_attr, None)
    if col is None:
        return []
    try:
        async with get_session() as session:
            result = await session.execute(
                select(ClientSnapshotORM.window_start, col)
                .where(
                    ClientSnapshotORM.merchant_id == merchant_id,
                    ClientSnapshotORM.window_start >= from_dt,
                    ClientSnapshotORM.window_start <= to_dt,
                )
                .order_by(ClientSnapshotORM.window_start.asc())
                .limit(limit)
            )
            return [(row[0], float(row[1] or 0.0)) for row in result.all()]
    except Exception:
        logger.exception("get_client_metric_series failed merchant=%s", merchant_id)
        return []
