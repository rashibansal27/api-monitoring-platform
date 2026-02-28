"""
GET /api/latency

Returns p95 / p99 / avg latency time-series for a payment API,
with SLO breach flags and baseline comparison.

SLO thresholds (from constants.py):
  p95 > 500ms  → SLO breach
  p99 > 1000ms → SLO breach

Query parameters:
  api_name  — required
  hours     — lookback window (default 24)
  limit     — max data points (default 1440)
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query

from api.dependencies import parse_time_range
from api.schemas import LatencyDataPoint, LatencyResponse
from common.constants import LATENCY_P95_SLO_MS, LATENCY_P99_SLO_MS
from storage.timeseries import get_metric_series, get_windowed_averages

router = APIRouter(prefix="/latency", tags=["Latency"])
logger = logging.getLogger(__name__)


@router.get(
    "",
    response_model=LatencyResponse,
    summary="Latency time-series (p95 / p99 / avg)",
    description=(
        "Returns per-minute latency percentile time-series for the given API. "
        "Each data point includes p95, p99 and average latency in milliseconds, "
        "plus an SLO breach flag when p95 > 500ms or p99 > 1000ms."
    ),
)
async def get_latency(
    api_name: str = Query(..., description="Payment API name (e.g. UPI_COLLECT)"),
    hours: int = Query(default=24, ge=1, le=168, description="Lookback in hours"),
    limit: int = Query(default=1440, ge=1, le=5000),
):
    from_dt, to_dt = parse_time_range(hours)

    # Fetch all three latency series in parallel
    import asyncio
    p95_series, p99_series, avg_series = await asyncio.gather(
        get_metric_series(api_name, "p95_latency_ms", from_dt, to_dt, limit),
        get_metric_series(api_name, "p99_latency_ms", from_dt, to_dt, limit),
        get_metric_series(api_name, "avg_latency_ms", from_dt, to_dt, limit),
    )

    if not p95_series:
        raise HTTPException(
            status_code=404,
            detail=f"No latency data for api_name='{api_name}'.",
        )

    # Index p99 and avg by timestamp for O(1) join
    p99_map = {ts: val for ts, val in p99_series}
    avg_map = {ts: val for ts, val in avg_series}

    baseline = await get_windowed_averages(api_name, "p95_latency_ms", hours=24)

    data_points: list[LatencyDataPoint] = []
    slo_breach_count = 0

    for ts, p95 in p95_series:
        p99 = p99_map.get(ts, 0.0)
        avg = avg_map.get(ts, 0.0)
        is_anomaly = p95 > LATENCY_P95_SLO_MS or p99 > LATENCY_P99_SLO_MS
        if is_anomaly:
            slo_breach_count += 1
        data_points.append(LatencyDataPoint(
            timestamp=ts,
            p95_ms=round(p95, 2),
            p99_ms=round(p99, 2),
            avg_ms=round(avg, 2),
            is_anomaly=is_anomaly,
        ))

    return LatencyResponse(
        api_name=api_name,
        from_dt=from_dt,
        to_dt=to_dt,
        data=data_points,
        baseline_p95_mean=baseline["mean"],
        slo_p95_ms=LATENCY_P95_SLO_MS,
        slo_p99_ms=LATENCY_P99_SLO_MS,
        slo_breach_count=slo_breach_count,
    )
