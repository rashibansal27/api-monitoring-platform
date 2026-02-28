"""
GET /api/tps

Returns TPS time-series for a payment API with baseline overlay
and per-point anomaly flags derived from active Redis anomaly signals.

Query parameters:
  api_name  — required; which API to query
  hours     — lookback window (1–168, default 24)
  limit     — max data points (default 1440)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from api.dependencies import parse_time_range
from api.schemas import TpsDataPoint, TpsResponse
from storage.redis_client import get_active_anomalies, get_redis
from storage.timeseries import get_metric_series, get_windowed_averages
from common.enums import AnomalyType

router = APIRouter(prefix="/tps", tags=["TPS"])
logger = logging.getLogger(__name__)

_TPS_ANOMALY_TYPES = {AnomalyType.TPS_SPIKE.value, AnomalyType.TPS_DROP.value}


@router.get(
    "",
    response_model=TpsResponse,
    summary="TPS time-series",
    description=(
        "Returns transactions-per-second time-series data for the given API "
        "over the requested lookback window, with 24h baseline statistics "
        "and anomaly flags on each data point."
    ),
)
async def get_tps(
    api_name: str = Query(..., description="Payment API name (e.g. UPI_COLLECT)"),
    hours: int = Query(default=24, ge=1, le=168, description="Lookback in hours"),
    limit: int = Query(default=1440, ge=1, le=5000),
):
    from_dt, to_dt = parse_time_range(hours)

    # Fetch time-series from TimescaleDB
    series = await get_metric_series(api_name, "tps", from_dt, to_dt, limit)
    if not series:
        raise HTTPException(
            status_code=404,
            detail=f"No TPS data found for api_name='{api_name}' in the last {(to_dt - from_dt).seconds // 3600}h.",
        )

    # Fetch 24h baseline stats
    baseline = await get_windowed_averages(api_name, "tps", hours=24)

    # Get active anomaly signals from Redis to flag individual points
    r = get_redis()
    active_anomalies = await get_active_anomalies(r, api_name)
    anomaly_types_active = {
        v.get("anomaly_type") for v in active_anomalies.values()
    }
    has_tps_anomaly = bool(anomaly_types_active & _TPS_ANOMALY_TYPES)

    # Build per-point anomaly flag
    # A point is flagged if it deviates > 2x from baseline mean
    mean = baseline["mean"]
    threshold_high = mean * 3.0 if mean > 0 else float("inf")
    threshold_low = mean * 0.1 if mean > 0 else 0.0

    data_points: list[TpsDataPoint] = []
    anomaly_count = 0
    for ts, tps_val in series:
        is_anomaly = (
            has_tps_anomaly
            and mean > 0
            and (tps_val > threshold_high or tps_val < threshold_low)
        )
        if is_anomaly:
            anomaly_count += 1
        data_points.append(TpsDataPoint(
            timestamp=ts,
            tps=round(tps_val, 4),
            is_anomaly=is_anomaly,
        ))

    current_tps = data_points[-1].tps if data_points else 0.0

    return TpsResponse(
        api_name=api_name,
        from_dt=from_dt,
        to_dt=to_dt,
        window_seconds=60,
        data=data_points,
        baseline_mean=baseline["mean"],
        baseline_max=baseline["max"],
        current_tps=current_tps,
        anomaly_count=anomaly_count,
    )
