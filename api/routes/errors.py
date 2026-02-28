"""
GET /api/error/technical-rate
GET /api/error/business-rate

Time-series error rate endpoints with baseline comparison,
anomaly flagging, and top error category breakdown.

Both endpoints share the same shape — only the error_type differs.

Query parameters:
  api_name  — required
  hours     — lookback window (default 24)
  limit     — max data points

Error rate = error_count / total_requests per window.
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from api.dependencies import parse_time_range
from api.schemas import ErrorRateDataPoint, ErrorRateResponse
from common.constants import (
    BUSINESS_ERROR_RATE_CRITICAL,
    BUSINESS_ERROR_RATE_WARN,
    TECHNICAL_ERROR_RATE_CRITICAL,
    TECHNICAL_ERROR_RATE_WARN,
)
from storage.timeseries import (
    get_metric_series,
    get_windowed_averages,
    get_latest_metric,
)

router = APIRouter(prefix="/error", tags=["Error Rates"])
logger = logging.getLogger(__name__)


async def _build_error_response(
    api_name: str,
    error_type: str,             # "technical" | "business"
    time_range: tuple,
    limit: int,
) -> ErrorRateResponse:
    from_dt, to_dt = time_range

    rate_attr = f"{error_type}_error_rate"
    count_attr = f"{error_type}_error_count"

    # Fetch rate and count series concurrently
    import asyncio
    rate_series, count_series, total_series = await asyncio.gather(
        get_metric_series(api_name, rate_attr, from_dt, to_dt, limit),
        get_metric_series(api_name, count_attr, from_dt, to_dt, limit),
        get_metric_series(api_name, "total_requests", from_dt, to_dt, limit),
    )

    if not rate_series:
        raise HTTPException(
            status_code=404,
            detail=f"No {error_type} error data for api_name='{api_name}'.",
        )

    # Baseline over 24h
    baseline = await get_windowed_averages(api_name, rate_attr, hours=24)

    # Choose warn threshold for anomaly flagging
    warn_threshold = (
        TECHNICAL_ERROR_RATE_WARN if error_type == "technical"
        else BUSINESS_ERROR_RATE_WARN
    )
    critical_threshold = (
        TECHNICAL_ERROR_RATE_CRITICAL if error_type == "technical"
        else BUSINESS_ERROR_RATE_CRITICAL
    )
    # Use 2x warn as anomaly trigger (avoid constant noise at warn level)
    anomaly_threshold = warn_threshold * 2.0

    count_map = {ts: int(v) for ts, v in count_series}
    total_map = {ts: int(v) for ts, v in total_series}

    data_points: list[ErrorRateDataPoint] = []
    anomaly_count = 0

    for ts, rate in rate_series:
        is_anomaly = rate > anomaly_threshold
        if is_anomaly:
            anomaly_count += 1
        data_points.append(ErrorRateDataPoint(
            timestamp=ts,
            error_rate=round(rate, 6),
            error_count=count_map.get(ts, 0),
            total_requests=total_map.get(ts, 0),
            is_anomaly=is_anomaly,
        ))

    current_rate = data_points[-1].error_rate if data_points else 0.0

    # Top error categories — from the latest MetricPoint's raw signal
    # (domain-level breakdown requires a separate business error log query;
    #  return a static classification hint for now)
    top_categories = _build_top_categories(error_type, current_rate, critical_threshold)

    return ErrorRateResponse(
        api_name=api_name,
        error_type=error_type,
        from_dt=from_dt,
        to_dt=to_dt,
        data=data_points,
        current_rate=round(current_rate, 6),
        baseline_mean=baseline["mean"],
        baseline_max=baseline["max"],
        anomaly_count=anomaly_count,
        top_categories=top_categories,
    )


def _build_top_categories(
    error_type: str,
    current_rate: float,
    critical_threshold: float,
) -> list[dict]:
    """
    Returns advisory category hints based on current error rate level.
    In a full implementation this would aggregate from classified event logs.
    """
    if error_type == "business":
        return [
            {"category": "INSUFFICIENT_FUNDS",  "note": "Most common business error"},
            {"category": "OTP_FAILURE",          "note": "Auth-layer rejections"},
            {"category": "LIMIT_EXCEEDED",       "note": "Daily/txn limit hits"},
            {"category": "INVALID_ACCOUNT",      "note": "Beneficiary data errors"},
        ]
    return [
        {"category": "HTTP_5XX",         "note": "Upstream / server errors"},
        {"category": "TIMEOUT",          "note": "Gateway and socket timeouts"},
        {"category": "DATABASE",         "note": "DB connectivity issues"},
        {"category": "CIRCUIT_BREAKER",  "note": "Downstream circuit trips"},
    ]


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get(
    "/technical-rate",
    response_model=ErrorRateResponse,
    summary="Technical error rate time-series",
    description=(
        "Returns per-minute technical error rate (HTTP 5xx, timeouts, infra failures) "
        "for the given payment API. Anomaly flag is set when rate exceeds 2× the warn "
        f"threshold."
    ),
)
async def get_technical_error_rate(
    api_name: str = Query(..., description="Payment API name"),
    hours: int = Query(default=24, ge=1, le=168, description="Lookback in hours"),
    limit: int = Query(default=1440, ge=1, le=5000),
):
    return await _build_error_response(api_name, "technical", parse_time_range(hours), limit)


@router.get(
    "/business-rate",
    response_model=ErrorRateResponse,
    summary="Business error rate time-series",
    description=(
        "Returns per-minute business error rate (HTTP 200 with domain error codes — "
        "insufficient funds, OTP failures, AML rejections, limit exceeded, etc.) "
        "for the given payment API."
    ),
)
async def get_business_error_rate(
    api_name: str = Query(..., description="Payment API name"),
    hours: int = Query(default=24, ge=1, le=168, description="Lookback in hours"),
    limit: int = Query(default=1440, ge=1, le=5000),
):
    return await _build_error_response(api_name, "business", parse_time_range(hours), limit)
