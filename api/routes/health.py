"""
GET /api/health/payment
GET /api/health/payment/all

Returns real-time API health scores and platform-wide summaries.

Health score formula (0–100):
  penalty = (
      tech_error_rate   * 40    # technical errors hurt most
    + biz_error_rate    * 20    # business errors reduce score
    + latency_slo_ratio * 25    # p95 vs SLO
    + (1 - success_rate)* 15    # success rate deficit
  ) * 100

  health_score = max(0, 100 - penalty)

Status tiers:
  ≥ 80 → healthy
  ≥ 50 → degraded
  <  50 → critical
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Query

from alerting.alert_store import alert_store
from api.schemas import ApiHealthResponse, PlatformHealthResponse
from common.constants import LATENCY_P95_SLO_MS
from storage.timeseries import (
    get_all_api_names,
    get_latest_metric,
    get_top_failing_merchants,
    get_windowed_averages,
)

router = APIRouter(prefix="/health", tags=["Health"])
logger = logging.getLogger(__name__)


def _compute_health_score(
    tech_error_rate: float,
    biz_error_rate: float,
    p95_latency_ms: float,
    success_rate: float,
) -> float:
    """Compute a 0–100 health score. Lower penalty = higher health."""
    latency_slo_ratio = min(p95_latency_ms / LATENCY_P95_SLO_MS, 3.0)  # cap at 3x SLO
    penalty = (
        tech_error_rate * 40
        + biz_error_rate * 20
        + latency_slo_ratio * 25
        + (1.0 - min(success_rate, 1.0)) * 15
    ) * 100
    return round(max(0.0, 100.0 - penalty), 2)


def _health_status(score: float) -> str:
    if score >= 80:
        return "healthy"
    if score >= 50:
        return "degraded"
    return "critical"


async def _build_api_health(api_name: str) -> Optional[ApiHealthResponse]:
    """Build a health response for one API. Returns None if no data exists."""
    latest = await get_latest_metric(api_name)
    if latest is None:
        return None

    tps_stats = await get_windowed_averages(api_name, "tps", hours=24)
    latency_stats = await get_windowed_averages(api_name, "p95_latency_ms", hours=24)
    tech_err_stats = await get_windowed_averages(api_name, "technical_error_rate", hours=24)
    biz_err_stats = await get_windowed_averages(api_name, "business_error_rate", hours=24)

    tps = float(latest.tps or 0.0)
    tech_err = float(latest.technical_error_rate or 0.0)
    biz_err = float(latest.business_error_rate or 0.0)
    p95 = float(latest.p95_latency_ms or 0.0)
    p99 = float(latest.p99_latency_ms or 0.0)
    avg_lat = float(latest.avg_latency_ms or 0.0)
    success = float(latest.success_rate or 0.0)

    health_score = _compute_health_score(tech_err, biz_err, p95, success)
    open_alerts = await alert_store.count_open_alerts(api_name=api_name, since_hours=1)

    return ApiHealthResponse(
        api_name=api_name,
        timestamp=datetime.now(timezone.utc),
        window_seconds=int(latest.window_seconds or 60),
        tps=round(tps, 4),
        success_rate=round(success, 6),
        technical_error_rate=round(tech_err, 6),
        business_error_rate=round(biz_err, 6),
        p95_latency_ms=round(p95, 2),
        p99_latency_ms=round(p99, 2),
        avg_latency_ms=round(avg_lat, 2),
        tps_baseline_mean=tps_stats["mean"],
        tps_baseline_max=tps_stats["max"],
        latency_p95_baseline_mean=latency_stats["mean"],
        tech_error_rate_24h_mean=tech_err_stats["mean"],
        biz_error_rate_24h_mean=biz_err_stats["mean"],
        health_score=health_score,
        active_open_alerts=open_alerts,
        status=_health_status(health_score),
    )


@router.get(
    "/payment",
    response_model=ApiHealthResponse,
    summary="Payment API health",
    description=(
        "Returns current TPS, error rates, latency percentiles and a composite "
        "health score (0–100) for a specific payment API."
    ),
)
async def get_payment_health(
    api_name: str = Query(
        default="UPI_COLLECT",
        description="Payment API name (e.g. UPI_COLLECT, IMPS_TRANSFER)",
    ),
):
    health = await _build_api_health(api_name)
    if health is None:
        from fastapi import HTTPException
        raise HTTPException(
            status_code=404,
            detail=f"No metrics found for api_name='{api_name}'. "
                   "Ensure the ingestion pipeline is running.",
        )
    return health


@router.get(
    "/payment/all",
    response_model=PlatformHealthResponse,
    summary="Platform-wide health summary",
    description=(
        "Returns health scores for all active APIs seen in the last hour, "
        "plus top failing merchants and open alert count."
    ),
)
async def get_platform_health():
    api_names = await get_all_api_names(since_hours=1)
    summaries = []
    for name in api_names:
        h = await _build_api_health(name)
        if h:
            summaries.append(h)

    overall = round(
        sum(s.health_score for s in summaries) / len(summaries), 2
    ) if summaries else 100.0

    top_merchants = await get_top_failing_merchants(limit=10, since_hours=1)
    total_alerts = await alert_store.count_open_alerts(since_hours=1)

    return PlatformHealthResponse(
        timestamp=datetime.now(timezone.utc),
        api_summaries=summaries,
        overall_health_score=overall,
        top_failing_merchants=top_merchants,
        total_open_alerts=total_alerts,
    )
