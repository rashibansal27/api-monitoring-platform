"""
Route-level request and response schemas.

Kept separate from common/schemas.py (domain models) so the API contract
can evolve independently of the internal data pipeline.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field

from common.enums import AlertStatus, AnomalyType, Severity


# ---------------------------------------------------------------------------
# Shared pagination / meta
# ---------------------------------------------------------------------------

class TimeRangeParams(BaseModel):
    """Common query parameters for time-range endpoints."""
    hours: int = Field(default=24, ge=1, le=168, description="Lookback in hours (max 7 days)")
    limit: int = Field(default=1440, ge=1, le=10000, description="Max data points returned")


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

class ApiHealthResponse(BaseModel):
    api_name: str
    timestamp: datetime
    window_seconds: int

    # Current metrics
    tps: float
    success_rate: float
    technical_error_rate: float
    business_error_rate: float
    p95_latency_ms: float
    p99_latency_ms: float
    avg_latency_ms: float

    # 24h baseline comparisons
    tps_baseline_mean: float
    tps_baseline_max: float
    latency_p95_baseline_mean: float
    tech_error_rate_24h_mean: float
    biz_error_rate_24h_mean: float

    # Derived
    health_score: float = Field(description="0–100 composite health score")
    active_open_alerts: int
    status: str = Field(description="healthy | degraded | critical")


class PlatformHealthResponse(BaseModel):
    """Health summary across all active APIs."""
    timestamp: datetime
    api_summaries: list[ApiHealthResponse]
    overall_health_score: float
    top_failing_merchants: list[dict[str, Any]]
    total_open_alerts: int


# ---------------------------------------------------------------------------
# TPS
# ---------------------------------------------------------------------------

class TpsDataPoint(BaseModel):
    timestamp: datetime
    tps: float
    is_anomaly: bool = False


class TpsResponse(BaseModel):
    api_name: str
    from_dt: datetime
    to_dt: datetime
    window_seconds: int
    data: list[TpsDataPoint]
    baseline_mean: float
    baseline_max: float
    current_tps: float
    anomaly_count: int


# ---------------------------------------------------------------------------
# Latency
# ---------------------------------------------------------------------------

class LatencyDataPoint(BaseModel):
    timestamp: datetime
    p95_ms: float
    p99_ms: float
    avg_ms: float
    is_anomaly: bool = False


class LatencyResponse(BaseModel):
    api_name: str
    from_dt: datetime
    to_dt: datetime
    data: list[LatencyDataPoint]
    baseline_p95_mean: float
    slo_p95_ms: float
    slo_p99_ms: float
    slo_breach_count: int


# ---------------------------------------------------------------------------
# Error rates
# ---------------------------------------------------------------------------

class ErrorRateDataPoint(BaseModel):
    timestamp: datetime
    error_rate: float
    error_count: int
    total_requests: int
    is_anomaly: bool = False


class ErrorRateResponse(BaseModel):
    api_name: str
    error_type: str       # "technical" | "business"
    from_dt: datetime
    to_dt: datetime
    data: list[ErrorRateDataPoint]
    current_rate: float
    baseline_mean: float
    baseline_max: float
    anomaly_count: int
    top_categories: list[dict[str, Any]]


# ---------------------------------------------------------------------------
# Client / merchant anomaly
# ---------------------------------------------------------------------------

class SignalSummary(BaseModel):
    signal_id: str
    detector: str
    anomaly_type: str
    metric_name: str
    observed_value: float
    expected_value: float
    deviation_score: float
    confidence: float
    description: str
    detected_at: datetime


class ClientSnapshotSummary(BaseModel):
    merchant_id: str
    window_start: datetime
    txn_volume: int
    txn_frequency_per_min: float
    failure_ratio: float
    avg_txn_amount: float
    geo_entropy: float
    unique_countries: list[str]
    burst_detected: bool
    silence_detected: bool


class AlertSummary(BaseModel):
    alert_id: str
    created_at: datetime
    severity: str
    risk_score: float
    title: str
    status: str


class ClientAnomalyResponse(BaseModel):
    merchant_id: str
    timestamp: datetime
    risk_score: float
    severity: Severity
    active_signals: list[SignalSummary]
    latest_snapshot: Optional[ClientSnapshotSummary]
    recent_alerts: list[AlertSummary]


# ---------------------------------------------------------------------------
# Anomaly signal list (generic endpoint)
# ---------------------------------------------------------------------------

class AnomalyListResponse(BaseModel):
    api_name: Optional[str]
    from_dt: datetime
    to_dt: datetime
    total_signals: int
    signals: list[SignalSummary]
