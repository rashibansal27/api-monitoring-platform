"""
Runtime Pydantic schemas — data contracts between modules.

Flow:
  LogEvent (raw, normalized)
    → ClassifiedEvent (+ error classification)
      → MetricPoint (aggregated per window)
      → ClientSnapshot (per merchant behaviour)
        → AnomalySignal (per detector output)
          → ScoredAlert (final risk score)
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator

from common.enums import (
    AlertStatus,
    AnomalyType,
    BusinessErrorCategory,
    DataSource,
    DetectorName,
    ErrorType,
    MetricName,
    Severity,
    TechnicalErrorCategory,
)


# ---------------------------------------------------------------------------
# Ingestion Layer
# ---------------------------------------------------------------------------

class LogEvent(BaseModel):
    """
    Unified log event produced by the ingestion normalizer.
    All sources (ES, Prometheus, Kafka) map into this schema.
    """
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime
    source: DataSource

    # API identification
    api_name: str                               # e.g. "UPI_COLLECT", "IMPS_TRANSFER"
    api_path: str                               # e.g. "/payment/upi/collect"
    http_method: str = "POST"

    # HTTP layer
    http_status_code: int
    response_time_ms: float

    # Transaction context
    merchant_id: Optional[str] = None
    client_id: Optional[str] = None
    transaction_id: Optional[str] = None
    transaction_amount: Optional[float] = None
    currency: str = "INR"

    # Geographic context
    request_geo_country: Optional[str] = None
    request_geo_city: Optional[str] = None

    # Response body — needed for HTTP 200 business error detection
    response_body: Optional[dict[str, Any]] = None

    # Error hints from gateway/infra layer
    error_message: Optional[str] = None
    trace_id: Optional[str] = None

    # Original raw payload — preserved for debugging
    raw: dict[str, Any] = Field(default_factory=dict)

    @field_validator("http_status_code")
    @classmethod
    def validate_status_code(cls, v: int) -> int:
        if not (100 <= v <= 599):
            raise ValueError(f"Invalid HTTP status code: {v}")
        return v

    @field_validator("response_time_ms")
    @classmethod
    def validate_response_time(cls, v: float) -> float:
        if v < 0:
            raise ValueError("response_time_ms cannot be negative")
        return v


# ---------------------------------------------------------------------------
# Classification Layer
# ---------------------------------------------------------------------------

class ClassifiedEvent(LogEvent):
    """
    LogEvent enriched with error classification tags.
    Produced by classification/pipeline.py.
    """
    error_type: ErrorType = ErrorType.NONE
    is_success: bool = True

    # Technical error details
    technical_error_category: Optional[TechnicalErrorCategory] = None

    # Business error details (HTTP 200 + domain code)
    business_error_category: Optional[BusinessErrorCategory] = None
    domain_error_code: Optional[str] = None     # Raw code from response body e.g. "RR"
    domain_error_message: Optional[str] = None  # Human-readable from response

    severity: Severity = Severity.INFO
    classification_confidence: float = 1.0      # 0–1; lower for ambiguous cases
    classification_notes: Optional[str] = None


# ---------------------------------------------------------------------------
# Metrics Layer
# ---------------------------------------------------------------------------

class MetricPoint(BaseModel):
    """
    Aggregated metrics for one API over one time window.
    Stored in TimescaleDB as a hypertable row.
    """
    metric_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    window_start: datetime                      # Inclusive start of aggregation window
    window_seconds: int                         # e.g. 60, 300, 3600

    # Scope
    api_name: str
    merchant_id: Optional[str] = None          # None = platform-wide aggregate

    # Volume
    total_requests: int = 0
    success_count: int = 0
    technical_error_count: int = 0
    business_error_count: int = 0

    # Rates (0.0 – 1.0)
    tps: float = 0.0
    success_rate: float = 0.0
    technical_error_rate: float = 0.0
    business_error_rate: float = 0.0

    # Latency (milliseconds)
    avg_latency_ms: float = 0.0
    p50_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0

    # Transaction financials
    total_txn_amount: float = 0.0
    avg_txn_amount: float = 0.0
    max_txn_amount: float = 0.0


class ClientSnapshot(BaseModel):
    """
    Per-merchant behavioural snapshot for one time window.
    Used for client-level anomaly detection.
    """
    snapshot_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    merchant_id: str
    window_start: datetime
    window_seconds: int

    # Volume & frequency
    txn_volume: int = 0
    txn_frequency_per_min: float = 0.0
    failure_ratio: float = 0.0              # technical + business failures / total

    # Financial behaviour
    avg_txn_amount: float = 0.0
    max_txn_amount: float = 0.0
    total_txn_amount: float = 0.0

    # Geographic patterns
    unique_countries: list[str] = Field(default_factory=list)
    unique_cities: list[str] = Field(default_factory=list)
    geo_entropy: float = 0.0               # Shannon entropy of geo distribution

    # Time-of-day pattern (hour 0-23 → count)
    hour_distribution: dict[int, int] = Field(default_factory=dict)

    # Burst / silence flags
    burst_detected: bool = False
    silence_detected: bool = False

    # API breakdown
    api_breakdown: dict[str, int] = Field(default_factory=dict)  # api_name → count


# ---------------------------------------------------------------------------
# Baseline Layer
# ---------------------------------------------------------------------------

class BaselineRecord(BaseModel):
    """
    Expected value for a metric at a given scope + time bucket.
    Supports both rolling and seasonal baselines.
    """
    baseline_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    computed_at: datetime
    metric_name: MetricName

    # Scope
    api_name: str
    merchant_id: Optional[str] = None

    # Rolling baseline (last 7 days, 24h window)
    rolling_mean: float = 0.0
    rolling_std: float = 0.0
    rolling_p95: float = 0.0
    rolling_window_seconds: int = 604800       # 7 days

    # Seasonal baseline (same hour + day-of-week)
    seasonal_mean: float = 0.0
    seasonal_std: float = 0.0
    hour_of_day: Optional[int] = None          # 0–23
    day_of_week: Optional[int] = None          # 0=Monday, 6=Sunday


# ---------------------------------------------------------------------------
# Anomaly Detection Layer
# ---------------------------------------------------------------------------

class AnomalySignal(BaseModel):
    """
    Output of a single anomaly detector for a single metric observation.
    Multiple signals are merged in the scoring engine.
    """
    signal_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    detected_at: datetime

    detector: DetectorName
    anomaly_type: AnomalyType
    metric_name: MetricName

    # Scope
    api_name: Optional[str] = None
    merchant_id: Optional[str] = None

    # Values
    observed_value: float
    expected_value: float
    deviation_score: float                     # Raw score (e.g. Z-score, EWMA residual)
    is_anomaly: bool

    # Confidence (0–1); detectors should calibrate this
    confidence: float = 1.0

    # Human-readable explanation
    description: str = ""

    # Extra detector-specific metadata
    details: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Scoring Layer
# ---------------------------------------------------------------------------

class ScoredAlert(BaseModel):
    """
    Final composite alert produced by the scoring engine.
    risk_score = 0.4 * error_rate_anomaly
               + 0.3 * txn_pattern_anomaly
               + 0.2 * latency_anomaly
               + 0.1 * client_failure_spike
    """
    alert_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = Field(default_factory=datetime.utcnow)

    # Scope
    api_name: Optional[str] = None
    merchant_id: Optional[str] = None

    # Component scores (0–1 each)
    error_rate_anomaly_score: float = 0.0
    txn_pattern_anomaly_score: float = 0.0
    latency_anomaly_score: float = 0.0
    client_failure_spike_score: float = 0.0

    # Composite (weighted)
    risk_score: float = 0.0

    # Derived severity from risk_score thresholds
    severity: Severity = Severity.INFO

    # All contributing signals
    signals: list[AnomalySignal] = Field(default_factory=list)

    # Lifecycle
    status: AlertStatus = AlertStatus.OPEN

    # Human-readable summary
    title: str = ""
    description: str = ""


# ---------------------------------------------------------------------------
# API Response Schemas (read-path)
# ---------------------------------------------------------------------------

class HealthResponse(BaseModel):
    api_name: str
    timestamp: datetime
    tps: float
    success_rate: float
    technical_error_rate: float
    business_error_rate: float
    p95_latency_ms: float
    p99_latency_ms: float
    health_score: float                         # 0–100; derived by API layer
    active_anomalies: int


class TpsResponse(BaseModel):
    api_name: str
    window_seconds: int
    timestamps: list[datetime]
    tps_values: list[float]
    baseline_mean: float
    baseline_std: float
    anomaly_flags: list[bool]


class LatencyResponse(BaseModel):
    api_name: str
    window_seconds: int
    timestamps: list[datetime]
    p95_values: list[float]
    p99_values: list[float]
    avg_values: list[float]
    baseline_p95: float


class ErrorRateResponse(BaseModel):
    api_name: str
    window_seconds: int
    timestamps: list[datetime]
    error_rates: list[float]
    baseline_mean: float
    top_error_categories: list[dict[str, Any]]


class ClientAnomalyResponse(BaseModel):
    merchant_id: str
    timestamp: datetime
    risk_score: float
    severity: Severity
    active_signals: list[AnomalySignal]
    snapshot: Optional[ClientSnapshot] = None
