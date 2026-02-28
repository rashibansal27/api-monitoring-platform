"""
SQLAlchemy ORM models for TimescaleDB (hypertables) and PostgreSQL.

Tables:
  metric_points       — TimescaleDB hypertable; time-series API metrics
  client_snapshots    — TimescaleDB hypertable; per-merchant behavioural snapshots
  baselines           — Rolling and seasonal baseline values
  alerts              — Alert lifecycle records
  anomaly_signals     — Individual detector signals linked to alerts
"""

import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# metric_points  (TimescaleDB hypertable — partition by window_start)
# ---------------------------------------------------------------------------

class MetricPointORM(Base):
    """
    One aggregated metric snapshot per API per time window.
    Created as a TimescaleDB hypertable on `window_start`.
    """
    __tablename__ = "metric_points"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    window_start = Column(DateTime(timezone=True), nullable=False, index=True)
    window_seconds = Column(Integer, nullable=False, default=60)

    # Scope
    api_name = Column(String(120), nullable=False, index=True)
    merchant_id = Column(String(120), nullable=True, index=True)  # None = platform-wide

    # Volume
    total_requests = Column(BigInteger, default=0)
    success_count = Column(BigInteger, default=0)
    technical_error_count = Column(BigInteger, default=0)
    business_error_count = Column(BigInteger, default=0)

    # Rates
    tps = Column(Float, default=0.0)
    success_rate = Column(Float, default=0.0)
    technical_error_rate = Column(Float, default=0.0)
    business_error_rate = Column(Float, default=0.0)

    # Latency (ms)
    avg_latency_ms = Column(Float, default=0.0)
    p50_latency_ms = Column(Float, default=0.0)
    p95_latency_ms = Column(Float, default=0.0)
    p99_latency_ms = Column(Float, default=0.0)

    # Financial
    total_txn_amount = Column(Float, default=0.0)
    avg_txn_amount = Column(Float, default=0.0)
    max_txn_amount = Column(Float, default=0.0)

    __table_args__ = (
        Index("ix_metric_points_api_window", "api_name", "window_start"),
        Index("ix_metric_points_merchant_window", "merchant_id", "window_start"),
    )


# ---------------------------------------------------------------------------
# client_snapshots  (TimescaleDB hypertable — partition by window_start)
# ---------------------------------------------------------------------------

class ClientSnapshotORM(Base):
    """
    Per-merchant behavioural snapshot for a time window.
    Used for client-level anomaly detection and trend analysis.
    """
    __tablename__ = "client_snapshots"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    merchant_id = Column(String(120), nullable=False, index=True)
    window_start = Column(DateTime(timezone=True), nullable=False, index=True)
    window_seconds = Column(Integer, nullable=False, default=60)

    # Volume & frequency
    txn_volume = Column(BigInteger, default=0)
    txn_frequency_per_min = Column(Float, default=0.0)
    failure_ratio = Column(Float, default=0.0)

    # Financial behaviour
    avg_txn_amount = Column(Float, default=0.0)
    max_txn_amount = Column(Float, default=0.0)
    total_txn_amount = Column(Float, default=0.0)

    # Geographic — stored as JSONB for flexibility
    unique_countries = Column(JSONB, default=list)          # ["IN", "SG"]
    unique_cities = Column(JSONB, default=list)
    geo_entropy = Column(Float, default=0.0)

    # Time-of-day pattern
    hour_distribution = Column(JSONB, default=dict)         # {"9": 45, "10": 67, ...}

    # Burst / silence
    burst_detected = Column(Boolean, default=False)
    silence_detected = Column(Boolean, default=False)

    # API breakdown
    api_breakdown = Column(JSONB, default=dict)

    __table_args__ = (
        Index("ix_client_snapshots_merchant_window", "merchant_id", "window_start"),
    )


# ---------------------------------------------------------------------------
# baselines
# ---------------------------------------------------------------------------

class BaselineORM(Base):
    """
    Rolling and seasonal baseline values for a metric at a given scope.
    Updated periodically by the baseline service.
    """
    __tablename__ = "baselines"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    computed_at = Column(DateTime(timezone=True), nullable=False, index=True)

    metric_name = Column(String(80), nullable=False)
    api_name = Column(String(120), nullable=False, index=True)
    merchant_id = Column(String(120), nullable=True)

    # Rolling statistics (7-day window)
    rolling_mean = Column(Float, default=0.0)
    rolling_std = Column(Float, default=0.0)
    rolling_p95 = Column(Float, default=0.0)
    rolling_window_seconds = Column(Integer, default=604800)

    # Seasonal statistics (same hour + DOW over 4 weeks)
    seasonal_mean = Column(Float, default=0.0)
    seasonal_std = Column(Float, default=0.0)
    hour_of_day = Column(Integer, nullable=True)            # 0–23
    day_of_week = Column(Integer, nullable=True)            # 0=Mon, 6=Sun

    __table_args__ = (
        Index("ix_baselines_api_metric", "api_name", "metric_name"),
        Index(
            "ix_baselines_seasonal",
            "api_name", "metric_name", "hour_of_day", "day_of_week",
        ),
    )


# ---------------------------------------------------------------------------
# alerts
# ---------------------------------------------------------------------------

class AlertORM(Base):
    """
    Persisted alert record. One row per scored alert event.
    """
    __tablename__ = "alerts"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    created_at = Column(DateTime(timezone=True), nullable=False, index=True)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)

    # Scope
    api_name = Column(String(120), nullable=True, index=True)
    merchant_id = Column(String(120), nullable=True, index=True)

    # Scores
    risk_score = Column(Float, nullable=False)
    error_rate_anomaly_score = Column(Float, default=0.0)
    txn_pattern_anomaly_score = Column(Float, default=0.0)
    latency_anomaly_score = Column(Float, default=0.0)
    client_failure_spike_score = Column(Float, default=0.0)

    # Classification
    severity = Column(String(20), nullable=False, default="info")
    status = Column(String(20), nullable=False, default="open", index=True)

    # Human-readable
    title = Column(String(255), default="")
    description = Column(Text, default="")

    # Linked signals (FK)
    signals = relationship("AnomalySignalORM", back_populates="alert", cascade="all, delete-orphan")


# ---------------------------------------------------------------------------
# anomaly_signals
# ---------------------------------------------------------------------------

class AnomalySignalORM(Base):
    """
    Individual anomaly detector output linked to an alert.
    One alert can aggregate many signals from different detectors.
    """
    __tablename__ = "anomaly_signals"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    detected_at = Column(DateTime(timezone=True), nullable=False, index=True)

    # Parent alert
    alert_id = Column(
        UUID(as_uuid=True),
        ForeignKey("alerts.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    alert = relationship("AlertORM", back_populates="signals")

    # Detector identity
    detector = Column(String(40), nullable=False)
    anomaly_type = Column(String(60), nullable=False)
    metric_name = Column(String(80), nullable=False)

    # Scope
    api_name = Column(String(120), nullable=True)
    merchant_id = Column(String(120), nullable=True)

    # Values
    observed_value = Column(Float, nullable=False)
    expected_value = Column(Float, nullable=False)
    deviation_score = Column(Float, nullable=False)
    is_anomaly = Column(Boolean, nullable=False, default=True)
    confidence = Column(Float, default=1.0)

    # Extra context
    description = Column(Text, default="")
    details = Column(JSONB, default=dict)

    __table_args__ = (
        Index("ix_anomaly_signals_detected_at", "detected_at"),
        Index("ix_anomaly_signals_api_type", "api_name", "anomaly_type"),
    )
