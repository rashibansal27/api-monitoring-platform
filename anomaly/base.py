"""
Abstract base classes and shared utilities for all anomaly detectors.

Two detector contracts:
  MetricDetector   — operates on MetricPoint (univariate + multivariate)
  ClientDetector   — operates on ClientSnapshot (client-level behaviour)

Both return list[AnomalySignal] — empty list = no anomaly detected.

AnomalyType resolution:
  The anomaly type depends on BOTH the metric being tested AND the direction
  of the deviation (spike vs. drop). resolve_anomaly_type() encodes this.
"""

from __future__ import annotations

import math
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

from common.constants import (
    SEVERITY_CRITICAL_THRESHOLD,
    SEVERITY_HIGH_THRESHOLD,
    SEVERITY_LOW_THRESHOLD,
    SEVERITY_MEDIUM_THRESHOLD,
    ZSCORE_ANOMALY_THRESHOLD,
    ZSCORE_WARNING_THRESHOLD,
)
from common.enums import AnomalyType, DetectorName, MetricName, Severity
from common.schemas import AnomalySignal, ClientSnapshot, MetricPoint


# ---------------------------------------------------------------------------
# Abstract base classes
# ---------------------------------------------------------------------------

class MetricDetector(ABC):
    """
    Univariate or multivariate detector that ingests a MetricPoint.
    Stateful — maintains rolling state per (api_name, metric_name) key.
    """

    @abstractmethod
    def detect(self, metric: MetricPoint) -> list[AnomalySignal]:
        """
        Evaluate the MetricPoint for anomalies.
        Must not raise. Returns empty list if no anomaly is found.
        """

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"


class ClientDetector(ABC):
    """
    Detector that operates on per-merchant ClientSnapshot objects.
    """

    @abstractmethod
    def detect(self, snapshot: ClientSnapshot) -> list[AnomalySignal]:
        """
        Evaluate the ClientSnapshot for anomalies.
        Must not raise. Returns empty list if no anomaly is found.
        """


# ---------------------------------------------------------------------------
# AnomalyType resolution
# ---------------------------------------------------------------------------

# Metrics where a high value is "bad" (spike = anomaly)
_SPIKE_METRICS: frozenset[MetricName] = frozenset({
    MetricName.TECHNICAL_ERROR_RATE,
    MetricName.BUSINESS_ERROR_RATE,
    MetricName.P95_LATENCY_MS,
    MetricName.P99_LATENCY_MS,
    MetricName.AVG_LATENCY_MS,
    MetricName.FAILURE_RATIO,
})

# Metrics where a low value is "bad" (drop = anomaly)
_DROP_METRICS: frozenset[MetricName] = frozenset({
    MetricName.SUCCESS_RATE,
    MetricName.TXN_VOLUME,
})

# Metrics that can spike OR drop
_BIDIRECTIONAL_METRICS: frozenset[MetricName] = frozenset({
    MetricName.TPS,
    MetricName.AVG_TXN_AMOUNT,
})

_METRIC_TO_SPIKE_TYPE: dict[MetricName, AnomalyType] = {
    MetricName.TPS:                   AnomalyType.TPS_SPIKE,
    MetricName.TECHNICAL_ERROR_RATE:  AnomalyType.TECHNICAL_ERROR_SPIKE,
    MetricName.BUSINESS_ERROR_RATE:   AnomalyType.BUSINESS_ERROR_SPIKE,
    MetricName.P95_LATENCY_MS:        AnomalyType.LATENCY_INCREASE,
    MetricName.P99_LATENCY_MS:        AnomalyType.LATENCY_INCREASE,
    MetricName.AVG_LATENCY_MS:        AnomalyType.LATENCY_INCREASE,
    MetricName.SUCCESS_RATE:          AnomalyType.ERROR_RATE_SPIKE,   # drop in success = error spike
    MetricName.AVG_TXN_AMOUNT:        AnomalyType.AMOUNT_ANOMALY,
    MetricName.FAILURE_RATIO:         AnomalyType.CLIENT_FAILURE_SPIKE,
}

_METRIC_TO_DROP_TYPE: dict[MetricName, AnomalyType] = {
    MetricName.TPS:        AnomalyType.TPS_DROP,
    MetricName.TXN_VOLUME: AnomalyType.TPS_DROP,
    MetricName.SUCCESS_RATE: AnomalyType.ERROR_RATE_SPIKE,
    MetricName.AVG_TXN_AMOUNT: AnomalyType.AMOUNT_ANOMALY,
}


def resolve_anomaly_type(metric_name: MetricName, z_score: float) -> AnomalyType:
    """
    Determine the AnomalyType given a metric and the signed deviation.
    Positive z_score = current > expected (spike).
    Negative z_score = current < expected (drop).
    """
    is_spike = z_score >= 0

    if is_spike:
        return _METRIC_TO_SPIKE_TYPE.get(metric_name, AnomalyType.ERROR_RATE_SPIKE)
    else:
        return _METRIC_TO_DROP_TYPE.get(metric_name, AnomalyType.TPS_DROP)


# ---------------------------------------------------------------------------
# Confidence calibration
# ---------------------------------------------------------------------------

def calibrate_confidence(deviation_score: float, threshold: float = ZSCORE_ANOMALY_THRESHOLD) -> float:
    """
    Map a raw deviation score to a [0, 1] confidence value.
    Uses a sigmoid-like curve anchored at the detection threshold.

    At threshold   → confidence ~0.70
    At 2x threshold → confidence ~0.92
    At 3x threshold → confidence ~0.98
    """
    if deviation_score <= 0:
        return 0.0
    if deviation_score < threshold:
        # Below threshold — scale linearly from 0 to 0.65
        return min(0.65, (deviation_score / threshold) * 0.65)

    # At or above threshold — sigmoid squeeze into [0.70, 1.0]
    ratio = deviation_score / threshold
    sigmoid = 1.0 / (1.0 + math.exp(-(ratio - 1) * 2.5))
    return round(0.65 + 0.35 * sigmoid, 4)


# ---------------------------------------------------------------------------
# Signal builders
# ---------------------------------------------------------------------------

def make_signal(
    detector: DetectorName,
    anomaly_type: AnomalyType,
    metric_name: MetricName,
    observed: float,
    expected: float,
    deviation_score: float,
    is_anomaly: bool,
    detected_at: datetime,
    api_name: Optional[str] = None,
    merchant_id: Optional[str] = None,
    description: str = "",
    details: Optional[dict] = None,
) -> AnomalySignal:
    confidence = calibrate_confidence(deviation_score) if is_anomaly else 0.0
    return AnomalySignal(
        detected_at=detected_at,
        detector=detector,
        anomaly_type=anomaly_type,
        metric_name=metric_name,
        api_name=api_name,
        merchant_id=merchant_id,
        observed_value=round(observed, 6),
        expected_value=round(expected, 6),
        deviation_score=round(deviation_score, 4),
        is_anomaly=is_anomaly,
        confidence=confidence,
        description=description,
        details=details or {},
    )


def severity_from_score(risk_score: float) -> Severity:
    if risk_score >= SEVERITY_CRITICAL_THRESHOLD:
        return Severity.CRITICAL
    if risk_score >= SEVERITY_HIGH_THRESHOLD:
        return Severity.HIGH
    if risk_score >= SEVERITY_MEDIUM_THRESHOLD:
        return Severity.MEDIUM
    if risk_score >= SEVERITY_LOW_THRESHOLD:
        return Severity.LOW
    return Severity.INFO
