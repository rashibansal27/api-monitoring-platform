"""
EWMA (Exponentially Weighted Moving Average) anomaly detector.

Unlike Z-score, EWMA weights recent observations more heavily — critical
for payment APIs where baseline behaviour shifts during business hours,
weekends, and promotional events.

State per (api_name, metric_name):
  ewma_mean     = α * x_t + (1-α) * ewma_mean_{t-1}
  ewma_variance = (1-α) * (ewma_variance_{t-1} + α * (x_t - ewma_mean_{t-1})²)
  ewma_std      = sqrt(ewma_variance)

Anomaly condition:
  |current - ewma_mean| > EWMA_ANOMALY_THRESHOLD_MULTIPLIER * ewma_std

Key advantage over Z-score:
  Adapts to trend shifts without requiring a hard window reset.
  A legitimate surge in TPS during a flash sale will shift the EWMA
  gradually without triggering false positives after the first peak.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import timezone

from anomaly.base import (
    MetricDetector,
    make_signal,
    resolve_anomaly_type,
)
from common.constants import (
    EWMA_ALPHA,
    EWMA_ANOMALY_THRESHOLD_MULTIPLIER,
    MIN_SAMPLES_FOR_BASELINE,
)
from common.enums import DetectorName, MetricName
from common.schemas import AnomalySignal, MetricPoint

logger = logging.getLogger(__name__)

_METRIC_ATTRS: list[tuple[MetricName, str]] = [
    (MetricName.TPS,                  "tps"),
    (MetricName.TECHNICAL_ERROR_RATE, "technical_error_rate"),
    (MetricName.BUSINESS_ERROR_RATE,  "business_error_rate"),
    (MetricName.P95_LATENCY_MS,       "p95_latency_ms"),
    (MetricName.SUCCESS_RATE,         "success_rate"),
]


@dataclass
class _EWMAState:
    mean: float
    variance: float
    count: int = 0

    @property
    def std(self) -> float:
        return math.sqrt(self.variance) if self.variance > 0 else 0.0


class EWMADetector(MetricDetector):
    """
    EWMA-based anomaly detector.
    Stateful: one _EWMAState per (api_name, metric_name) key.
    """

    def __init__(
        self,
        alpha: float = EWMA_ALPHA,
        threshold_multiplier: float = EWMA_ANOMALY_THRESHOLD_MULTIPLIER,
    ) -> None:
        if not (0 < alpha < 1):
            raise ValueError(f"EWMA alpha must be in (0, 1), got {alpha}")
        self._alpha = alpha
        self._threshold = threshold_multiplier
        # (api_name, metric_name) → _EWMAState
        self._state: dict[tuple[str, str], _EWMAState] = {}

    def detect(self, metric: MetricPoint) -> list[AnomalySignal]:
        signals: list[AnomalySignal] = []
        ts = metric.window_start.replace(tzinfo=timezone.utc)

        for metric_name, attr in _METRIC_ATTRS:
            current = getattr(metric, attr, None)
            if current is None:
                continue

            key = (metric.api_name, metric_name.value)

            # Initialise state on first observation
            if key not in self._state:
                self._state[key] = _EWMAState(mean=current, variance=0.0, count=1)
                continue

            state = self._state[key]
            prev_mean = state.mean
            alpha = self._alpha

            # Update EWMA mean and variance BEFORE scoring
            # so the signal reflects the previous expectation vs. current
            deviation = current - prev_mean

            # Variance update (Welford-style EWMA)
            state.variance = (1 - alpha) * (state.variance + alpha * deviation ** 2)
            state.mean = alpha * current + (1 - alpha) * state.mean
            state.count += 1

            if state.count < MIN_SAMPLES_FOR_BASELINE:
                continue

            ewma_std = state.std
            if ewma_std < 1e-10:
                continue

            abs_deviation = abs(deviation)
            threshold_value = self._threshold * ewma_std
            is_anomaly = abs_deviation > threshold_value

            if not is_anomaly:
                continue

            deviation_score = abs_deviation / ewma_std

            signals.append(make_signal(
                detector=DetectorName.EWMA,
                anomaly_type=resolve_anomaly_type(metric_name, deviation),
                metric_name=metric_name,
                observed=current,
                expected=prev_mean,
                deviation_score=deviation_score,
                is_anomaly=True,
                detected_at=ts,
                api_name=metric.api_name,
                description=(
                    f"[EWMA] {metric.api_name} {metric_name.value} "
                    f"deviated {deviation:+.4f} from EWMA={prev_mean:.4f} "
                    f"(threshold={threshold_value:.4f}, std={ewma_std:.4f})"
                ),
                details={
                    "ewma_mean": round(prev_mean, 6),
                    "ewma_std": round(ewma_std, 6),
                    "deviation": round(deviation, 6),
                    "threshold_multiplier": self._threshold,
                    "alpha": self._alpha,
                    "count": state.count,
                },
            ))

        return signals

    def get_state(self, api_name: str, metric_name: MetricName) -> _EWMAState | None:
        return self._state.get((api_name, metric_name.value))

    def reset(self, api_name: str | None = None) -> None:
        """Reset state for a specific API or all APIs (useful in tests)."""
        if api_name is None:
            self._state.clear()
        else:
            keys = [k for k in self._state if k[0] == api_name]
            for k in keys:
                del self._state[k]
