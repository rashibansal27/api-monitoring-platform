"""
Rolling percentile band detector.

Maintains a fixed-size rolling buffer of historical observations.
Flags a new value as anomalous if it falls OUTSIDE the [p5, p95] band
(configurable via PERCENTILE_LOWER_BAND and PERCENTILE_UPPER_BAND).

Complements Z-score:
  Z-score assumes normality — percentile bands make no distributional assumption.
  Payment API metrics (TPS, latency) are often log-normal or bimodal;
  percentile bands handle these shapes correctly.

Direction semantics:
  value > p95  → spike    (TPS surge, error rate spike, latency spike)
  value < p5   → drop     (TPS collapse, success rate collapse)
"""

from __future__ import annotations

import logging
from collections import deque
from datetime import timezone

import numpy as np

from anomaly.base import (
    MetricDetector,
    make_signal,
    resolve_anomaly_type,
)
from common.constants import (
    MIN_SAMPLES_FOR_BASELINE,
    PERCENTILE_LOWER_BAND,
    PERCENTILE_UPPER_BAND,
    PERCENTILE_WINDOW_SIZE,
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


class PercentileDetector(MetricDetector):
    """
    Non-parametric rolling percentile band detector.
    Fires when a value crosses outside the [p_lower, p_upper] historical band.
    """

    def __init__(
        self,
        window_size: int = PERCENTILE_WINDOW_SIZE,
        lower_band: float = PERCENTILE_LOWER_BAND,
        upper_band: float = PERCENTILE_UPPER_BAND,
    ) -> None:
        self._window_size = window_size
        self._lower_band = lower_band
        self._upper_band = upper_band
        # (api_name, metric_name) → deque of float
        self._buffers: dict[tuple[str, str], deque[float]] = {}

    def detect(self, metric: MetricPoint) -> list[AnomalySignal]:
        signals: list[AnomalySignal] = []
        ts = metric.window_start.replace(tzinfo=timezone.utc)

        for metric_name, attr in _METRIC_ATTRS:
            current = getattr(metric, attr, None)
            if current is None:
                continue

            key = (metric.api_name, metric_name.value)
            buf = self._buffers.setdefault(
                key, deque(maxlen=self._window_size)
            )

            if len(buf) < MIN_SAMPLES_FOR_BASELINE:
                buf.append(current)
                continue

            arr = np.array(buf, dtype=np.float64)
            p_low = float(np.percentile(arr, self._lower_band))
            p_high = float(np.percentile(arr, self._upper_band))
            band_width = max(p_high - p_low, 1e-10)

            buf.append(current)  # Update after scoring

            if current > p_high:
                deviation_score = (current - p_high) / band_width
                signals.append(make_signal(
                    detector=DetectorName.ROLLING_PERCENTILE,
                    anomaly_type=resolve_anomaly_type(metric_name, z_score=1.0),
                    metric_name=metric_name,
                    observed=current,
                    expected=p_high,
                    deviation_score=deviation_score,
                    is_anomaly=True,
                    detected_at=ts,
                    api_name=metric.api_name,
                    description=(
                        f"[Percentile] {metric.api_name} {metric_name.value} "
                        f"{current:.4f} > p{self._upper_band:.0f}={p_high:.4f}"
                    ),
                    details={
                        "p_lower": round(p_low, 6),
                        "p_upper": round(p_high, 6),
                        "direction": "spike",
                        "window_size": len(arr),
                    },
                ))

            elif current < p_low:
                deviation_score = (p_low - current) / band_width
                signals.append(make_signal(
                    detector=DetectorName.ROLLING_PERCENTILE,
                    anomaly_type=resolve_anomaly_type(metric_name, z_score=-1.0),
                    metric_name=metric_name,
                    observed=current,
                    expected=p_low,
                    deviation_score=deviation_score,
                    is_anomaly=True,
                    detected_at=ts,
                    api_name=metric.api_name,
                    description=(
                        f"[Percentile] {metric.api_name} {metric_name.value} "
                        f"{current:.4f} < p{self._lower_band:.0f}={p_low:.4f}"
                    ),
                    details={
                        "p_lower": round(p_low, 6),
                        "p_upper": round(p_high, 6),
                        "direction": "drop",
                        "window_size": len(arr),
                    },
                ))

        return signals
