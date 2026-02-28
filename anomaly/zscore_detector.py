"""
Z-score anomaly detector.

For each (api_name, metric_name) pair, maintains a rolling window of
historical observations. When a new value arrives:

  Z = (current - window_mean) / window_std

  |Z| > ZSCORE_ANOMALY_THRESHOLD (3.0) → anomaly
  |Z| > ZSCORE_WARNING_THRESHOLD (2.0) → warning (is_anomaly=False but signal emitted)

Self-contained — no external baseline store needed.
The rolling window IS the baseline.

Metrics tested per MetricPoint:
  TPS, technical_error_rate, business_error_rate, p95_latency_ms, success_rate
"""

from __future__ import annotations

import logging
from collections import deque
from datetime import timezone

import numpy as np

from anomaly.base import (
    MetricDetector,
    calibrate_confidence,
    make_signal,
    resolve_anomaly_type,
)
from common.constants import (
    MIN_SAMPLES_FOR_BASELINE,
    ZSCORE_ANOMALY_THRESHOLD,
    ZSCORE_WARNING_THRESHOLD,
)
from common.enums import DetectorName, MetricName
from common.schemas import AnomalySignal, MetricPoint

logger = logging.getLogger(__name__)

# Metrics extracted from MetricPoint and their attribute names
_METRIC_ATTRS: list[tuple[MetricName, str]] = [
    (MetricName.TPS,                  "tps"),
    (MetricName.TECHNICAL_ERROR_RATE, "technical_error_rate"),
    (MetricName.BUSINESS_ERROR_RATE,  "business_error_rate"),
    (MetricName.P95_LATENCY_MS,       "p95_latency_ms"),
    (MetricName.SUCCESS_RATE,         "success_rate"),
]

# Rolling window size (number of observations)
_WINDOW_SIZE = 60


class ZScoreDetector(MetricDetector):
    """
    Rolling Z-score detector.
    Stateful: one deque per (api_name, metric_name) key.
    """

    def __init__(
        self,
        window_size: int = _WINDOW_SIZE,
        anomaly_threshold: float = ZSCORE_ANOMALY_THRESHOLD,
        warning_threshold: float = ZSCORE_WARNING_THRESHOLD,
    ) -> None:
        self._window_size = window_size
        self._anomaly_threshold = anomaly_threshold
        self._warning_threshold = warning_threshold
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

            # Need enough history before scoring
            if len(buf) < MIN_SAMPLES_FOR_BASELINE:
                buf.append(current)
                continue

            arr = np.array(buf, dtype=np.float64)
            mean = float(np.mean(arr))
            std = float(np.std(arr, ddof=1))

            # Update buffer AFTER scoring so the current point
            # does not inflate its own mean/std
            buf.append(current)

            if std < 1e-10:
                # Perfectly flat history — any change is notable
                if abs(current - mean) > 1e-6:
                    signals.append(make_signal(
                        detector=DetectorName.ZSCORE,
                        anomaly_type=resolve_anomaly_type(metric_name, current - mean),
                        metric_name=metric_name,
                        observed=current,
                        expected=mean,
                        deviation_score=self._anomaly_threshold,
                        is_anomaly=True,
                        detected_at=ts,
                        api_name=metric.api_name,
                        description=(
                            f"[Z-score] {metric_name.value} deviated from "
                            f"flat baseline {mean:.4f} → {current:.4f}"
                        ),
                        details={"std": 0.0, "z_score": None, "window_size": len(arr)},
                    ))
                continue

            z = (current - mean) / std
            abs_z = abs(z)

            if abs_z < self._warning_threshold:
                continue

            is_anomaly = abs_z >= self._anomaly_threshold
            signals.append(make_signal(
                detector=DetectorName.ZSCORE,
                anomaly_type=resolve_anomaly_type(metric_name, z),
                metric_name=metric_name,
                observed=current,
                expected=mean,
                deviation_score=abs_z,
                is_anomaly=is_anomaly,
                detected_at=ts,
                api_name=metric.api_name,
                description=(
                    f"[Z-score] {metric.api_name} {metric_name.value} "
                    f"Z={z:+.2f} (obs={current:.4f} mean={mean:.4f} std={std:.4f})"
                ),
                details={
                    "z_score": round(z, 4),
                    "mean": round(mean, 6),
                    "std": round(std, 6),
                    "window_size": len(arr),
                    "threshold": self._anomaly_threshold,
                },
            ))

        return signals

    def buffer_sizes(self) -> dict[str, int]:
        """Diagnostic: return buffer fill levels for each tracked series."""
        return {f"{k[0]}:{k[1]}": len(v) for k, v in self._buffers.items()}
