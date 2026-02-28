"""
Isolation Forest multivariate anomaly detector.

Unlike the univariate detectors (Z-score, EWMA, Percentile), the Isolation Forest
sees the FULL feature vector per MetricPoint and detects anomalies that only
manifest as a combination of signals — e.g. normal TPS but high latency AND
elevated error rate together.

Feature vector per API window:
  [tps, technical_error_rate, business_error_rate, p95_latency_ms, avg_txn_amount]

Lifecycle per API:
  1. Collect training data (min MIN_TRAINING_SAMPLES points)
  2. Train IsolationForest
  3. Score each new MetricPoint
  4. Retrain every ISOLATION_FOREST_RETRAIN_INTERVAL seconds with latest data

Model isolation:
  One model per api_name — avoids cross-API contamination.
  UPI_COLLECT and IMPS_TRANSFER have very different baseline behaviours.

Score interpretation:
  model.score_samples(X) returns the anomaly score:
    ~0.0  → average point
    < -0.3 → likely anomaly
  model.predict(X) returns: 1 = normal, -1 = anomaly
"""

from __future__ import annotations

import logging
import time
from datetime import timezone

import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from anomaly.base import MetricDetector, make_signal
from common.constants import (
    ISOLATION_FOREST_CONTAMINATION,
    ISOLATION_FOREST_N_ESTIMATORS,
    ISOLATION_FOREST_RETRAIN_INTERVAL,
    MIN_SAMPLES_FOR_BASELINE,
)
from common.enums import AnomalyType, DetectorName, MetricName
from common.schemas import AnomalySignal, MetricPoint

logger = logging.getLogger(__name__)

_MIN_TRAINING_SAMPLES = max(MIN_SAMPLES_FOR_BASELINE, 50)
_MAX_TRAINING_BUFFER = 500      # Keep last N points for retraining

# Feature names in order — used for explainability in signal details
_FEATURE_NAMES = [
    "tps",
    "technical_error_rate",
    "business_error_rate",
    "p95_latency_ms",
    "avg_txn_amount",
]


def _extract_features(metric: MetricPoint) -> list[float]:
    return [
        metric.tps,
        metric.technical_error_rate,
        metric.business_error_rate,
        metric.p95_latency_ms,
        metric.avg_txn_amount,
    ]


class IsolationForestDetector(MetricDetector):
    """
    Per-API Isolation Forest detector with periodic retraining.
    """

    def __init__(
        self,
        contamination: float = ISOLATION_FOREST_CONTAMINATION,
        n_estimators: int = ISOLATION_FOREST_N_ESTIMATORS,
        retrain_interval: int = ISOLATION_FOREST_RETRAIN_INTERVAL,
    ) -> None:
        self._contamination = contamination
        self._n_estimators = n_estimators
        self._retrain_interval = retrain_interval

        # Per-api state
        self._training_data: dict[str, list[list[float]]] = {}
        self._models: dict[str, IsolationForest] = {}
        self._scalers: dict[str, StandardScaler] = {}
        self._last_trained: dict[str, float] = {}

    def detect(self, metric: MetricPoint) -> list[AnomalySignal]:
        api = metric.api_name
        features = _extract_features(metric)
        ts = metric.window_start.replace(tzinfo=timezone.utc)

        # Accumulate training data
        if api not in self._training_data:
            self._training_data[api] = []
        self._training_data[api].append(features)

        # Cap buffer to avoid unbounded memory growth
        if len(self._training_data[api]) > _MAX_TRAINING_BUFFER:
            self._training_data[api] = self._training_data[api][-_MAX_TRAINING_BUFFER:]

        data = self._training_data[api]

        # Not enough data to train yet
        if len(data) < _MIN_TRAINING_SAMPLES:
            return []

        # Retrain if needed
        now = time.monotonic()
        last = self._last_trained.get(api, 0.0)
        if api not in self._models or (now - last) > self._retrain_interval:
            self._train(api)
            self._last_trained[api] = now

        model = self._models.get(api)
        scaler = self._scalers.get(api)
        if model is None or scaler is None:
            return []

        # Score current point
        try:
            X_raw = np.array([features], dtype=np.float64)
            X = scaler.transform(X_raw)
            prediction = model.predict(X)[0]          # 1=normal, -1=anomaly
            raw_score = model.score_samples(X)[0]     # more negative = more anomalous
        except Exception:
            logger.exception("IsolationForest scoring failed for api=%s", api)
            return []

        is_anomaly = prediction == -1

        # Normalise raw_score to a positive deviation score (0 = normal, higher = more anomalous)
        # IsolationForest scores typically range from ~-0.5 (anomaly) to ~0 (normal)
        deviation_score = max(0.0, -raw_score * 2)

        if not is_anomaly:
            return []

        # Find the most deviant feature for the description
        mean_features = np.mean(np.array(data[-100:]), axis=0)
        feature_deviations = [
            (abs(features[i] - mean_features[i]) / (abs(mean_features[i]) + 1e-10), _FEATURE_NAMES[i])
            for i in range(len(_FEATURE_NAMES))
        ]
        top_feature = max(feature_deviations, key=lambda x: x[0])

        return [make_signal(
            detector=DetectorName.ISOLATION_FOREST,
            anomaly_type=AnomalyType.MULTIVARIATE,
            metric_name=MetricName.TPS,     # Multivariate — TPS used as representative
            observed=metric.tps,
            expected=float(mean_features[0]),
            deviation_score=deviation_score,
            is_anomaly=True,
            detected_at=ts,
            api_name=api,
            description=(
                f"[IsolationForest] {api} multivariate anomaly detected. "
                f"Most deviant feature: {top_feature[1]} "
                f"(score={raw_score:.4f})"
            ),
            details={
                "raw_score": round(raw_score, 6),
                "features": dict(zip(_FEATURE_NAMES, [round(f, 4) for f in features])),
                "mean_features": dict(zip(_FEATURE_NAMES, [round(float(m), 4) for m in mean_features])),
                "top_deviant_feature": top_feature[1],
                "training_samples": len(data),
            },
        )]

    def _train(self, api: str) -> None:
        data = self._training_data[api]
        X = np.array(data, dtype=np.float64)

        # Fit scaler on training data
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        model = IsolationForest(
            contamination=self._contamination,
            n_estimators=self._n_estimators,
            random_state=42,
            n_jobs=1,
        )
        model.fit(X_scaled)

        self._scalers[api] = scaler
        self._models[api] = model
        logger.info(
            "IsolationForest retrained for api=%s on %d samples",
            api, len(data),
        )

    def model_status(self) -> dict[str, dict]:
        """Diagnostic: return model status per API."""
        return {
            api: {
                "trained": api in self._models,
                "training_samples": len(self._training_data.get(api, [])),
                "last_trained_ago_s": round(time.monotonic() - self._last_trained.get(api, 0)),
            }
            for api in self._training_data
        }
