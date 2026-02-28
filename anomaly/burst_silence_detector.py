"""
Client-level burst, silence, failure spike, and geographic anomaly detector.

Operates on ClientSnapshot objects (not MetricPoints).
Each snapshot already contains pre-computed burst/silence flags from the
ClientProfiler's EWMA baseline — this detector translates those flags
into AnomalySignal objects and adds further checks:

  1. Burst traffic         — txn_frequency_per_min >> baseline
  2. Silence / dead client — txn_frequency_per_min << baseline
  3. Client failure spike  — failure_ratio above warn/critical threshold
  4. Amount anomaly        — avg_txn_amount deviates significantly from
                             the client's rolling baseline
  5. Geographic deviation  — new countries seen, or entropy spike

The amount anomaly uses a per-merchant EWMA baseline maintained here
(seeded from ClientSnapshot history as snapshots arrive).
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import timezone

from anomaly.base import ClientDetector, make_signal
from common.constants import (
    CLIENT_FAILURE_RATIO_CRITICAL,
    CLIENT_FAILURE_RATIO_WARN,
    CLIENT_GEO_ENTROPY_SPIKE,
)
from common.enums import AnomalyType, DetectorName, MetricName, Severity
from common.schemas import AnomalySignal, ClientSnapshot

logger = logging.getLogger(__name__)

_AMOUNT_EWMA_ALPHA = 0.15       # Smoothing factor for amount baseline
_AMOUNT_ANOMALY_MULTIPLIER = 3.0   # avg_amount > 3x baseline → anomaly
_MIN_SNAPSHOTS_FOR_AMOUNT_BASELINE = 10


@dataclass
class _MerchantBaseline:
    amount_ewma: float
    amount_ewma_var: float
    snapshot_count: int = 0


class BurstSilenceDetector(ClientDetector):
    """
    Detects client-level behavioural anomalies from ClientSnapshot objects.
    Maintains per-merchant amount baselines internally.
    """

    def __init__(self) -> None:
        # merchant_id → baseline state
        self._baselines: dict[str, _MerchantBaseline] = {}

    def detect(self, snapshot: ClientSnapshot) -> list[AnomalySignal]:
        signals: list[AnomalySignal] = []
        ts = snapshot.window_start.replace(tzinfo=timezone.utc)
        merchant = snapshot.merchant_id

        # ── 1. Burst traffic ─────────────────────────────────────────────────
        if snapshot.burst_detected:
            signals.append(make_signal(
                detector=DetectorName.BURST_SILENCE,
                anomaly_type=AnomalyType.CLIENT_BURST,
                metric_name=MetricName.TXN_VOLUME,
                observed=snapshot.txn_frequency_per_min,
                expected=snapshot.txn_frequency_per_min,   # baseline unknown here
                deviation_score=3.0,                        # burst flag = definite anomaly
                is_anomaly=True,
                detected_at=ts,
                merchant_id=merchant,
                description=(
                    f"[Burst] Merchant {merchant} burst detected: "
                    f"{snapshot.txn_frequency_per_min:.1f} txn/min"
                ),
                details={
                    "txn_frequency_per_min": snapshot.txn_frequency_per_min,
                    "txn_volume": snapshot.txn_volume,
                    "window_seconds": snapshot.window_seconds,
                },
            ))

        # ── 2. Silence / dead client ─────────────────────────────────────────
        if snapshot.silence_detected:
            signals.append(make_signal(
                detector=DetectorName.BURST_SILENCE,
                anomaly_type=AnomalyType.CLIENT_SILENCE,
                metric_name=MetricName.TXN_VOLUME,
                observed=snapshot.txn_frequency_per_min,
                expected=snapshot.txn_frequency_per_min,
                deviation_score=3.0,
                is_anomaly=True,
                detected_at=ts,
                merchant_id=merchant,
                description=(
                    f"[Silence] Merchant {merchant} traffic went silent: "
                    f"{snapshot.txn_frequency_per_min:.2f} txn/min"
                ),
                details={
                    "txn_frequency_per_min": snapshot.txn_frequency_per_min,
                    "txn_volume": snapshot.txn_volume,
                },
            ))

        # ── 3. Failure spike ─────────────────────────────────────────────────
        ratio = snapshot.failure_ratio
        if ratio >= CLIENT_FAILURE_RATIO_CRITICAL:
            signals.append(make_signal(
                detector=DetectorName.BURST_SILENCE,
                anomaly_type=AnomalyType.CLIENT_FAILURE_SPIKE,
                metric_name=MetricName.FAILURE_RATIO,
                observed=ratio,
                expected=CLIENT_FAILURE_RATIO_WARN,
                deviation_score=(ratio - CLIENT_FAILURE_RATIO_WARN) / max(CLIENT_FAILURE_RATIO_WARN, 0.01),
                is_anomaly=True,
                detected_at=ts,
                merchant_id=merchant,
                description=(
                    f"[FailureSpike] Merchant {merchant} failure ratio CRITICAL: "
                    f"{ratio:.1%} (threshold={CLIENT_FAILURE_RATIO_CRITICAL:.0%})"
                ),
                details={
                    "failure_ratio": round(ratio, 4),
                    "txn_volume": snapshot.txn_volume,
                    "threshold_critical": CLIENT_FAILURE_RATIO_CRITICAL,
                },
            ))
        elif ratio >= CLIENT_FAILURE_RATIO_WARN:
            signals.append(make_signal(
                detector=DetectorName.BURST_SILENCE,
                anomaly_type=AnomalyType.CLIENT_FAILURE_SPIKE,
                metric_name=MetricName.FAILURE_RATIO,
                observed=ratio,
                expected=CLIENT_FAILURE_RATIO_WARN,
                deviation_score=(ratio - CLIENT_FAILURE_RATIO_WARN) / max(CLIENT_FAILURE_RATIO_WARN, 0.01),
                is_anomaly=True,
                detected_at=ts,
                merchant_id=merchant,
                description=(
                    f"[FailureSpike] Merchant {merchant} failure ratio WARNING: "
                    f"{ratio:.1%}"
                ),
                details={"failure_ratio": round(ratio, 4)},
            ))

        # ── 4. Amount anomaly ─────────────────────────────────────────────────
        amount_signal = self._detect_amount_anomaly(snapshot, ts)
        if amount_signal:
            signals.append(amount_signal)

        # ── 5. Geographic deviation ───────────────────────────────────────────
        geo_signal = self._detect_geo_anomaly(snapshot, ts)
        if geo_signal:
            signals.append(geo_signal)

        # Update baselines AFTER scoring
        self._update_baseline(snapshot)

        return signals

    # ------------------------------------------------------------------
    # Amount anomaly
    # ------------------------------------------------------------------

    def _detect_amount_anomaly(
        self, snapshot: ClientSnapshot, ts
    ) -> AnomalySignal | None:
        if snapshot.avg_txn_amount <= 0:
            return None

        merchant = snapshot.merchant_id
        baseline = self._baselines.get(merchant)

        if baseline is None or baseline.snapshot_count < _MIN_SNAPSHOTS_FOR_AMOUNT_BASELINE:
            return None

        expected = baseline.amount_ewma
        ewma_std = math.sqrt(baseline.amount_ewma_var) if baseline.amount_ewma_var > 0 else 0.0

        if ewma_std < 1.0 or expected < 1.0:
            return None

        deviation = abs(snapshot.avg_txn_amount - expected)
        if deviation < _AMOUNT_ANOMALY_MULTIPLIER * ewma_std:
            return None

        deviation_score = deviation / ewma_std
        return make_signal(
            detector=DetectorName.BURST_SILENCE,
            anomaly_type=AnomalyType.AMOUNT_ANOMALY,
            metric_name=MetricName.AVG_TXN_AMOUNT,
            observed=snapshot.avg_txn_amount,
            expected=expected,
            deviation_score=deviation_score,
            is_anomaly=True,
            detected_at=ts,
            merchant_id=merchant,
            description=(
                f"[AmountAnomaly] Merchant {merchant} avg amount "
                f"₹{snapshot.avg_txn_amount:.0f} vs baseline ₹{expected:.0f} "
                f"({deviation_score:.1f}σ)"
            ),
            details={
                "avg_txn_amount": round(snapshot.avg_txn_amount, 2),
                "baseline_mean": round(expected, 2),
                "baseline_std": round(ewma_std, 2),
                "total_txn_amount": snapshot.total_txn_amount,
            },
        )

    # ------------------------------------------------------------------
    # Geo anomaly
    # ------------------------------------------------------------------

    def _detect_geo_anomaly(
        self, snapshot: ClientSnapshot, ts
    ) -> AnomalySignal | None:
        baseline = self._baselines.get(snapshot.merchant_id)
        if baseline is None or baseline.snapshot_count < _MIN_SNAPSHOTS_FOR_AMOUNT_BASELINE:
            return None

        # Use geo_entropy as the signal: sudden spike = new/diverse locations
        if snapshot.geo_entropy < CLIENT_GEO_ENTROPY_SPIKE:
            return None

        return make_signal(
            detector=DetectorName.BURST_SILENCE,
            anomaly_type=AnomalyType.GEO_DEVIATION,
            metric_name=MetricName.TXN_VOLUME,
            observed=snapshot.geo_entropy,
            expected=0.0,
            deviation_score=snapshot.geo_entropy,
            is_anomaly=True,
            detected_at=ts,
            merchant_id=snapshot.merchant_id,
            description=(
                f"[GeoDeviation] Merchant {snapshot.merchant_id} unusual geo spread: "
                f"entropy={snapshot.geo_entropy:.2f}, "
                f"countries={snapshot.unique_countries}"
            ),
            details={
                "geo_entropy": snapshot.geo_entropy,
                "unique_countries": snapshot.unique_countries,
                "unique_cities": snapshot.unique_cities,
                "country_count": len(snapshot.unique_countries),
            },
        )

    # ------------------------------------------------------------------
    # Baseline update
    # ------------------------------------------------------------------

    def _update_baseline(self, snapshot: ClientSnapshot) -> None:
        merchant = snapshot.merchant_id
        amount = snapshot.avg_txn_amount

        if amount <= 0:
            return

        if merchant not in self._baselines:
            self._baselines[merchant] = _MerchantBaseline(
                amount_ewma=amount,
                amount_ewma_var=0.0,
                snapshot_count=1,
            )
            return

        b = self._baselines[merchant]
        alpha = _AMOUNT_EWMA_ALPHA
        delta = amount - b.amount_ewma
        b.amount_ewma_var = (1 - alpha) * (b.amount_ewma_var + alpha * delta ** 2)
        b.amount_ewma = alpha * amount + (1 - alpha) * b.amount_ewma
        b.snapshot_count += 1

    def merchant_count(self) -> int:
        return len(self._baselines)
