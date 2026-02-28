"""
Alert Risk Scorer.

Converts a batch of AnomalySignals into a single ScoredAlert using the
weighted composite formula:

  risk_score = 0.4 * error_rate_anomaly_score
             + 0.3 * txn_pattern_anomaly_score
             + 0.2 * latency_anomaly_score
             + 0.1 * client_failure_spike_score

Each component score is the MAXIMUM confidence across all signals that
belong to that component's anomaly-type group. This gives:
  - A score of 0   → no signals of that type detected
  - A score of 1   → detector is maximally confident of that anomaly type
  - Weighted sum   → overall platform risk in [0.0, 1.0]

Severity thresholds (from constants.py):
  ≥ 0.85 → CRITICAL
  ≥ 0.65 → HIGH
  ≥ 0.40 → MEDIUM
  ≥ 0.20 → LOW
  <  0.20 → INFO
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Callable, Awaitable

from anomaly.base import severity_from_score
from common.constants import (
    SCORE_WEIGHT_CLIENT_SPIKE,
    SCORE_WEIGHT_ERROR_RATE,
    SCORE_WEIGHT_LATENCY,
    SCORE_WEIGHT_TXN_PATTERN,
)
from common.enums import AlertStatus, AnomalyType
from common.schemas import AnomalySignal, ScoredAlert

logger = logging.getLogger(__name__)

ScoredAlertHandler = Callable[[ScoredAlert], Awaitable[None]]

# ---------------------------------------------------------------------------
# AnomalyType → scoring component mapping
# ---------------------------------------------------------------------------

_ERROR_RATE_TYPES: frozenset[AnomalyType] = frozenset({
    AnomalyType.ERROR_RATE_SPIKE,
    AnomalyType.TECHNICAL_ERROR_SPIKE,
    AnomalyType.BUSINESS_ERROR_SPIKE,
})

_TXN_PATTERN_TYPES: frozenset[AnomalyType] = frozenset({
    AnomalyType.TPS_SPIKE,
    AnomalyType.TPS_DROP,
    AnomalyType.AMOUNT_ANOMALY,
    AnomalyType.CLIENT_BURST,
    AnomalyType.CLIENT_SILENCE,
    AnomalyType.MULTIVARIATE,        # IsolationForest captures combined patterns
})

_LATENCY_TYPES: frozenset[AnomalyType] = frozenset({
    AnomalyType.LATENCY_INCREASE,
})

_CLIENT_FAILURE_TYPES: frozenset[AnomalyType] = frozenset({
    AnomalyType.CLIENT_FAILURE_SPIKE,
    AnomalyType.GEO_DEVIATION,
})

# ---------------------------------------------------------------------------
# Alert title generation
# ---------------------------------------------------------------------------

_ANOMALY_TITLES: dict[AnomalyType, str] = {
    AnomalyType.ERROR_RATE_SPIKE:       "Error Rate Spike",
    AnomalyType.TECHNICAL_ERROR_SPIKE:  "Technical Error Spike",
    AnomalyType.BUSINESS_ERROR_SPIKE:   "Business Error Spike",
    AnomalyType.TPS_SPIKE:              "TPS Spike",
    AnomalyType.TPS_DROP:               "TPS Drop",
    AnomalyType.LATENCY_INCREASE:       "Latency Increase",
    AnomalyType.CLIENT_BURST:           "Client Burst Traffic",
    AnomalyType.CLIENT_SILENCE:         "Client Traffic Silence",
    AnomalyType.AMOUNT_ANOMALY:         "Abnormal Transaction Amount",
    AnomalyType.GEO_DEVIATION:          "Geographic Deviation",
    AnomalyType.CLIENT_FAILURE_SPIKE:   "Client Failure Spike",
    AnomalyType.MULTIVARIATE:           "Multivariate Anomaly",
}


def _build_title(signals: list[AnomalySignal], api_name: str | None) -> str:
    """
    Generate a human-readable alert title from the dominant signals.
    Uses the signal with highest confidence as the primary description.
    """
    if not signals:
        return "Anomaly Detected"

    top = max(signals, key=lambda s: s.confidence)
    type_label = _ANOMALY_TITLES.get(top.anomaly_type, top.anomaly_type.value)
    scope = api_name or top.merchant_id or "Platform"

    if len(signals) > 1:
        other_types = {
            _ANOMALY_TITLES.get(s.anomaly_type, s.anomaly_type.value)
            for s in signals
            if s.signal_id != top.signal_id
        }
        if other_types:
            return f"{type_label} on {scope} (+{', '.join(list(other_types)[:2])})"

    return f"{type_label} on {scope}"


def _build_description(
    signals: list[AnomalySignal],
    component_scores: dict[str, float],
    risk_score: float,
) -> str:
    lines = [
        f"risk_score={risk_score:.3f} | "
        f"error_rate={component_scores['error_rate']:.3f} "
        f"txn_pattern={component_scores['txn_pattern']:.3f} "
        f"latency={component_scores['latency']:.3f} "
        f"client_spike={component_scores['client_spike']:.3f}",
        "",
        f"Contributing signals ({len(signals)}):",
    ]
    for s in sorted(signals, key=lambda x: x.confidence, reverse=True)[:5]:
        lines.append(
            f"  [{s.detector.value}] {s.anomaly_type.value} "
            f"obs={s.observed_value:.4f} exp={s.expected_value:.4f} "
            f"score={s.deviation_score:.2f} conf={s.confidence:.2f}"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# RiskScorer
# ---------------------------------------------------------------------------

class RiskScorer:
    """
    Stateless scorer — converts a list of AnomalySignals into a ScoredAlert.

    Usage:
        scorer = RiskScorer()
        scorer.add_handler(alert_manager.handle)
        await scorer.handle(signals)   # called by AnomalyPipeline
    """

    def __init__(self) -> None:
        self._handlers: list[ScoredAlertHandler] = []

    def add_handler(self, handler: ScoredAlertHandler) -> None:
        self._handlers.append(handler)

    async def handle(self, signals: list[AnomalySignal]) -> None:
        """
        Called by AnomalyPipeline with a batch of signals from one detection run.
        Scores them and publishes the resulting ScoredAlert downstream.
        """
        if not signals:
            return

        try:
            alert = self.score(signals)
        except Exception:
            logger.exception(
                "RiskScorer.score() raised for %d signals", len(signals)
            )
            return

        logger.debug(
            "RiskScorer: risk=%.3f sev=%s api=%s merchant=%s signals=%d",
            alert.risk_score, alert.severity,
            alert.api_name, alert.merchant_id, len(signals),
        )

        for handler in self._handlers:
            try:
                await handler(alert)
            except Exception:
                logger.exception("RiskScorer handler %s raised", handler)

    def score(self, signals: list[AnomalySignal]) -> ScoredAlert:
        """
        Pure scoring function — deterministic given the same signals.
        Returns a ScoredAlert regardless of score (even INFO-level).
        """
        # ── Infer scope from signals ────────────────────────────────────────
        # Take the most common api_name and merchant_id across signals
        api_name = _most_common(s.api_name for s in signals if s.api_name)
        merchant_id = _most_common(s.merchant_id for s in signals if s.merchant_id)

        # ── Compute component scores ────────────────────────────────────────
        error_rate_score = _max_confidence(signals, _ERROR_RATE_TYPES)
        txn_pattern_score = _max_confidence(signals, _TXN_PATTERN_TYPES)
        latency_score = _max_confidence(signals, _LATENCY_TYPES)
        client_spike_score = _max_confidence(signals, _CLIENT_FAILURE_TYPES)

        # ── Weighted composite ──────────────────────────────────────────────
        risk_score = round(
            SCORE_WEIGHT_ERROR_RATE    * error_rate_score
            + SCORE_WEIGHT_TXN_PATTERN * txn_pattern_score
            + SCORE_WEIGHT_LATENCY     * latency_score
            + SCORE_WEIGHT_CLIENT_SPIKE * client_spike_score,
            6,
        )
        risk_score = max(0.0, min(1.0, risk_score))   # clamp to [0, 1]

        severity = severity_from_score(risk_score)

        component_scores = {
            "error_rate":   error_rate_score,
            "txn_pattern":  txn_pattern_score,
            "latency":      latency_score,
            "client_spike": client_spike_score,
        }

        return ScoredAlert(
            created_at=datetime.now(timezone.utc),
            api_name=api_name,
            merchant_id=merchant_id,
            error_rate_anomaly_score=round(error_rate_score, 6),
            txn_pattern_anomaly_score=round(txn_pattern_score, 6),
            latency_anomaly_score=round(latency_score, 6),
            client_failure_spike_score=round(client_spike_score, 6),
            risk_score=risk_score,
            severity=severity,
            signals=signals,
            status=AlertStatus.OPEN,
            title=_build_title(signals, api_name),
            description=_build_description(signals, component_scores, risk_score),
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _max_confidence(
    signals: list[AnomalySignal],
    anomaly_types: frozenset[AnomalyType],
) -> float:
    """Return the highest confidence among signals matching the given types."""
    matching = [s.confidence for s in signals if s.anomaly_type in anomaly_types]
    return max(matching) if matching else 0.0


def _most_common(values) -> str | None:
    """Return the most frequently occurring non-None value, or None."""
    from collections import Counter
    counts = Counter(v for v in values if v)
    return counts.most_common(1)[0][0] if counts else None


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------
risk_scorer = RiskScorer()
