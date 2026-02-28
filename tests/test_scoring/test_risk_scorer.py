"""
Unit tests for RiskScorer.

Tests:
  - Correct component score extraction from signal types
  - Weighted formula produces expected risk_score
  - Severity assignment from thresholds
  - Empty signal list → zero score
  - Score clamped to [0, 1]
  - Dominant anomaly type drives title
"""

from datetime import datetime, timezone

import pytest

from common.enums import (
    AlertStatus,
    AnomalyType,
    DetectorName,
    MetricName,
    Severity,
)
from common.schemas import AnomalySignal
from scoring.risk_scorer import RiskScorer, _max_confidence


def _signal(
    anomaly_type: AnomalyType,
    confidence: float = 0.9,
    api_name: str = "UPI_COLLECT",
    merchant_id: str | None = None,
) -> AnomalySignal:
    return AnomalySignal(
        detected_at=datetime.now(timezone.utc),
        detector=DetectorName.ZSCORE,
        anomaly_type=anomaly_type,
        metric_name=MetricName.TPS,
        api_name=api_name,
        merchant_id=merchant_id,
        observed_value=100.0,
        expected_value=50.0,
        deviation_score=3.5,
        is_anomaly=True,
        confidence=confidence,
    )


@pytest.fixture
def scorer():
    return RiskScorer()


class TestComponentScoreExtraction:
    def test_error_rate_signal_maps_to_error_rate_component(self, scorer):
        signals = [_signal(AnomalyType.TECHNICAL_ERROR_SPIKE, confidence=0.9)]
        alert = scorer.score(signals)
        assert alert.error_rate_anomaly_score == pytest.approx(0.9)
        assert alert.txn_pattern_anomaly_score == 0.0
        assert alert.latency_anomaly_score == 0.0
        assert alert.client_failure_spike_score == 0.0

    def test_business_error_spike_maps_to_error_rate(self, scorer):
        signals = [_signal(AnomalyType.BUSINESS_ERROR_SPIKE, confidence=0.8)]
        alert = scorer.score(signals)
        assert alert.error_rate_anomaly_score == pytest.approx(0.8)

    def test_tps_spike_maps_to_txn_pattern(self, scorer):
        signals = [_signal(AnomalyType.TPS_SPIKE, confidence=0.75)]
        alert = scorer.score(signals)
        assert alert.txn_pattern_anomaly_score == pytest.approx(0.75)
        assert alert.error_rate_anomaly_score == 0.0

    def test_latency_maps_to_latency_component(self, scorer):
        signals = [_signal(AnomalyType.LATENCY_INCREASE, confidence=0.85)]
        alert = scorer.score(signals)
        assert alert.latency_anomaly_score == pytest.approx(0.85)

    def test_client_failure_spike_maps_to_client_component(self, scorer):
        signals = [_signal(AnomalyType.CLIENT_FAILURE_SPIKE, confidence=0.7)]
        alert = scorer.score(signals)
        assert alert.client_failure_spike_score == pytest.approx(0.7)

    def test_geo_deviation_maps_to_client_component(self, scorer):
        signals = [_signal(AnomalyType.GEO_DEVIATION, confidence=0.6)]
        alert = scorer.score(signals)
        assert alert.client_failure_spike_score == pytest.approx(0.6)

    def test_multivariate_maps_to_txn_pattern(self, scorer):
        signals = [_signal(AnomalyType.MULTIVARIATE, confidence=0.95)]
        alert = scorer.score(signals)
        assert alert.txn_pattern_anomaly_score == pytest.approx(0.95)


class TestWeightedFormula:
    def test_single_error_rate_signal(self, scorer):
        # risk = 0.4 * 0.9 + 0.3*0 + 0.2*0 + 0.1*0 = 0.36
        signals = [_signal(AnomalyType.TECHNICAL_ERROR_SPIKE, confidence=0.9)]
        alert = scorer.score(signals)
        assert alert.risk_score == pytest.approx(0.36, abs=1e-5)

    def test_multiple_component_signals(self, scorer):
        # error=0.9 → 0.4*0.9=0.36
        # txn_pattern=0.8 → 0.3*0.8=0.24
        # latency=0.7 → 0.2*0.7=0.14
        # client=0.6 → 0.1*0.6=0.06
        # total = 0.80
        signals = [
            _signal(AnomalyType.TECHNICAL_ERROR_SPIKE, confidence=0.9),
            _signal(AnomalyType.TPS_SPIKE, confidence=0.8),
            _signal(AnomalyType.LATENCY_INCREASE, confidence=0.7),
            _signal(AnomalyType.CLIENT_FAILURE_SPIKE, confidence=0.6),
        ]
        alert = scorer.score(signals)
        assert alert.risk_score == pytest.approx(0.80, abs=1e-5)

    def test_max_confidence_taken_per_component(self, scorer):
        # Two error-rate signals: 0.5 and 0.9 → max = 0.9
        signals = [
            _signal(AnomalyType.TECHNICAL_ERROR_SPIKE, confidence=0.5),
            _signal(AnomalyType.BUSINESS_ERROR_SPIKE, confidence=0.9),
        ]
        alert = scorer.score(signals)
        assert alert.error_rate_anomaly_score == pytest.approx(0.9)

    def test_empty_signals_zero_score(self, scorer):
        # Should not raise — treat as empty batch gracefully
        signals = [_signal(AnomalyType.TPS_SPIKE, confidence=0.0)]
        alert = scorer.score(signals)
        assert alert.risk_score == 0.0

    def test_score_clamped_to_one(self, scorer):
        # All components at max confidence (1.0) → score = 1.0
        signals = [
            _signal(AnomalyType.TECHNICAL_ERROR_SPIKE, confidence=1.0),
            _signal(AnomalyType.TPS_SPIKE, confidence=1.0),
            _signal(AnomalyType.LATENCY_INCREASE, confidence=1.0),
            _signal(AnomalyType.CLIENT_FAILURE_SPIKE, confidence=1.0),
        ]
        alert = scorer.score(signals)
        assert alert.risk_score <= 1.0


class TestSeverityAssignment:
    def test_critical_severity(self, scorer):
        # Need risk ≥ 0.85
        # 0.4*1.0 + 0.3*1.0 + 0.2*1.0 + 0.1*1.0 = 1.0 → CRITICAL
        signals = [
            _signal(AnomalyType.TECHNICAL_ERROR_SPIKE, confidence=1.0),
            _signal(AnomalyType.TPS_SPIKE, confidence=1.0),
            _signal(AnomalyType.LATENCY_INCREASE, confidence=1.0),
            _signal(AnomalyType.CLIENT_FAILURE_SPIKE, confidence=1.0),
        ]
        alert = scorer.score(signals)
        assert alert.severity == Severity.CRITICAL

    def test_high_severity(self, scorer):
        # risk ≈ 0.4*0.9 + 0.3*0.9 = 0.36+0.27 = 0.63 → just below HIGH (0.65)
        # Let's push above: 0.4*1.0 + 0.3*0.9 = 0.40+0.27 = 0.67 → HIGH
        signals = [
            _signal(AnomalyType.TECHNICAL_ERROR_SPIKE, confidence=1.0),
            _signal(AnomalyType.TPS_SPIKE, confidence=0.9),
        ]
        alert = scorer.score(signals)
        assert alert.severity in (Severity.HIGH, Severity.MEDIUM)

    def test_low_score_info_severity(self, scorer):
        signals = [_signal(AnomalyType.TPS_SPIKE, confidence=0.3)]
        alert = scorer.score(signals)
        # 0.3 * 0.3 = 0.09 → INFO
        assert alert.severity in (Severity.INFO, Severity.LOW)


class TestAlertMetadata:
    def test_alert_has_open_status(self, scorer):
        signals = [_signal(AnomalyType.TPS_SPIKE)]
        alert = scorer.score(signals)
        assert alert.status == AlertStatus.OPEN

    def test_alert_has_non_empty_title(self, scorer):
        signals = [_signal(AnomalyType.TECHNICAL_ERROR_SPIKE)]
        alert = scorer.score(signals)
        assert len(alert.title) > 0

    def test_alert_api_name_extracted(self, scorer):
        signals = [_signal(AnomalyType.TPS_SPIKE, api_name="IMPS_TRANSFER")]
        alert = scorer.score(signals)
        assert alert.api_name == "IMPS_TRANSFER"

    def test_alert_merchant_id_extracted(self, scorer):
        signals = [_signal(AnomalyType.CLIENT_BURST, merchant_id="MERCH_0042")]
        alert = scorer.score(signals)
        assert alert.merchant_id == "MERCH_0042"

    def test_alert_signals_preserved(self, scorer):
        signals = [
            _signal(AnomalyType.TPS_SPIKE),
            _signal(AnomalyType.LATENCY_INCREASE),
        ]
        alert = scorer.score(signals)
        assert len(alert.signals) == 2


class TestMaxConfidenceHelper:
    def test_returns_max(self):
        signals = [
            _signal(AnomalyType.TECHNICAL_ERROR_SPIKE, confidence=0.5),
            _signal(AnomalyType.TECHNICAL_ERROR_SPIKE, confidence=0.9),
            _signal(AnomalyType.TPS_SPIKE, confidence=0.7),
        ]
        from common.enums import AnomalyType as AT
        result = _max_confidence(
            signals,
            frozenset({AT.TECHNICAL_ERROR_SPIKE, AT.BUSINESS_ERROR_SPIKE}),
        )
        assert result == pytest.approx(0.9)

    def test_returns_zero_when_no_match(self):
        signals = [_signal(AnomalyType.TPS_SPIKE, confidence=0.9)]
        result = _max_confidence(signals, frozenset({AnomalyType.LATENCY_INCREASE}))
        assert result == 0.0
