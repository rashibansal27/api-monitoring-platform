"""
Unit tests for TechnicalErrorClassifier.
"""

from datetime import datetime, timezone

import pytest

from classification.technical_classifier import TechnicalErrorClassifier
from common.enums import ErrorType, Severity, TechnicalErrorCategory
from common.schemas import LogEvent


def _event(**kwargs) -> LogEvent:
    defaults = dict(
        timestamp=datetime.now(timezone.utc),
        source="mock",
        api_name="UPI_COLLECT",
        api_path="/api/v1/upi/collect",
        http_status_code=200,
        response_time_ms=120.0,
    )
    defaults.update(kwargs)
    return LogEvent(**defaults)


@pytest.fixture
def classifier():
    return TechnicalErrorClassifier()


class TestStatusCodeRules:
    def test_502_bad_gateway(self, classifier):
        result = classifier.classify(_event(http_status_code=502))
        assert result.error_type == ErrorType.TECHNICAL
        assert result.technical_error_category == TechnicalErrorCategory.UPSTREAM_FAILURE
        assert result.severity == Severity.HIGH
        assert not result.is_success

    def test_503_service_unavailable(self, classifier):
        result = classifier.classify(_event(http_status_code=503))
        assert result.error_type == ErrorType.TECHNICAL
        assert result.technical_error_category == TechnicalErrorCategory.INFRASTRUCTURE

    def test_504_gateway_timeout(self, classifier):
        result = classifier.classify(_event(http_status_code=504))
        assert result.error_type == ErrorType.TECHNICAL
        assert result.technical_error_category == TechnicalErrorCategory.TIMEOUT

    def test_500_internal_server_error(self, classifier):
        result = classifier.classify(_event(http_status_code=500))
        assert result.error_type == ErrorType.TECHNICAL
        assert result.technical_error_category == TechnicalErrorCategory.HTTP_5XX
        assert result.severity == Severity.HIGH

    def test_408_request_timeout(self, classifier):
        result = classifier.classify(_event(http_status_code=408))
        assert result.error_type == ErrorType.TECHNICAL
        assert result.technical_error_category == TechnicalErrorCategory.TIMEOUT

    def test_unlisted_5xx_range_fallback(self, classifier):
        result = classifier.classify(_event(http_status_code=511))
        assert result.error_type == ErrorType.TECHNICAL
        assert result.technical_error_category == TechnicalErrorCategory.HTTP_5XX

    def test_200_not_technical(self, classifier):
        result = classifier.classify(_event(http_status_code=200))
        assert result.error_type == ErrorType.NONE
        assert result.is_success is True

    def test_404_not_technical(self, classifier):
        result = classifier.classify(_event(http_status_code=404))
        assert result.error_type == ErrorType.NONE


class TestLatencyTimeout:
    def test_30s_latency_is_critical_timeout(self, classifier):
        result = classifier.classify(_event(http_status_code=200, response_time_ms=31_000))
        assert result.error_type == ErrorType.TECHNICAL
        assert result.technical_error_category == TechnicalErrorCategory.TIMEOUT
        assert result.severity == Severity.CRITICAL

    def test_10s_latency_is_high_timeout(self, classifier):
        result = classifier.classify(_event(http_status_code=200, response_time_ms=11_000))
        assert result.error_type == ErrorType.TECHNICAL
        assert result.severity == Severity.HIGH

    def test_5s_latency_is_medium_timeout(self, classifier):
        result = classifier.classify(_event(http_status_code=200, response_time_ms=6_000))
        assert result.error_type == ErrorType.TECHNICAL
        assert result.severity == Severity.MEDIUM

    def test_fast_response_not_timeout(self, classifier):
        result = classifier.classify(_event(http_status_code=200, response_time_ms=150))
        assert result.error_type == ErrorType.NONE


class TestErrorMessagePatterns:
    def test_db_connection_error(self, classifier):
        result = classifier.classify(
            _event(http_status_code=200, error_message="database connection failed: timeout")
        )
        assert result.error_type == ErrorType.TECHNICAL
        assert result.technical_error_category == TechnicalErrorCategory.DATABASE

    def test_connection_refused(self, classifier):
        result = classifier.classify(
            _event(http_status_code=200, error_message="ECONNREFUSED 127.0.0.1:5432")
        )
        assert result.error_type == ErrorType.TECHNICAL
        assert result.technical_error_category == TechnicalErrorCategory.CONNECTION_REFUSED

    def test_circuit_breaker(self, classifier):
        result = classifier.classify(
            _event(http_status_code=200, error_message="circuit breaker is OPEN")
        )
        assert result.error_type == ErrorType.TECHNICAL
        assert result.technical_error_category == TechnicalErrorCategory.CIRCUIT_BREAKER

    def test_oom(self, classifier):
        result = classifier.classify(
            _event(http_status_code=200, error_message="java.lang.OutOfMemoryError: Java heap space")
        )
        assert result.error_type == ErrorType.TECHNICAL
        assert result.severity == Severity.CRITICAL

    def test_no_error_message(self, classifier):
        result = classifier.classify(_event(http_status_code=200, error_message=None))
        assert result.error_type == ErrorType.NONE


class TestClassifierRobustness:
    def test_does_not_raise_on_malformed_event(self, classifier):
        # Force an internal processing error to verify the except handler
        # catches it and returns a safe fallback ClassifiedEvent.
        from unittest.mock import patch
        with patch.object(classifier, "_classify", side_effect=RuntimeError("injected error")):
            result = classifier.classify(_event())
        assert result is not None
        assert result.classification_notes == "classifier_error"
