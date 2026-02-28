"""
Unit tests for BusinessErrorClassifier.

Key scenarios:
  - HTTP 200 with a known domain error code in the body
  - HTTP 200 with a success code (should NOT be classified as error)
  - HTTP 200 with success_indicator=true (should NOT be classified as error)
  - HTTP 200 with no response body (should be treated as success)
  - HTTP 500 (should be skipped — business classifier only handles eligible codes)
  - Unknown domain code → UNKNOWN category, low confidence
  - API-specific code resolution
"""

from datetime import datetime, timezone

import pytest

from classification.business_classifier import BusinessErrorClassifier
from common.enums import BusinessErrorCategory, ErrorType, Severity
from common.schemas import LogEvent


def _event(api_name="UPI_COLLECT", http_status_code=200, response_body=None, **kwargs) -> LogEvent:
    defaults = dict(
        timestamp=datetime.now(timezone.utc),
        source="mock",
        api_name=api_name,
        api_path="/api/v1/upi/collect",
        http_status_code=http_status_code,
        response_time_ms=120.0,
        response_body=response_body,
    )
    defaults.update(kwargs)
    return LogEvent(**defaults)


@pytest.fixture
def classifier():
    return BusinessErrorClassifier()


class TestKnownErrorCodes:
    def test_insufficient_funds_rr_code(self, classifier):
        event = _event(response_body={"responseCode": "RR", "message": "Insufficient balance"})
        result = classifier.classify(event)
        assert result.error_type == ErrorType.BUSINESS
        assert result.business_error_category == BusinessErrorCategory.INSUFFICIENT_FUNDS
        assert result.domain_error_code == "RR"
        assert result.is_success is False
        assert result.severity == Severity.MEDIUM

    def test_otp_failure(self, classifier):
        event = _event(response_body={"responseCode": "OTP_INVALID", "message": "Invalid OTP"})
        result = classifier.classify(event)
        assert result.error_type == ErrorType.BUSINESS
        assert result.business_error_category == BusinessErrorCategory.OTP_FAILURE

    def test_limit_exceeded_u30(self, classifier):
        event = _event(response_body={"responseCode": "U30"})
        result = classifier.classify(event)
        assert result.error_type == ErrorType.BUSINESS
        assert result.business_error_category == BusinessErrorCategory.LIMIT_EXCEEDED

    def test_aml_rejection_critical_severity(self, classifier):
        event = _event(response_body={"errorCode": "AML001"})
        result = classifier.classify(event)
        assert result.error_type == ErrorType.BUSINESS
        assert result.business_error_category == BusinessErrorCategory.AML_REJECTION
        assert result.severity == Severity.CRITICAL

    def test_account_frozen_critical(self, classifier):
        event = _event(response_body={"responseCode": "U66"})
        result = classifier.classify(event)
        assert result.business_error_category == BusinessErrorCategory.ACCOUNT_FROZEN
        assert result.severity == Severity.CRITICAL

    def test_invalid_vpa(self, classifier):
        event = _event(response_body={"responseCode": "B1"})
        result = classifier.classify(event)
        assert result.business_error_category == BusinessErrorCategory.INVALID_VPA
        assert result.severity == Severity.LOW


class TestSuccessCodes:
    def test_success_code_00(self, classifier):
        event = _event(response_body={"responseCode": "00", "message": "Success"})
        result = classifier.classify(event)
        assert result.error_type == ErrorType.NONE
        assert result.is_success is True

    def test_success_code_SUCCESS(self, classifier):
        event = _event(response_body={"responseCode": "SUCCESS"})
        result = classifier.classify(event)
        assert result.error_type == ErrorType.NONE

    def test_success_indicator_true(self, classifier):
        event = _event(response_body={"responseCode": "RR", "success": True})
        result = classifier.classify(event)
        assert result.error_type == ErrorType.NONE

    def test_success_indicator_string_true(self, classifier):
        event = _event(response_body={"responseCode": "RR", "success": "true"})
        result = classifier.classify(event)
        assert result.error_type == ErrorType.NONE


class TestNoBodyOrIneligibleStatus:
    def test_no_response_body(self, classifier):
        event = _event(response_body=None)
        result = classifier.classify(event)
        assert result.error_type == ErrorType.NONE
        assert result.is_success is True

    def test_empty_response_body(self, classifier):
        event = _event(response_body={})
        result = classifier.classify(event)
        assert result.error_type == ErrorType.NONE

    def test_http_500_skipped(self, classifier):
        event = _event(http_status_code=500, response_body={"responseCode": "RR"})
        result = classifier.classify(event)
        assert result.error_type == ErrorType.NONE

    def test_no_error_code_field_in_body(self, classifier):
        event = _event(response_body={"data": "some payload without code field"})
        result = classifier.classify(event)
        assert result.error_type == ErrorType.NONE


class TestApiSpecificCodes:
    def test_imps_nbin002_insufficient_funds(self, classifier):
        event = _event(
            api_name="IMPS_TRANSFER",
            response_body={"txnStatus": "NBIN002"},
        )
        result = classifier.classify(event)
        assert result.error_type == ErrorType.BUSINESS
        assert result.business_error_category == BusinessErrorCategory.INSUFFICIENT_FUNDS

    def test_payment_gateway_decline(self, classifier):
        event = _event(
            api_name="PAYMENT_GATEWAY",
            response_body={"declineCode": "DECLINE"},
        )
        result = classifier.classify(event)
        assert result.error_type == ErrorType.BUSINESS
        assert result.business_error_category == BusinessErrorCategory.INSUFFICIENT_FUNDS

    def test_nested_body_path(self, classifier):
        event = _event(response_body={"data": {"errorCode": "AML001"}})
        result = classifier.classify(event)
        assert result.error_type == ErrorType.BUSINESS
        assert result.business_error_category == BusinessErrorCategory.AML_REJECTION


class TestUnknownCode:
    def test_unknown_code_still_classifies(self, classifier):
        event = _event(response_body={"responseCode": "ZZZ999"})
        result = classifier.classify(event)
        # Unknown code → UNKNOWN category but still a BUSINESS error (low confidence)
        assert result.error_type == ErrorType.BUSINESS
        assert result.business_error_category == BusinessErrorCategory.UNKNOWN
        assert result.classification_confidence < 1.0
