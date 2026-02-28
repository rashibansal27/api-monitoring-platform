"""
Business error rule tables.

Defines:
  - Which HTTP status codes CAN carry a business error in the body
  - Severity mapping: BusinessErrorCategory → Severity
  - Domain error codes that signal a TRUE success (don't misclassify)
  - Error codes that should always be flagged regardless of HTTP status

Business errors are domain-level — they travel inside HTTP 200 responses.
The actual code-to-category mapping lives in config/error_codes.py.
This file controls the CLASSIFICATION POLICY around those codes.
"""

from common.enums import BusinessErrorCategory, Severity

# ---------------------------------------------------------------------------
# HTTP status codes that may carry a business error in the response body.
# Only events with these codes are passed to BusinessErrorClassifier.
# ---------------------------------------------------------------------------
BUSINESS_ERROR_ELIGIBLE_STATUS_CODES: frozenset[int] = frozenset({
    200,    # Most common — success envelope with error payload inside
    201,    # Created — occasionally misused for partial success
    202,    # Accepted — async flows that later fail
    400,    # Some APIs return 400 for business validation failures
    422,    # Unprocessable entity — business validation
})

# ---------------------------------------------------------------------------
# Domain error codes that represent TRUE SUCCESS.
# If the response body contains one of these, skip business error detection.
# ---------------------------------------------------------------------------
SUCCESS_DOMAIN_CODES: frozenset[str] = frozenset({
    "00",       # NPCI/IMPS success
    "000",
    "0000",
    "SUCCESS",
    "00000000",
    "APPROVED",
    "ACCEPT",
    "00 ",      # Trailing space variant seen in some gateways
})

# ---------------------------------------------------------------------------
# BusinessErrorCategory → Severity
# Severity reflects the IMPACT on the user/business, not the system.
# ---------------------------------------------------------------------------
BUSINESS_ERROR_SEVERITY: dict[BusinessErrorCategory, Severity] = {
    # High customer impact — transaction hard fails
    BusinessErrorCategory.AML_REJECTION:            Severity.CRITICAL,
    BusinessErrorCategory.ACCOUNT_FROZEN:           Severity.CRITICAL,
    BusinessErrorCategory.DEBIT_FREEZE:             Severity.CRITICAL,
    BusinessErrorCategory.CREDIT_FREEZE:            Severity.CRITICAL,
    BusinessErrorCategory.RISK_DECLINED:            Severity.CRITICAL,
    BusinessErrorCategory.TRANSACTION_NOT_PERMITTED:Severity.HIGH,
    BusinessErrorCategory.BENEFICIARY_BANK_DOWN:    Severity.HIGH,

    # Medium impact — user can retry or fix
    BusinessErrorCategory.INSUFFICIENT_FUNDS:       Severity.MEDIUM,
    BusinessErrorCategory.LIMIT_EXCEEDED:           Severity.MEDIUM,
    BusinessErrorCategory.OTP_FAILURE:              Severity.MEDIUM,
    BusinessErrorCategory.DUPLICATE_TRANSACTION:    Severity.MEDIUM,
    BusinessErrorCategory.EXPIRED_TOKEN:            Severity.MEDIUM,

    # Lower impact — data / config issue
    BusinessErrorCategory.INVALID_ACCOUNT:         Severity.LOW,
    BusinessErrorCategory.INVALID_VPA:             Severity.LOW,
    BusinessErrorCategory.INVALID_IFSC:            Severity.LOW,

    # Unknown — treat conservatively
    BusinessErrorCategory.UNKNOWN:                 Severity.LOW,
}

# ---------------------------------------------------------------------------
# Categories that should trigger an AML / fraud alert regardless of volume.
# The scoring engine gives these special weight.
# ---------------------------------------------------------------------------
HIGH_RISK_BUSINESS_CATEGORIES: frozenset[BusinessErrorCategory] = frozenset({
    BusinessErrorCategory.AML_REJECTION,
    BusinessErrorCategory.RISK_DECLINED,
    BusinessErrorCategory.ACCOUNT_FROZEN,
    BusinessErrorCategory.DEBIT_FREEZE,
    BusinessErrorCategory.CREDIT_FREEZE,
})

# ---------------------------------------------------------------------------
# Categories that are purely informational (user error, not platform issue).
# Do NOT count these toward the business-error-rate anomaly signal.
# ---------------------------------------------------------------------------
INFORMATIONAL_BUSINESS_CATEGORIES: frozenset[BusinessErrorCategory] = frozenset({
    BusinessErrorCategory.INVALID_ACCOUNT,
    BusinessErrorCategory.INVALID_VPA,
    BusinessErrorCategory.INVALID_IFSC,
    BusinessErrorCategory.DUPLICATE_TRANSACTION,
})

# ---------------------------------------------------------------------------
# Minimum confidence to accept a business classification.
# Below this threshold, mark as UNKNOWN rather than a false positive.
# ---------------------------------------------------------------------------
MIN_BUSINESS_CLASSIFICATION_CONFIDENCE: float = 0.70
