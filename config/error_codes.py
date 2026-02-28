"""
Business error code registry.

Banks return domain-specific error codes inside HTTP 200 response bodies.
This module maps those codes → BusinessErrorCategory for classification.

Structure:
  ERROR_CODE_REGISTRY   — flat code → category map (universal codes)
  API_ERROR_CODE_MAP    — api_name → {code → category} (API-specific overrides)
  RESPONSE_BODY_PATHS   — api_name → list of JSON paths to inspect for error codes

Adding a new bank/API:
  1. Add its response body field paths to RESPONSE_BODY_PATHS
  2. Add its domain error codes to API_ERROR_CODE_MAP
  3. Codes not found in API map fall back to ERROR_CODE_REGISTRY
"""

from common.enums import BusinessErrorCategory

# ---------------------------------------------------------------------------
# Universal / NPCI standard codes (UPI, IMPS, NEFT, RTGS)
# ---------------------------------------------------------------------------
ERROR_CODE_REGISTRY: dict[str, BusinessErrorCategory] = {
    # NPCI UPI response codes
    "RR": BusinessErrorCategory.INSUFFICIENT_FUNDS,
    "U30": BusinessErrorCategory.LIMIT_EXCEEDED,
    "U16": BusinessErrorCategory.INVALID_ACCOUNT,
    "U13": BusinessErrorCategory.INVALID_ACCOUNT,
    "U28": BusinessErrorCategory.LIMIT_EXCEEDED,
    "U66": BusinessErrorCategory.ACCOUNT_FROZEN,
    "U67": BusinessErrorCategory.DEBIT_FREEZE,
    "U68": BusinessErrorCategory.CREDIT_FREEZE,
    "U69": BusinessErrorCategory.ACCOUNT_FROZEN,
    "B1": BusinessErrorCategory.INVALID_VPA,
    "XB": BusinessErrorCategory.BENEFICIARY_BANK_DOWN,
    "XD": BusinessErrorCategory.DUPLICATE_TRANSACTION,
    "XT": BusinessErrorCategory.TRANSACTION_NOT_PERMITTED,
    "AM": BusinessErrorCategory.LIMIT_EXCEEDED,
    "AM04": BusinessErrorCategory.INSUFFICIENT_FUNDS,
    "AM12": BusinessErrorCategory.INVALID_ACCOUNT,
    "AC01": BusinessErrorCategory.INVALID_ACCOUNT,
    "AC06": BusinessErrorCategory.ACCOUNT_FROZEN,

    # OTP / authentication
    "OTP_EXPIRED": BusinessErrorCategory.OTP_FAILURE,
    "OTP_INVALID": BusinessErrorCategory.OTP_FAILURE,
    "OTP_MISMATCH": BusinessErrorCategory.OTP_FAILURE,
    "AUTH_FAILED": BusinessErrorCategory.OTP_FAILURE,
    "CRED_LOCKED": BusinessErrorCategory.OTP_FAILURE,

    # AML / risk
    "AML001": BusinessErrorCategory.AML_REJECTION,
    "AML002": BusinessErrorCategory.AML_REJECTION,
    "RISK_BLOCK": BusinessErrorCategory.RISK_DECLINED,
    "RISK_REVIEW": BusinessErrorCategory.RISK_DECLINED,
    "FRAUD_SUSPECTED": BusinessErrorCategory.RISK_DECLINED,

    # Account / limits
    "INSUFF_BAL": BusinessErrorCategory.INSUFFICIENT_FUNDS,
    "LOW_BALANCE": BusinessErrorCategory.INSUFFICIENT_FUNDS,
    "TXN_LIMIT": BusinessErrorCategory.LIMIT_EXCEEDED,
    "DAILY_LIMIT": BusinessErrorCategory.LIMIT_EXCEEDED,
    "MONTHLY_LIMIT": BusinessErrorCategory.LIMIT_EXCEEDED,
    "INVALID_ACC": BusinessErrorCategory.INVALID_ACCOUNT,
    "INVALID_IFSC": BusinessErrorCategory.INVALID_IFSC,
    "ACC_CLOSED": BusinessErrorCategory.INVALID_ACCOUNT,
    "ACC_DORMANT": BusinessErrorCategory.INVALID_ACCOUNT,
    "DUPE_TXN": BusinessErrorCategory.DUPLICATE_TRANSACTION,
    "TXN_NOT_PERMITTED": BusinessErrorCategory.TRANSACTION_NOT_PERMITTED,
    "TOKEN_EXPIRED": BusinessErrorCategory.EXPIRED_TOKEN,
}

# ---------------------------------------------------------------------------
# API-specific overrides
# Codes here shadow ERROR_CODE_REGISTRY for that api_name
# ---------------------------------------------------------------------------
API_ERROR_CODE_MAP: dict[str, dict[str, BusinessErrorCategory]] = {
    "UPI_COLLECT": {
        "RR": BusinessErrorCategory.INSUFFICIENT_FUNDS,
        "B1": BusinessErrorCategory.INVALID_VPA,
        "U30": BusinessErrorCategory.LIMIT_EXCEEDED,
        "U16": BusinessErrorCategory.INVALID_ACCOUNT,
        "Z9": BusinessErrorCategory.BENEFICIARY_BANK_DOWN,
    },
    "UPI_PAY": {
        "RR": BusinessErrorCategory.INSUFFICIENT_FUNDS,
        "B1": BusinessErrorCategory.INVALID_VPA,
        "XD": BusinessErrorCategory.DUPLICATE_TRANSACTION,
    },
    "IMPS_TRANSFER": {
        "NBIN001": BusinessErrorCategory.INVALID_ACCOUNT,
        "NBIN002": BusinessErrorCategory.INSUFFICIENT_FUNDS,
        "NBIN003": BusinessErrorCategory.LIMIT_EXCEEDED,
        "NBIN007": BusinessErrorCategory.BENEFICIARY_BANK_DOWN,
        "NBIN009": BusinessErrorCategory.DUPLICATE_TRANSACTION,
        "NBIN011": BusinessErrorCategory.TRANSACTION_NOT_PERMITTED,
        "INVALID_IFSC": BusinessErrorCategory.INVALID_IFSC,
    },
    "NEFT_TRANSFER": {
        "AC01": BusinessErrorCategory.INVALID_ACCOUNT,
        "AC06": BusinessErrorCategory.ACCOUNT_FROZEN,
        "AM04": BusinessErrorCategory.INSUFFICIENT_FUNDS,
        "AM12": BusinessErrorCategory.INVALID_ACCOUNT,
    },
    "RTGS_TRANSFER": {
        "AC01": BusinessErrorCategory.INVALID_ACCOUNT,
        "AC06": BusinessErrorCategory.ACCOUNT_FROZEN,
        "AM04": BusinessErrorCategory.INSUFFICIENT_FUNDS,
    },
    "PAYMENT_GATEWAY": {
        "DECLINE": BusinessErrorCategory.INSUFFICIENT_FUNDS,
        "RISK_BLOCK": BusinessErrorCategory.RISK_DECLINED,
        "CVV_FAIL": BusinessErrorCategory.OTP_FAILURE,
        "OTP_FAIL": BusinessErrorCategory.OTP_FAILURE,
        "EXPIRED_CARD": BusinessErrorCategory.EXPIRED_TOKEN,
        "INVALID_CARD": BusinessErrorCategory.INVALID_ACCOUNT,
        "DUPE": BusinessErrorCategory.DUPLICATE_TRANSACTION,
    },
}

# ---------------------------------------------------------------------------
# Response body field paths to inspect for error codes
# Each entry is an ordered list of dot-notation paths.
# First non-null value found is used as the domain error code.
# ---------------------------------------------------------------------------
RESPONSE_BODY_PATHS: dict[str, list[str]] = {
    # Default — tried for any api_name not explicitly listed
    "default": [
        "errorCode",
        "error_code",
        "responseCode",
        "response_code",
        "code",
        "status.code",
        "error.code",
        "data.errorCode",
    ],
    "UPI_COLLECT": [
        "responseCode",
        "errorCode",
        "data.responseCode",
        "data.errorCode",
    ],
    "UPI_PAY": [
        "responseCode",
        "errorCode",
        "data.responseCode",
    ],
    "IMPS_TRANSFER": [
        "txnStatus",
        "errorCode",
        "data.errorCode",
    ],
    "NEFT_TRANSFER": [
        "statusCode",
        "errorCode",
        "response.code",
    ],
    "RTGS_TRANSFER": [
        "statusCode",
        "errorCode",
        "response.code",
    ],
    "PAYMENT_GATEWAY": [
        "declineCode",
        "errorCode",
        "error.code",
        "data.declineCode",
    ],
}

# ---------------------------------------------------------------------------
# Success indicator paths
# If response body contains these paths with truthy values → treat as success
# even if error code fields are also present (edge case: partial success)
# ---------------------------------------------------------------------------
SUCCESS_INDICATOR_PATHS: dict[str, list[str]] = {
    "default": ["success", "isSuccess", "data.success"],
    "UPI_COLLECT": ["success", "data.success"],
    "PAYMENT_GATEWAY": ["authorized", "success"],
}


def resolve_error_code(api_name: str, code: str) -> BusinessErrorCategory:
    """
    Resolve a raw domain error code to a BusinessErrorCategory.
    API-specific map takes precedence over the universal registry.
    """
    api_map = API_ERROR_CODE_MAP.get(api_name, {})
    return api_map.get(code) or ERROR_CODE_REGISTRY.get(code, BusinessErrorCategory.UNKNOWN)


def get_body_paths(api_name: str) -> list[str]:
    """Return the ordered list of JSON paths to probe for an error code."""
    return RESPONSE_BODY_PATHS.get(api_name, RESPONSE_BODY_PATHS["default"])


def extract_nested(data: dict, path: str):
    """
    Extract a value from a nested dict using dot-notation path.
    Returns None if any segment is missing.
    Example: extract_nested(d, "data.errorCode")
    """
    parts = path.split(".")
    current = data
    for part in parts:
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current
