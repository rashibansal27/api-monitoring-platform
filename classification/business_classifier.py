"""
BusinessErrorClassifier

Detects domain-level payment failures that travel inside HTTP 200 responses.

The core challenge: a bank returns HTTP 200 with a JSON body like:
  { "responseCode": "RR", "message": "Insufficient balance" }

This is a BUSINESS FAILURE — the transaction did not go through —
but standard HTTP monitoring would count it as a success.

Detection strategy:
  1. Check if status code is eligible for business error inspection
  2. Walk the response body using configured JSON paths (per API)
  3. Strip and normalise the extracted code
  4. Check against success code whitelist first (fast exit)
  5. Resolve code → BusinessErrorCategory via error_codes registry
  6. Check optional success-indicator fields (e.g. "success": true)
  7. Assign severity from the business rules table
"""

import logging

from classification.base import BaseClassifier
from classification.rules.business_rules import (
    BUSINESS_ERROR_ELIGIBLE_STATUS_CODES,
    BUSINESS_ERROR_SEVERITY,
    MIN_BUSINESS_CLASSIFICATION_CONFIDENCE,
    SUCCESS_DOMAIN_CODES,
)
from config.error_codes import (
    extract_nested,
    get_body_paths,
    resolve_error_code,
)
from common.enums import BusinessErrorCategory, ErrorType, Severity
from common.schemas import ClassifiedEvent, LogEvent

logger = logging.getLogger(__name__)


class BusinessErrorClassifier(BaseClassifier):
    """
    Inspects the response body of eligible events for domain error codes.

    Should be called AFTER TechnicalErrorClassifier — only run on events
    that are still marked is_success=True (no technical error found).
    """

    def classify(self, event: LogEvent) -> ClassifiedEvent:
        try:
            return self._classify(event)
        except Exception:
            logger.exception(
                "BusinessErrorClassifier raised on event %s — returning unclassified",
                event.event_id,
            )
            return ClassifiedEvent(**{
                **event.model_dump(),
                "error_type": ErrorType.NONE,
                "is_success": True,
                "classification_confidence": 0.0,
                "classification_notes": "classifier_error",
            })

    def _classify(self, event: LogEvent) -> ClassifiedEvent:
        # ── Step 1: Is this status code eligible? ────────────────────────────
        if event.http_status_code not in BUSINESS_ERROR_ELIGIBLE_STATUS_CODES:
            return self._pass_through(event)

        # ── Step 2: Is there a response body to inspect? ─────────────────────
        body = event.response_body
        if not body or not isinstance(body, dict):
            return self._pass_through(event)

        # ── Step 3: Extract domain error code from body ──────────────────────
        raw_code, path_used = self._extract_error_code(event.api_name, body)

        if raw_code is None:
            # No error code field found → treat as success
            return self._pass_through(event, notes="no_error_code_field")

        normalised_code = raw_code.strip().upper()

        # ── Step 4: Success code whitelist check ─────────────────────────────
        if normalised_code in SUCCESS_DOMAIN_CODES:
            return self._pass_through(event, notes=f"success_code:{normalised_code}")

        # ── Step 5: Check success-indicator fields ───────────────────────────
        if self._has_success_indicator(event.api_name, body):
            return self._pass_through(event, notes="success_indicator_true")

        # ── Step 6: Resolve code → BusinessErrorCategory ─────────────────────
        category = resolve_error_code(event.api_name, normalised_code)

        # Confidence: known code = 1.0, unknown fallback = 0.7
        confidence = (
            1.0 if category != BusinessErrorCategory.UNKNOWN
            else 0.7
        )

        if confidence < MIN_BUSINESS_CLASSIFICATION_CONFIDENCE:
            return self._pass_through(
                event,
                notes=f"low_confidence:{normalised_code}:{confidence:.2f}",
            )

        # ── Step 7: Assign severity ───────────────────────────────────────────
        severity = BUSINESS_ERROR_SEVERITY.get(category, Severity.LOW)

        # Extract human-readable message from body if available
        domain_message = (
            body.get("message")
            or body.get("errorMessage")
            or body.get("description")
            or str(category.value)
        )

        return ClassifiedEvent(**{
            **event.model_dump(),
            "error_type": ErrorType.BUSINESS,
            "is_success": False,
            "business_error_category": category,
            "domain_error_code": normalised_code,
            "domain_error_message": str(domain_message)[:255],
            "severity": severity,
            "classification_confidence": confidence,
            "classification_notes": f"path:{path_used}",
        })

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_error_code(
        api_name: str, body: dict
    ) -> tuple[str | None, str]:
        """
        Walk the configured JSON paths for this API.
        Returns (raw_code, path_used) — both None/"" if not found.
        """
        for path in get_body_paths(api_name):
            value = extract_nested(body, path)
            if value is not None:
                return str(value), path
        return None, ""

    @staticmethod
    def _has_success_indicator(api_name: str, body: dict) -> bool:
        """
        Check if the body contains a success indicator set to a truthy value.
        Uses the SUCCESS_INDICATOR_PATHS from config/error_codes.py.
        """
        from config.error_codes import SUCCESS_INDICATOR_PATHS
        paths = SUCCESS_INDICATOR_PATHS.get(api_name, SUCCESS_INDICATOR_PATHS["default"])
        for path in paths:
            value = extract_nested(body, path)
            if value is True or value == "true" or value == "TRUE" or value == 1:
                return True
        return False

    @staticmethod
    def _pass_through(event: LogEvent, notes: str = "") -> ClassifiedEvent:
        """Return the event as a clean success — no business error found."""
        return ClassifiedEvent(**{
            **event.model_dump(),
            "error_type": ErrorType.NONE,
            "is_success": True,
            "classification_confidence": 1.0,
            "classification_notes": notes or None,
        })
