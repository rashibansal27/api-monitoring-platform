"""
TechnicalErrorClassifier

Detects infrastructure and network-layer failures:
  - HTTP 5xx responses
  - Gateway timeouts (502, 503, 504)
  - Request/response timeouts (408, 504, latency threshold)
  - Database errors (pattern-matched from error_message)
  - Circuit breaker trips
  - Upstream / dependency failures

Evaluation order (first match wins):
  1. Explicit status code rule  — fastest, most authoritative
  2. Status code range (5xx)    — catch-all for unlisted 5xx
  3. Latency threshold          — slow success can still be a timeout
  4. Error message patterns     — infra signals in text fields
"""

import logging

from classification.base import BaseClassifier
from classification.rules.technical_rules import (
    ERROR_MESSAGE_RULES,
    LATENCY_TIMEOUT_RULES,
    NON_TECHNICAL_STATUS_CODES,
    STATUS_CODE_RULE_MAP,
    classify_by_status_range,
)
from common.enums import ErrorType, Severity, TechnicalErrorCategory
from common.schemas import ClassifiedEvent, LogEvent

logger = logging.getLogger(__name__)


class TechnicalErrorClassifier(BaseClassifier):
    """
    Classifies a LogEvent as a technical error or passes it through unchanged.

    Returns:
      ClassifiedEvent with error_type=TECHNICAL if a technical error is found.
      ClassifiedEvent with error_type=NONE (is_success=True) otherwise —
      so the business classifier can then inspect it.
    """

    def classify(self, event: LogEvent) -> ClassifiedEvent:
        try:
            return self._classify(event)
        except Exception:
            logger.exception(
                "TechnicalErrorClassifier raised on event %s — returning unclassified",
                event.event_id,
            )
            return ClassifiedEvent(
                **event.model_dump(),
                error_type=ErrorType.NONE,
                is_success=True,
                classification_confidence=0.0,
                classification_notes="classifier_error",
            )

    def _classify(self, event: LogEvent) -> ClassifiedEvent:
        status = event.http_status_code
        latency = event.response_time_ms
        error_msg = (event.error_message or "").strip()

        # ── Step 1: Explicit status code rule ────────────────────────────────
        if status in STATUS_CODE_RULE_MAP:
            rule = STATUS_CODE_RULE_MAP[status]
            return self._make_technical(
                event,
                category=rule.category,
                severity=rule.severity,
                notes=rule.description,
                confidence=1.0,
            )

        # ── Step 2: 5xx range fallback ───────────────────────────────────────
        range_result = classify_by_status_range(status)
        if range_result:
            category, severity = range_result
            return self._make_technical(
                event,
                category=category,
                severity=severity,
                notes=f"HTTP {status} — unclassified 5xx",
                confidence=0.95,
            )

        # ── Step 3: Latency-based timeout (only for non-error status codes) ──
        # A 200 that took 15s is a timeout, even if the payload says "success".
        if status in NON_TECHNICAL_STATUS_CODES or (200 <= status <= 299):
            for rule in LATENCY_TIMEOUT_RULES:
                if latency >= rule.threshold_ms:
                    return self._make_technical(
                        event,
                        category=TechnicalErrorCategory.TIMEOUT,
                        severity=rule.severity,
                        notes=f"{rule.description} (latency={latency:.0f}ms)",
                        confidence=0.85,
                    )

        # ── Step 4: Error message pattern matching ───────────────────────────
        if error_msg:
            for rule in ERROR_MESSAGE_RULES:
                if rule.pattern.search(error_msg):
                    return self._make_technical(
                        event,
                        category=rule.category,
                        severity=rule.severity,
                        notes=f"{rule.description} [pattern match]",
                        confidence=0.80,
                    )

        # ── No technical error found ─────────────────────────────────────────
        return ClassifiedEvent(
            **event.model_dump(),
            error_type=ErrorType.NONE,
            is_success=True,
            classification_confidence=1.0,
        )

    @staticmethod
    def _make_technical(
        event: LogEvent,
        category: TechnicalErrorCategory,
        severity: Severity,
        notes: str,
        confidence: float,
    ) -> ClassifiedEvent:
        return ClassifiedEvent(
            **event.model_dump(),
            error_type=ErrorType.TECHNICAL,
            is_success=False,
            technical_error_category=category,
            severity=severity,
            classification_confidence=confidence,
            classification_notes=notes,
        )
