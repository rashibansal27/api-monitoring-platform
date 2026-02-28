"""
Classification pipeline.

Chains TechnicalErrorClassifier → BusinessErrorClassifier in sequence.

Logic:
  1. Run TechnicalErrorClassifier first.
     If it finds a technical error → return immediately (no business check needed).
  2. Run BusinessErrorClassifier on events that passed through step 1.
     If it finds a business error → return with business classification.
  3. Remaining events are clean successes.

The pipeline also acts as the EventBus handler — it subscribes to the
InMemoryEventBus and publishes ClassifiedEvents downstream.
"""

import logging
from typing import Callable, Awaitable

from classification.business_classifier import BusinessErrorClassifier
from classification.technical_classifier import TechnicalErrorClassifier
from common.enums import ErrorType
from common.schemas import ClassifiedEvent, LogEvent

logger = logging.getLogger(__name__)

# Type alias for downstream async handlers
ClassifiedEventHandler = Callable[[ClassifiedEvent], Awaitable[None]]


class ClassificationPipeline:
    """
    Stateless classification pipeline.
    Thread-safe and async-safe — classifiers hold no mutable state.
    """

    def __init__(self) -> None:
        self._technical = TechnicalErrorClassifier()
        self._business = BusinessErrorClassifier()
        self._handlers: list[ClassifiedEventHandler] = []

    def add_handler(self, handler: ClassifiedEventHandler) -> None:
        """Register a downstream async handler for classified events."""
        self._handlers.append(handler)

    def classify(self, event: LogEvent) -> ClassifiedEvent:
        """
        Synchronous classification — useful for testing and batch processing.
        Returns ClassifiedEvent with full error tagging.
        """
        # Stage 1: technical
        result = self._technical.classify(event)
        if result.error_type == ErrorType.TECHNICAL:
            self._log_classification(result)
            return result

        # Stage 2: business (only if not already flagged as technical)
        result = self._business.classify(result)
        self._log_classification(result)
        return result

    async def handle(self, event: LogEvent) -> None:
        """
        Async EventBus handler.
        Classifies the event then fans out to all registered downstream handlers.
        """
        classified = self.classify(event)
        for handler in self._handlers:
            try:
                await handler(classified)
            except Exception:
                logger.exception(
                    "ClassificationPipeline downstream handler %s raised for event %s",
                    handler,
                    event.event_id,
                )

    @staticmethod
    def _log_classification(event: ClassifiedEvent) -> None:
        if event.error_type == ErrorType.NONE:
            return  # Don't flood logs with successful transactions

        if event.error_type == ErrorType.TECHNICAL:
            logger.debug(
                "TECHNICAL %s | api=%s status=%d latency=%.0fms cat=%s sev=%s | %s",
                event.event_id[:8],
                event.api_name,
                event.http_status_code,
                event.response_time_ms,
                event.technical_error_category,
                event.severity,
                event.classification_notes or "",
            )
        elif event.error_type == ErrorType.BUSINESS:
            logger.debug(
                "BUSINESS  %s | api=%s code=%s cat=%s sev=%s merchant=%s | %s",
                event.event_id[:8],
                event.api_name,
                event.domain_error_code,
                event.business_error_category,
                event.severity,
                event.merchant_id or "-",
                event.domain_error_message or "",
            )


# ---------------------------------------------------------------------------
# Module-level singleton — imported by main.py and other consumers
# ---------------------------------------------------------------------------
pipeline = ClassificationPipeline()
