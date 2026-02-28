"""
Abstract base class for all classifiers.
"""

from abc import ABC, abstractmethod

from common.schemas import ClassifiedEvent, LogEvent


class BaseClassifier(ABC):
    """
    A classifier accepts a LogEvent and returns a ClassifiedEvent.
    If the classifier cannot determine the type it should return
    the event unchanged (is_success=True, error_type=NONE).

    Classifiers must never raise — any internal error should be caught,
    logged, and the event returned with classification_confidence=0.
    """

    @abstractmethod
    def classify(self, event: LogEvent) -> ClassifiedEvent:
        """Classify a single event. Must not raise."""

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"
