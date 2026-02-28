"""Abstract base class for all alert notifiers."""

from abc import ABC, abstractmethod

from common.schemas import ScoredAlert


class BaseNotifier(ABC):
    """
    Every notifier receives a ScoredAlert and dispatches it
    to an external channel (log, webhook, email, Slack, PagerDuty…).
    Must never raise — catch exceptions internally and log them.
    """

    @abstractmethod
    async def notify(self, alert: ScoredAlert) -> None:
        """Dispatch the alert. Must not raise."""

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"
