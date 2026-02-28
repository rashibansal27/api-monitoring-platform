"""
AlertManager — deduplication, cooldown, and notification dispatch.

Receives ScoredAlerts from the RiskScorer and applies:

  1. Minimum score filter   — drop INFO-level alerts (score < LOW threshold)
  2. Redis cooldown check   — suppress if an identical alert fired recently
  3. Persist to DB          — via AlertStore
  4. Fan-out to notifiers   — LogNotifier always; WebhookNotifier if configured
  5. Set Redis cooldown      — prevent re-firing for ALERT_COOLDOWN_SECONDS

Deduplication key:
  (api_name or "platform") + ":" + dominant_anomaly_type
  This means a TPS spike on UPI_COLLECT and on IMPS_TRANSFER are treated as
  separate alerts, but two TPS spikes on UPI_COLLECT within the cooldown
  window are merged (only the first fires).

Cooldown suppression vs. escalation:
  If a suppressed alert has a HIGHER severity than the active one,
  it bypasses the cooldown (escalation override). This ensures that
  a spike escalating from HIGH to CRITICAL still notifies.
"""

from __future__ import annotations

import logging
from typing import Optional

from alerting.alert_store import alert_store
from common.constants import ALERT_COOLDOWN_SECONDS
from common.enums import AlertStatus, Severity
from common.schemas import AnomalySignal, ScoredAlert
from storage.redis_client import (
    get_redis,
    is_alert_in_cooldown,
    set_alert_cooldown,
)

logger = logging.getLogger(__name__)

# Minimum risk score to process an alert at all
_MIN_SCORE_TO_ALERT = 0.20      # INFO threshold — below this is noise

# Severity ordering for escalation check
_SEVERITY_ORDER = {
    Severity.INFO: 0,
    Severity.LOW: 1,
    Severity.MEDIUM: 2,
    Severity.HIGH: 3,
    Severity.CRITICAL: 4,
}

# Redis key for tracking the severity of the currently active alert
_ACTIVE_SEVERITY_PREFIX = "apm:alert:active_sev:"


class AlertManager:
    """
    Central alert lifecycle manager.
    Registered as the downstream handler of RiskScorer.
    """

    def __init__(self) -> None:
        self._notifiers: list = []     # BaseNotifier instances
        self._redis = get_redis()

    def add_notifier(self, notifier) -> None:
        """Register a notifier (LogNotifier, WebhookNotifier, etc.)."""
        self._notifiers.append(notifier)

    async def handle(self, alert: ScoredAlert) -> None:
        """
        Main entry point — called by RiskScorer for every scored alert.
        """
        # ── 1. Score threshold ────────────────────────────────────────────
        if alert.risk_score < _MIN_SCORE_TO_ALERT:
            logger.debug(
                "AlertManager: dropped low-score alert %.3f (api=%s)",
                alert.risk_score, alert.api_name,
            )
            return

        # ── 2. Build dedup key ────────────────────────────────────────────
        dedup_key = self._dedup_key(alert)

        # ── 3. Cooldown check with escalation override ────────────────────
        in_cooldown = await is_alert_in_cooldown(
            self._redis, alert.api_name or "platform", dedup_key
        )

        if in_cooldown:
            # Check if this alert is more severe than the suppressed one
            active_sev = await self._get_active_severity(dedup_key)
            if active_sev is not None:
                if _SEVERITY_ORDER.get(alert.severity, 0) <= _SEVERITY_ORDER.get(active_sev, 0):
                    logger.debug(
                        "AlertManager: suppressed (cooldown) %s sev=%s key=%s",
                        alert.alert_id[:8], alert.severity, dedup_key,
                    )
                    return
                # Escalation — fall through and notify
                logger.info(
                    "AlertManager: ESCALATION %s → %s for key=%s",
                    active_sev, alert.severity, dedup_key,
                )
            else:
                return

        # ── 4. Persist alert ──────────────────────────────────────────────
        try:
            await alert_store.save_alert(alert)
        except Exception:
            logger.exception(
                "AlertManager: DB persist failed for alert %s — continuing to notify",
                alert.alert_id,
            )

        # ── 5. Notify ─────────────────────────────────────────────────────
        await self._notify(alert)

        # ── 6. Set cooldown ───────────────────────────────────────────────
        await set_alert_cooldown(
            self._redis,
            alert.api_name or "platform",
            dedup_key,
            ttl_seconds=ALERT_COOLDOWN_SECONDS,
        )
        await self._set_active_severity(dedup_key, alert.severity)

        logger.info(
            "AlertManager: fired alert %s | risk=%.3f sev=%s | %s",
            alert.alert_id[:8],
            alert.risk_score,
            alert.severity.value.upper(),
            alert.title,
        )

    async def acknowledge(self, alert_id: str) -> bool:
        """Mark an alert as ACKNOWLEDGED."""
        return await alert_store.update_status(alert_id, AlertStatus.ACKNOWLEDGED)

    async def resolve(self, alert_id: str) -> bool:
        """Mark an alert as RESOLVED."""
        return await alert_store.update_status(alert_id, AlertStatus.RESOLVED)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _dedup_key(alert: ScoredAlert) -> str:
        """
        Deterministic dedup key from the dominant anomaly type.
        Same API + same anomaly type within cooldown = same alert.
        """
        dominant = _dominant_anomaly_type(alert.signals)
        scope = alert.api_name or alert.merchant_id or "platform"
        return f"{scope}:{dominant}"

    async def _notify(self, alert: ScoredAlert) -> None:
        for notifier in self._notifiers:
            try:
                await notifier.notify(alert)
            except Exception:
                logger.exception(
                    "Notifier %s raised for alert %s",
                    notifier.__class__.__name__, alert.alert_id,
                )

    async def _get_active_severity(self, dedup_key: str) -> Optional[Severity]:
        key = _ACTIVE_SEVERITY_PREFIX + dedup_key
        val = await self._redis.get(key)
        if val is None:
            return None
        try:
            return Severity(val)
        except ValueError:
            return None

    async def _set_active_severity(
        self, dedup_key: str, severity: Severity
    ) -> None:
        key = _ACTIVE_SEVERITY_PREFIX + dedup_key
        await self._redis.set(key, severity.value, ex=ALERT_COOLDOWN_SECONDS)


def _dominant_anomaly_type(signals: list[AnomalySignal]) -> str:
    """Return the anomaly_type of the highest-confidence signal."""
    if not signals:
        return "unknown"
    top = max(signals, key=lambda s: s.confidence)
    return top.anomaly_type.value


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------
alert_manager = AlertManager()
