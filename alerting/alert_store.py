"""
AlertStore — async persistence and query layer for alerts and signals.

Writes:
  save_alert(ScoredAlert)  → AlertORM + AnomalySignalORM rows

Reads (used by API routes):
  get_recent_alerts()      → list of recent AlertORM rows
  get_alert(alert_id)      → single AlertORM with signals
  get_alerts_for_api()     → paginated query per api_name
  get_alerts_for_merchant()→ paginated query per merchant_id

Lifecycle:
  update_status()          → OPEN → ACKNOWLEDGED / RESOLVED / SUPPRESSED
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional
from uuid import UUID

from sqlalchemy import desc, select, update

from common.enums import AlertStatus
from common.schemas import AnomalySignal, ScoredAlert
from storage.database import get_session
from storage.models import AlertORM, AnomalySignalORM

logger = logging.getLogger(__name__)


class AlertStore:

    async def save_alert(self, alert: ScoredAlert) -> str:
        """
        Persist a ScoredAlert and all its AnomalySignals.
        Returns the alert_id string.
        """
        try:
            async with get_session() as session:
                orm_alert = AlertORM(
                    id=alert.alert_id,
                    created_at=alert.created_at,
                    updated_at=datetime.now(timezone.utc),
                    api_name=alert.api_name,
                    merchant_id=alert.merchant_id,
                    risk_score=alert.risk_score,
                    error_rate_anomaly_score=alert.error_rate_anomaly_score,
                    txn_pattern_anomaly_score=alert.txn_pattern_anomaly_score,
                    latency_anomaly_score=alert.latency_anomaly_score,
                    client_failure_spike_score=alert.client_failure_spike_score,
                    severity=alert.severity.value,
                    status=alert.status.value,
                    title=alert.title,
                    description=alert.description,
                )
                session.add(orm_alert)

                for sig in alert.signals:
                    orm_signal = self._signal_to_orm(sig, alert.alert_id)
                    session.add(orm_signal)

            logger.debug("Saved alert %s (risk=%.3f sev=%s)", alert.alert_id[:8], alert.risk_score, alert.severity)
            return alert.alert_id

        except Exception:
            logger.exception("Failed to save alert %s", alert.alert_id)
            raise

    async def update_status(self, alert_id: str, status: AlertStatus) -> bool:
        """Update alert status. Returns True if the row was found and updated."""
        try:
            async with get_session() as session:
                result = await session.execute(
                    update(AlertORM)
                    .where(AlertORM.id == alert_id)
                    .values(
                        status=status.value,
                        updated_at=datetime.now(timezone.utc),
                    )
                )
            return result.rowcount > 0
        except Exception:
            logger.exception("Failed to update alert status %s → %s", alert_id, status)
            return False

    async def get_alert(self, alert_id: str) -> Optional[AlertORM]:
        try:
            async with get_session() as session:
                result = await session.execute(
                    select(AlertORM).where(AlertORM.id == alert_id)
                )
                return result.scalar_one_or_none()
        except Exception:
            logger.exception("Failed to fetch alert %s", alert_id)
            return None

    async def get_recent_alerts(
        self,
        limit: int = 50,
        min_severity: Optional[str] = None,
        status: Optional[str] = None,
        since_hours: int = 24,
    ) -> list[AlertORM]:
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)
            async with get_session() as session:
                q = select(AlertORM).where(AlertORM.created_at >= cutoff)
                if min_severity:
                    # Severity ordering: info < low < medium < high < critical
                    _order = ["info", "low", "medium", "high", "critical"]
                    idx = _order.index(min_severity.lower()) if min_severity.lower() in _order else 0
                    q = q.where(AlertORM.severity.in_(_order[idx:]))
                if status:
                    q = q.where(AlertORM.status == status)
                q = q.order_by(desc(AlertORM.created_at)).limit(limit)
                result = await session.execute(q)
                return list(result.scalars().all())
        except Exception:
            logger.exception("Failed to fetch recent alerts")
            return []

    async def get_alerts_for_api(
        self,
        api_name: str,
        limit: int = 50,
        since_hours: int = 24,
    ) -> list[AlertORM]:
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)
            async with get_session() as session:
                result = await session.execute(
                    select(AlertORM)
                    .where(
                        AlertORM.api_name == api_name,
                        AlertORM.created_at >= cutoff,
                    )
                    .order_by(desc(AlertORM.created_at))
                    .limit(limit)
                )
                return list(result.scalars().all())
        except Exception:
            logger.exception("Failed to fetch alerts for api=%s", api_name)
            return []

    async def get_alerts_for_merchant(
        self,
        merchant_id: str,
        limit: int = 50,
        since_hours: int = 24,
    ) -> list[AlertORM]:
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)
            async with get_session() as session:
                result = await session.execute(
                    select(AlertORM)
                    .where(
                        AlertORM.merchant_id == merchant_id,
                        AlertORM.created_at >= cutoff,
                    )
                    .order_by(desc(AlertORM.created_at))
                    .limit(limit)
                )
                return list(result.scalars().all())
        except Exception:
            logger.exception("Failed to fetch alerts for merchant=%s", merchant_id)
            return []

    async def get_signals_for_alert(self, alert_id: str) -> list[AnomalySignalORM]:
        try:
            async with get_session() as session:
                result = await session.execute(
                    select(AnomalySignalORM)
                    .where(AnomalySignalORM.alert_id == alert_id)
                    .order_by(desc(AnomalySignalORM.detected_at))
                )
                return list(result.scalars().all())
        except Exception:
            logger.exception("Failed to fetch signals for alert %s", alert_id)
            return []

    async def count_open_alerts(
        self,
        api_name: Optional[str] = None,
        since_hours: int = 1,
    ) -> int:
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)
            async with get_session() as session:
                q = select(AlertORM).where(
                    AlertORM.status == AlertStatus.OPEN.value,
                    AlertORM.created_at >= cutoff,
                )
                if api_name:
                    q = q.where(AlertORM.api_name == api_name)
                result = await session.execute(q)
                return len(result.scalars().all())
        except Exception:
            logger.exception("count_open_alerts failed")
            return 0

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _signal_to_orm(sig: AnomalySignal, alert_id: str) -> AnomalySignalORM:
        return AnomalySignalORM(
            id=sig.signal_id,
            detected_at=sig.detected_at,
            alert_id=alert_id,
            detector=sig.detector.value,
            anomaly_type=sig.anomaly_type.value,
            metric_name=sig.metric_name.value,
            api_name=sig.api_name,
            merchant_id=sig.merchant_id,
            observed_value=sig.observed_value,
            expected_value=sig.expected_value,
            deviation_score=sig.deviation_score,
            is_anomaly=sig.is_anomaly,
            confidence=sig.confidence,
            description=sig.description,
            details=sig.details,
        )


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------
alert_store = AlertStore()
