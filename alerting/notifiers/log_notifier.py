"""
LogNotifier — structured log-based alert notifier.

Always registered as the first notifier. Maps alert severity to the
appropriate Python log level so log aggregators (ELK, Loki, CloudWatch)
can filter and route by severity without additional parsing.

Severity → log level:
  CRITICAL → logging.CRITICAL
  HIGH     → logging.ERROR
  MEDIUM   → logging.WARNING
  LOW      → logging.INFO
  INFO     → logging.DEBUG
"""

import logging

from alerting.notifiers.base import BaseNotifier
from common.enums import Severity
from common.schemas import ScoredAlert

logger = logging.getLogger("alerting.notifiers.log")

_LEVEL_MAP = {
    Severity.CRITICAL: logging.CRITICAL,
    Severity.HIGH:     logging.ERROR,
    Severity.MEDIUM:   logging.WARNING,
    Severity.LOW:      logging.INFO,
    Severity.INFO:     logging.DEBUG,
}


class LogNotifier(BaseNotifier):
    """
    Emits a structured log line per alert.
    The log record's `extra` fields are machine-parseable for log aggregators.
    """

    async def notify(self, alert: ScoredAlert) -> None:
        level = _LEVEL_MAP.get(alert.severity, logging.WARNING)

        # Build a structured extra dict — parseable by logstash / fluentd
        extra = {
            "alert_id":          alert.alert_id,
            "risk_score":        round(alert.risk_score, 4),
            "severity":          alert.severity.value,
            "status":            alert.status.value,
            "api_name":          alert.api_name or "platform",
            "merchant_id":       alert.merchant_id or "",
            "error_rate_score":  round(alert.error_rate_anomaly_score, 4),
            "txn_pattern_score": round(alert.txn_pattern_anomaly_score, 4),
            "latency_score":     round(alert.latency_anomaly_score, 4),
            "client_spike_score":round(alert.client_failure_spike_score, 4),
            "signal_count":      len(alert.signals),
            "signal_types":      list({s.anomaly_type.value for s in alert.signals}),
            "detectors":         list({s.detector.value for s in alert.signals}),
        }

        message = (
            f"ALERT [{alert.severity.value.upper()}] {alert.title} | "
            f"risk={alert.risk_score:.3f} | "
            f"api={extra['api_name']} | "
            f"merchant={extra['merchant_id'] or '-'} | "
            f"id={alert.alert_id[:8]}"
        )

        logger.log(level, message, extra=extra)

        # For CRITICAL alerts, also log each contributing signal individually
        if alert.severity == Severity.CRITICAL:
            for sig in sorted(alert.signals, key=lambda s: s.confidence, reverse=True):
                logger.critical(
                    "  ↳ signal [%s] %s obs=%.4f exp=%.4f score=%.2f conf=%.2f | %s",
                    sig.detector.value,
                    sig.anomaly_type.value,
                    sig.observed_value,
                    sig.expected_value,
                    sig.deviation_score,
                    sig.confidence,
                    sig.description[:120],
                )
