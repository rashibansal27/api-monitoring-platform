"""
WebhookNotifier — HTTP POST to an external endpoint on each alert.

Payload format (generic — works with Slack incoming webhooks,
PagerDuty Events API v2, custom internal endpoints):

  {
    "alert_id":   "...",
    "title":      "TPS Spike on UPI_COLLECT",
    "severity":   "high",
    "risk_score": 0.742,
    "api_name":   "UPI_COLLECT",
    "merchant_id": null,
    "created_at": "2024-01-15T10:30:00Z",
    "scores": {
      "error_rate": 0.82,
      "txn_pattern": 0.65,
      "latency": 0.30,
      "client_spike": 0.0
    },
    "signals": [
      { "detector": "zscore", "type": "tps_spike", "observed": 340.2, ... }
    ]
  }

Retry policy:
  3 attempts with exponential back-off (1s, 2s, 4s).
  On all retries exhausted: log error, do NOT raise.

Configuration (via .env):
  WEBHOOK_URL      — required, POST target
  WEBHOOK_SECRET   — optional, added as X-Webhook-Secret header
  WEBHOOK_MIN_SEV  — minimum severity to send (default: medium)
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime

import httpx

from alerting.notifiers.base import BaseNotifier
from common.enums import Severity
from common.schemas import ScoredAlert

logger = logging.getLogger(__name__)

_SEVERITY_ORDER = {
    Severity.INFO: 0, Severity.LOW: 1,
    Severity.MEDIUM: 2, Severity.HIGH: 3, Severity.CRITICAL: 4,
}

_MAX_RETRIES = 3
_RETRY_BASE_DELAY = 1.0     # seconds; doubles on each retry
_TIMEOUT = 8.0              # seconds per attempt


class WebhookNotifier(BaseNotifier):
    """
    Sends a structured JSON payload to a configured webhook URL.
    """

    def __init__(
        self,
        url: str,
        secret: str | None = None,
        min_severity: Severity = Severity.MEDIUM,
    ) -> None:
        if not url:
            raise ValueError("WebhookNotifier requires a non-empty URL")
        self._url = url
        self._secret = secret
        self._min_severity = min_severity

    async def notify(self, alert: ScoredAlert) -> None:
        # Apply minimum severity filter
        if _SEVERITY_ORDER.get(alert.severity, 0) < _SEVERITY_ORDER.get(self._min_severity, 0):
            return

        payload = self._build_payload(alert)
        headers = {"Content-Type": "application/json"}
        if self._secret:
            headers["X-Webhook-Secret"] = self._secret

        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                    response = await client.post(self._url, json=payload, headers=headers)
                    response.raise_for_status()
                    logger.info(
                        "WebhookNotifier: delivered alert %s (attempt %d, status %d)",
                        alert.alert_id[:8], attempt, response.status_code,
                    )
                    return  # Success

            except httpx.HTTPStatusError as exc:
                logger.error(
                    "WebhookNotifier: HTTP %d on attempt %d for alert %s",
                    exc.response.status_code, attempt, alert.alert_id[:8],
                )
                if exc.response.status_code < 500:
                    return  # 4xx — don't retry

            except httpx.RequestError as exc:
                logger.warning(
                    "WebhookNotifier: request error on attempt %d for alert %s: %s",
                    attempt, alert.alert_id[:8], exc,
                )

            if attempt < _MAX_RETRIES:
                await asyncio.sleep(_RETRY_BASE_DELAY * (2 ** (attempt - 1)))

        logger.error(
            "WebhookNotifier: all %d retries exhausted for alert %s — giving up",
            _MAX_RETRIES, alert.alert_id[:8],
        )

    @staticmethod
    def _build_payload(alert: ScoredAlert) -> dict:
        return {
            "alert_id":   alert.alert_id,
            "title":      alert.title,
            "severity":   alert.severity.value,
            "status":     alert.status.value,
            "risk_score": round(alert.risk_score, 4),
            "api_name":   alert.api_name,
            "merchant_id": alert.merchant_id,
            "created_at": alert.created_at.isoformat(),
            "scores": {
                "error_rate":   round(alert.error_rate_anomaly_score, 4),
                "txn_pattern":  round(alert.txn_pattern_anomaly_score, 4),
                "latency":      round(alert.latency_anomaly_score, 4),
                "client_spike": round(alert.client_failure_spike_score, 4),
            },
            "signals": [
                {
                    "signal_id":      s.signal_id,
                    "detector":       s.detector.value,
                    "anomaly_type":   s.anomaly_type.value,
                    "metric_name":    s.metric_name.value,
                    "observed_value": round(s.observed_value, 4),
                    "expected_value": round(s.expected_value, 4),
                    "deviation_score":round(s.deviation_score, 4),
                    "confidence":     round(s.confidence, 4),
                    "description":    s.description[:200],
                }
                for s in sorted(alert.signals, key=lambda x: x.confidence, reverse=True)[:10]
            ],
        }
