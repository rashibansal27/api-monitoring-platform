"""
Prometheus ingestor.

Runs a set of PromQL queries against the Prometheus HTTP API
and converts the results into synthetic LogEvents and raw metric samples.

Design note:
  Prometheus gives us pre-aggregated metrics (TPS, error rate, latency histograms).
  We use it to SUPPLEMENT ES log data — ES gives us per-request detail,
  Prometheus gives us accurate aggregates at scale.
"""

import logging
from datetime import datetime, timezone
from typing import Any

import httpx

from common.schemas import LogEvent
from config.settings import get_settings
from ingestion.base import BaseIngestor, EventBus
from ingestion.normalizer import normalize_prometheus_sample

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# PromQL query registry
# Each entry: (metric_name, promql_expression)
# Results are published as synthetic LogEvents for the metrics pipeline.
# ---------------------------------------------------------------------------
PROMQL_QUERIES: list[tuple[str, str]] = [
    (
        "http_request_tps",
        'sum(rate(http_requests_total[1m])) by (api_name, method, status_code)',
    ),
    (
        "http_error_rate",
        'sum(rate(http_requests_total{status_code=~"5.."}[1m])) by (api_name)',
    ),
    (
        "http_latency_p95",
        'histogram_quantile(0.95, sum(rate(http_request_duration_ms_bucket[5m])) by (le, api_name))',
    ),
    (
        "http_latency_p99",
        'histogram_quantile(0.99, sum(rate(http_request_duration_ms_bucket[5m])) by (le, api_name))',
    ),
    (
        "payment_txn_amount_avg",
        'avg(payment_transaction_amount_rupees) by (api_name, merchant_id)',
    ),
]


class PrometheusIngestor(BaseIngestor):
    """
    Fires PromQL instant queries, converts each (metric, label, value) sample
    into a synthetic LogEvent and publishes to the bus.
    """

    def __init__(self, bus: EventBus) -> None:
        super().__init__(bus)
        self._settings = get_settings().prometheus
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        self._client = httpx.AsyncClient(
            base_url=self._settings.url,
            timeout=self._settings.timeout_seconds,
        )
        logger.info("PrometheusIngestor started → %s", self._settings.url)

    async def stop(self) -> None:
        if self._client:
            await self._client.aclose()

    # ------------------------------------------------------------------
    # BaseIngestor implementation
    # ------------------------------------------------------------------

    async def fetch(self) -> list[dict]:
        """
        Runs all PromQL queries; returns a flat list of raw sample dicts.
        Each dict includes the metric_name so the normalizer can use it.
        """
        if self._client is None:
            await self.start()

        all_samples: list[dict] = []
        for metric_name, expr in PROMQL_QUERIES:
            samples = await self._query_instant(metric_name, expr)
            all_samples.extend(samples)

        logger.debug("Prometheus fetched %d samples", len(all_samples))
        return all_samples

    async def normalize(self, raw_records: list[dict]) -> list[LogEvent]:
        events: list[LogEvent] = []
        for record in raw_records:
            event = normalize_prometheus_sample(
                metric_labels=record.get("labels", {}),
                timestamp=record.get("timestamp", 0.0),
                value=record.get("value", 0.0),
                metric_name=record.get("metric_name", ""),
            )
            if event is not None:
                events.append(event)
        return events

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _query_instant(self, metric_name: str, expr: str) -> list[dict[str, Any]]:
        """Run a single PromQL instant query; return raw sample dicts."""
        try:
            resp = await self._client.get(
                "/api/v1/query",
                params={"query": expr},
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error("Prometheus query '%s' failed: %s", metric_name, exc.response.text[:200])
            return []
        except httpx.RequestError as exc:
            logger.error("Prometheus connection error for '%s': %s", metric_name, exc)
            return []

        data = resp.json()
        if data.get("status") != "success":
            logger.warning("Prometheus returned non-success for '%s': %r", metric_name, data)
            return []

        results: list[dict] = []
        for result in data.get("data", {}).get("result", []):
            labels: dict = result.get("metric", {})
            timestamp_str, value_str = result.get("value", [None, "0"])
            try:
                timestamp = float(timestamp_str) if timestamp_str else datetime.now(timezone.utc).timestamp()
                value = float(value_str)
            except (TypeError, ValueError):
                continue

            results.append({
                "metric_name": metric_name,
                "labels": labels,
                "timestamp": timestamp,
                "value": value,
            })

        return results
