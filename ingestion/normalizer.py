"""
Normalizer: maps source-native records → unified LogEvent.

Each source has its own field naming, nesting, and date formats.
All of that messiness is isolated here so downstream modules
only ever see clean LogEvent objects.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from common.enums import DataSource
from common.schemas import LogEvent

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_timestamp(value: Any, fallback: bool = True) -> Optional[datetime]:
    """
    Parse a timestamp value that may be:
      - ISO 8601 string ("2024-01-15T10:30:00.000Z")
      - Unix epoch int/float (seconds or milliseconds)
    Returns None (or utcnow if fallback=True) on failure.
    """
    if value is None:
        return _utcnow() if fallback else None

    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value

    if isinstance(value, (int, float)):
        # Detect millis vs seconds
        ts = value / 1000.0 if value > 1e10 else float(value)
        return datetime.fromtimestamp(ts, tz=timezone.utc)

    if isinstance(value, str):
        for fmt in (
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S",
        ):
            try:
                dt = datetime.strptime(value, fmt)
                return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
            except ValueError:
                continue

    logger.warning("Could not parse timestamp: %r", value)
    return _utcnow() if fallback else None


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 200) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Elasticsearch normalizer
# ---------------------------------------------------------------------------

# Field name aliases — covers common ECS, custom Kibana, and legacy schemas
_ES_FIELD_ALIASES: dict[str, list[str]] = {
    "timestamp":         ["@timestamp", "timestamp", "event.created", "time"],
    "api_name":          ["api.name", "api_name", "service.name", "labels.api_name", "fields.api"],
    "api_path":          ["url.path", "http.request.path", "request.path", "uri", "path"],
    "http_method":       ["http.request.method", "request.method", "method"],
    "http_status_code":  ["http.response.status_code", "status_code", "statusCode", "response.status"],
    "response_time_ms":  ["event.duration", "duration_ms", "latency_ms", "response_time_ms",
                          "http.response.elapsed.total_seconds"],
    "merchant_id":       ["merchant_id", "labels.merchant_id", "fields.merchantId", "merchantId"],
    "client_id":         ["client_id", "labels.client_id", "fields.clientId"],
    "transaction_id":    ["transaction_id", "txn_id", "labels.txn_id", "fields.transactionId"],
    "transaction_amount":["transaction_amount", "amount", "labels.amount", "fields.amount"],
    "currency":          ["currency", "labels.currency"],
    "request_geo_country": ["source.geo.country_iso_code", "geo.country", "geoip.country_code2"],
    "request_geo_city":  ["source.geo.city_name", "geo.city", "geoip.city_name"],
    "error_message":     ["error.message", "error_message", "message", "labels.error"],
    "trace_id":          ["trace.id", "traceId", "trace_id"],
    "response_body":     ["response.body", "responseBody", "http.response.body.content"],
}


def _extract_from_aliases(source: dict, field: str) -> Any:
    """Try each alias path for a field; return first non-None value."""
    for alias in _ES_FIELD_ALIASES.get(field, [field]):
        parts = alias.split(".")
        current: Any = source
        for part in parts:
            if not isinstance(current, dict):
                current = None
                break
            current = current.get(part)
        if current is not None:
            return current
    return None


def normalize_es_hit(hit: dict) -> Optional[LogEvent]:
    """
    Convert a single Elasticsearch document hit → LogEvent.
    `hit` is the raw dict from the ES hits[].hits[] array.
    Returns None if the record is too malformed to process.
    """
    try:
        source: dict = hit.get("_source", hit)

        # Duration from ES is often in nanoseconds (ECS standard)
        raw_duration = _extract_from_aliases(source, "response_time_ms")
        if raw_duration is not None:
            duration_val = _safe_float(raw_duration)
            # Detect nanoseconds → convert to ms
            if duration_val > 1_000_000:
                duration_val /= 1_000_000
            elif duration_val > 10_000:
                # Might be microseconds
                duration_val /= 1_000
        else:
            duration_val = 0.0

        raw_body = _extract_from_aliases(source, "response_body")
        if isinstance(raw_body, str):
            import json
            try:
                raw_body = json.loads(raw_body)
            except Exception:
                raw_body = None

        return LogEvent(
            timestamp=_parse_timestamp(_extract_from_aliases(source, "timestamp")),
            source=DataSource.ELASTICSEARCH,
            api_name=str(_extract_from_aliases(source, "api_name") or "UNKNOWN"),
            api_path=str(_extract_from_aliases(source, "api_path") or "/"),
            http_method=str(_extract_from_aliases(source, "http_method") or "POST").upper(),
            http_status_code=_safe_int(_extract_from_aliases(source, "http_status_code"), 200),
            response_time_ms=duration_val,
            merchant_id=_extract_from_aliases(source, "merchant_id"),
            client_id=_extract_from_aliases(source, "client_id"),
            transaction_id=_extract_from_aliases(source, "transaction_id"),
            transaction_amount=_safe_float(_extract_from_aliases(source, "transaction_amount")) or None,
            currency=str(_extract_from_aliases(source, "currency") or "INR"),
            request_geo_country=_extract_from_aliases(source, "request_geo_country"),
            request_geo_city=_extract_from_aliases(source, "request_geo_city"),
            response_body=raw_body if isinstance(raw_body, dict) else None,
            error_message=_extract_from_aliases(source, "error_message"),
            trace_id=_extract_from_aliases(source, "trace_id"),
            raw=source,
        )
    except Exception:
        logger.exception("Failed to normalize ES hit id=%s", hit.get("_id", "unknown"))
        return None


# ---------------------------------------------------------------------------
# Prometheus normalizer
# ---------------------------------------------------------------------------

def normalize_prometheus_sample(
    metric_labels: dict[str, str],
    timestamp: float,
    value: float,
    metric_name: str,
) -> Optional[LogEvent]:
    """
    Convert a Prometheus instant query sample into a synthetic LogEvent.

    Prometheus gives us aggregated metrics (TPS, error rate), not raw logs.
    We synthesise a LogEvent so downstream classification and metric pipelines
    can treat it uniformly. Fields not available from Prometheus are left None.
    """
    try:
        api_name = (
            metric_labels.get("api_name")
            or metric_labels.get("handler")
            or metric_labels.get("job")
            or "UNKNOWN"
        )
        status_code = _safe_int(metric_labels.get("status_code", "200"))

        return LogEvent(
            timestamp=_parse_timestamp(timestamp),
            source=DataSource.PROMETHEUS,
            api_name=api_name,
            api_path=metric_labels.get("path", "/"),
            http_method=metric_labels.get("method", "POST").upper(),
            http_status_code=status_code,
            # Prometheus doesn't give per-request latency; store the value raw
            response_time_ms=value if "latency" in metric_name.lower() else 0.0,
            merchant_id=metric_labels.get("merchant_id"),
            client_id=metric_labels.get("client_id"),
            raw={
                "metric_name": metric_name,
                "value": value,
                "labels": metric_labels,
                "timestamp": timestamp,
            },
        )
    except Exception:
        logger.exception("Failed to normalize Prometheus sample: %r", metric_labels)
        return None


# ---------------------------------------------------------------------------
# Kafka / generic payload normalizer
# ---------------------------------------------------------------------------

def normalize_kafka_payload(payload: dict) -> Optional[LogEvent]:
    """
    Normalize a Kafka message payload.
    Expects a fairly flat dict — gateway logs, downstream payment events, etc.
    """
    try:
        return LogEvent(
            timestamp=_parse_timestamp(
                payload.get("timestamp") or payload.get("eventTime") or payload.get("time")
            ),
            source=DataSource.KAFKA,
            api_name=str(
                payload.get("apiName")
                or payload.get("api_name")
                or payload.get("service")
                or "UNKNOWN"
            ),
            api_path=str(payload.get("path") or payload.get("url") or "/"),
            http_method=str(payload.get("method") or payload.get("httpMethod") or "POST").upper(),
            http_status_code=_safe_int(
                payload.get("statusCode") or payload.get("httpStatus") or payload.get("status"), 200
            ),
            response_time_ms=_safe_float(
                payload.get("responseTime") or payload.get("durationMs") or payload.get("latency")
            ),
            merchant_id=payload.get("merchantId") or payload.get("merchant_id"),
            client_id=payload.get("clientId") or payload.get("client_id"),
            transaction_id=payload.get("transactionId") or payload.get("txnId"),
            transaction_amount=_safe_float(payload.get("amount") or payload.get("txnAmount")) or None,
            currency=str(payload.get("currency") or "INR"),
            request_geo_country=payload.get("country") or payload.get("geoCountry"),
            request_geo_city=payload.get("city") or payload.get("geoCity"),
            response_body=payload.get("responseBody") if isinstance(payload.get("responseBody"), dict) else None,
            error_message=payload.get("errorMessage") or payload.get("error"),
            trace_id=payload.get("traceId") or payload.get("trace_id"),
            raw=payload,
        )
    except Exception:
        logger.exception("Failed to normalize Kafka payload")
        return None
