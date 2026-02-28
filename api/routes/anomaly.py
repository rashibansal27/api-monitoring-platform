"""
GET /api/anomaly/client/{merchant_id}

Returns a full anomaly profile for a single merchant:
  - Current risk score and severity (from latest active Redis signals)
  - All active anomaly signals
  - Latest ClientSnapshot
  - Recent alert history from DB

GET /api/anomaly/signals (optional listing endpoint)
  - Returns recent AnomalySignals across all or one API
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Path, Query

from alerting.alert_store import alert_store
from anomaly.pipeline import anomaly_pipeline
from api.dependencies import parse_time_range
from api.schemas import (
    AlertSummary,
    AnomalyListResponse,
    ClientAnomalyResponse,
    ClientSnapshotSummary,
    SignalSummary,
)
from common.enums import Severity
from scoring.risk_scorer import risk_scorer
from storage.redis_client import get_active_anomalies, get_redis
from storage.timeseries import get_latest_client_snapshot

router = APIRouter(prefix="/anomaly", tags=["Anomaly"])
logger = logging.getLogger(__name__)


@router.get(
    "/client/{merchant_id}",
    response_model=ClientAnomalyResponse,
    summary="Client anomaly profile",
    description=(
        "Returns the current anomaly profile for a specific merchant: "
        "risk score, active anomaly signals from all detectors, "
        "latest transaction behaviour snapshot, and recent alert history."
    ),
)
async def get_client_anomaly(
    merchant_id: str = Path(..., description="Merchant ID (e.g. MERCH_0042)"),
):
    r = get_redis()

    # ── Active anomaly signals from Redis ────────────────────────────────────
    # Signals are keyed by API — collect all APIs' signals for this merchant
    raw_signals: list[SignalSummary] = []
    all_active: dict = {}

    # Check Redis for active anomaly signals across all APIs
    # (In production, maintain a merchant-level index in Redis)
    # Here we pull from the anomaly pipeline's in-memory state
    pipeline_stats = anomaly_pipeline.stats()
    active_signals_raw: list[dict] = []

    # Check redis for merchant-level signals
    try:
        # Merchant signals stored under the merchant_id key in Redis
        merchant_active = await get_active_anomalies(r, merchant_id)
        for signal_id, signal_data in merchant_active.items():
            if signal_data.get("merchant_id") == merchant_id:
                active_signals_raw.append(signal_data)
    except Exception:
        logger.exception("Redis fetch failed for merchant %s", merchant_id)

    for s in active_signals_raw:
        raw_signals.append(SignalSummary(
            signal_id=s.get("signal_id", ""),
            detector=s.get("detector", ""),
            anomaly_type=s.get("anomaly_type", ""),
            metric_name=s.get("metric_name", ""),
            observed_value=float(s.get("observed_value", 0)),
            expected_value=float(s.get("expected_value", 0)),
            deviation_score=float(s.get("deviation_score", 0)),
            confidence=float(s.get("confidence", 0)),
            description=s.get("description", ""),
            detected_at=datetime.fromisoformat(s["detected_at"]) if s.get("detected_at") else datetime.now(timezone.utc),
        ))

    # ── Compute risk score from active signals ────────────────────────────────
    from common.schemas import AnomalySignal
    from common.enums import AnomalyType, DetectorName, MetricName

    risk_score_val = 0.0
    severity = Severity.INFO

    if raw_signals:
        # Reconstruct AnomalySignal objects for scoring
        try:
            signals_for_scoring = [
                AnomalySignal(
                    detected_at=s.detected_at,
                    detector=DetectorName(s.detector) if s.detector in [d.value for d in DetectorName] else DetectorName.ZSCORE,
                    anomaly_type=AnomalyType(s.anomaly_type) if s.anomaly_type in [a.value for a in AnomalyType] else AnomalyType.ERROR_RATE_SPIKE,
                    metric_name=MetricName(s.metric_name) if s.metric_name in [m.value for m in MetricName] else MetricName.TPS,
                    merchant_id=merchant_id,
                    observed_value=s.observed_value,
                    expected_value=s.expected_value,
                    deviation_score=s.deviation_score,
                    is_anomaly=True,
                    confidence=s.confidence,
                    description=s.description,
                )
                for s in raw_signals
            ]
            scored = risk_scorer.score(signals_for_scoring)
            risk_score_val = scored.risk_score
            severity = scored.severity
        except Exception:
            logger.exception("Re-scoring failed for merchant %s", merchant_id)

    # ── Latest ClientSnapshot ─────────────────────────────────────────────────
    snapshot_orm = await get_latest_client_snapshot(merchant_id)
    snapshot_summary = None
    if snapshot_orm:
        snapshot_summary = ClientSnapshotSummary(
            merchant_id=snapshot_orm.merchant_id,
            window_start=snapshot_orm.window_start,
            txn_volume=snapshot_orm.txn_volume,
            txn_frequency_per_min=round(snapshot_orm.txn_frequency_per_min, 4),
            failure_ratio=round(snapshot_orm.failure_ratio, 4),
            avg_txn_amount=round(snapshot_orm.avg_txn_amount, 2),
            geo_entropy=round(snapshot_orm.geo_entropy, 4),
            unique_countries=snapshot_orm.unique_countries or [],
            burst_detected=snapshot_orm.burst_detected,
            silence_detected=snapshot_orm.silence_detected,
        )
    elif not raw_signals:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for merchant_id='{merchant_id}'.",
        )

    # ── Recent alerts from DB ─────────────────────────────────────────────────
    recent_alert_orms = await alert_store.get_alerts_for_merchant(
        merchant_id, limit=10, since_hours=24
    )
    recent_alerts = [
        AlertSummary(
            alert_id=str(a.id),
            created_at=a.created_at,
            severity=a.severity,
            risk_score=round(a.risk_score, 4),
            title=a.title,
            status=a.status,
        )
        for a in recent_alert_orms
    ]

    return ClientAnomalyResponse(
        merchant_id=merchant_id,
        timestamp=datetime.now(timezone.utc),
        risk_score=round(risk_score_val, 4),
        severity=severity,
        active_signals=raw_signals,
        latest_snapshot=snapshot_summary,
        recent_alerts=recent_alerts,
    )


@router.get(
    "/signals",
    response_model=AnomalyListResponse,
    summary="Recent anomaly signals",
    description=(
        "Returns recent anomaly signals persisted in the alert store. "
        "Filter by api_name and lookback hours."
    ),
)
async def get_anomaly_signals(
    api_name: str = Query(None, description="Filter to specific API"),
    hours: int = Query(default=24, ge=1, le=168, description="Lookback in hours"),
    limit: int = Query(default=100, ge=1, le=1000),
):
    from_dt, to_dt = parse_time_range(hours)

    # Query recent alerts and unpack their signals
    if api_name:
        alerts = await alert_store.get_alerts_for_api(api_name, limit=limit, since_hours=hours)
    else:
        alerts = await alert_store.get_recent_alerts(limit=limit, since_hours=hours)

    signals: list[SignalSummary] = []
    for alert in alerts:
        signal_orms = await alert_store.get_signals_for_alert(str(alert.id))
        for s in signal_orms:
            signals.append(SignalSummary(
                signal_id=str(s.id),
                detector=s.detector,
                anomaly_type=s.anomaly_type,
                metric_name=s.metric_name,
                observed_value=s.observed_value,
                expected_value=s.expected_value,
                deviation_score=s.deviation_score,
                confidence=s.confidence,
                description=s.description or "",
                detected_at=s.detected_at,
            ))

    return AnomalyListResponse(
        api_name=api_name,
        from_dt=from_dt,
        to_dt=to_dt,
        total_signals=len(signals),
        signals=signals[:limit],
    )
