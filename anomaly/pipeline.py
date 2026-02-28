"""
Anomaly detection pipeline.

Orchestrates all detectors and fans signals out to the scoring engine.

Two input streams:
  handle_metric(MetricPoint)      → ZScore + EWMA + Percentile + IsolationForest
  handle_snapshot(ClientSnapshot) → BurstSilence

Any signals with is_anomaly=True are published to registered handlers
(the scoring engine subscribes here).

Deduplication:
  Multiple detectors often fire on the same underlying event.
  The pipeline emits ALL signals — deduplication and weighting is the
  responsibility of the scoring engine, not the detection layer.
"""

from __future__ import annotations

import logging
from typing import Callable, Awaitable

from anomaly.burst_silence_detector import BurstSilenceDetector
from anomaly.ewma_detector import EWMADetector
from anomaly.isolation_forest_detector import IsolationForestDetector
from anomaly.percentile_detector import PercentileDetector
from anomaly.zscore_detector import ZScoreDetector
from common.schemas import AnomalySignal, ClientSnapshot, MetricPoint

logger = logging.getLogger(__name__)

SignalHandler = Callable[[list[AnomalySignal]], Awaitable[None]]


class AnomalyPipeline:
    """
    Stateful anomaly pipeline.
    Holds one instance of each detector; all are long-lived singletons.
    """

    def __init__(self) -> None:
        self._zscore = ZScoreDetector()
        self._ewma = EWMADetector()
        self._percentile = PercentileDetector()
        self._isolation_forest = IsolationForestDetector()
        self._burst_silence = BurstSilenceDetector()

        self._handlers: list[SignalHandler] = []

        # Counters for observability
        self._metrics_processed: int = 0
        self._snapshots_processed: int = 0
        self._signals_emitted: int = 0

    def add_handler(self, handler: SignalHandler) -> None:
        """Register an async handler that receives lists of AnomalySignals."""
        self._handlers.append(handler)

    # ------------------------------------------------------------------
    # MetricPoint handler (from MetricsAggregator)
    # ------------------------------------------------------------------

    async def handle_metric(self, metric: MetricPoint) -> None:
        """
        Run all metric detectors on a single MetricPoint.
        Publishes anomaly signals if any detector fires.
        """
        self._metrics_processed += 1
        signals: list[AnomalySignal] = []

        # Univariate detectors — run independently, aggregate signals
        for detector in (self._zscore, self._ewma, self._percentile):
            try:
                new_signals = detector.detect(metric)
                signals.extend(new_signals)
            except Exception:
                logger.exception(
                    "%s raised on MetricPoint api=%s window=%s",
                    detector, metric.api_name, metric.window_start,
                )

        # Multivariate detector
        try:
            if_signals = self._isolation_forest.detect(metric)
            signals.extend(if_signals)
        except Exception:
            logger.exception(
                "IsolationForestDetector raised on api=%s", metric.api_name
            )

        # Only publish signals that actually fired
        active = [s for s in signals if s.is_anomaly]

        if active:
            self._signals_emitted += len(active)
            logger.info(
                "AnomalyPipeline: %d signal(s) from api=%s window=%s detectors=%s",
                len(active),
                metric.api_name,
                metric.window_start.isoformat(),
                list({s.detector.value for s in active}),
            )
            await self._publish(active)

    # ------------------------------------------------------------------
    # ClientSnapshot handler (from ClientProfiler)
    # ------------------------------------------------------------------

    async def handle_snapshot(self, snapshot: ClientSnapshot) -> None:
        """
        Run client-level detectors on a ClientSnapshot.
        """
        self._snapshots_processed += 1

        try:
            signals = self._burst_silence.detect(snapshot)
        except Exception:
            logger.exception(
                "BurstSilenceDetector raised on merchant=%s", snapshot.merchant_id
            )
            return

        active = [s for s in signals if s.is_anomaly]

        if active:
            self._signals_emitted += len(active)
            logger.info(
                "AnomalyPipeline: %d client signal(s) from merchant=%s types=%s",
                len(active),
                snapshot.merchant_id,
                [s.anomaly_type.value for s in active],
            )
            await self._publish(active)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _publish(self, signals: list[AnomalySignal]) -> None:
        for handler in self._handlers:
            try:
                await handler(signals)
            except Exception:
                logger.exception(
                    "AnomalyPipeline handler %s raised for %d signals",
                    handler, len(signals),
                )

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------

    def stats(self) -> dict:
        return {
            "metrics_processed": self._metrics_processed,
            "snapshots_processed": self._snapshots_processed,
            "signals_emitted": self._signals_emitted,
            "isolation_forest_status": self._isolation_forest.model_status(),
            "zscore_buffers": self._zscore.buffer_sizes(),
            "burst_silence_merchants": self._burst_silence.merchant_count(),
        }


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------
anomaly_pipeline = AnomalyPipeline()
