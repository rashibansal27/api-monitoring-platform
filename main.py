"""
FastAPI application entrypoint.

Lifespan:
  startup  → start EventBus, Kafka consumer, ES/Prometheus ingestors, scheduler
  shutdown → gracefully stop all background tasks
"""

import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config.settings import get_settings
from ingestion.base import InMemoryEventBus
from ingestion.elasticsearch_ingestor import ElasticsearchIngestor
from ingestion.prometheus_ingestor import PrometheusIngestor
from scheduler import get_scheduler, setup_jobs

settings = get_settings()

logging.basicConfig(
    level=settings.app.log_level,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared singletons — accessible via app.state
# ---------------------------------------------------------------------------
bus = InMemoryEventBus()
es_ingestor = ElasticsearchIngestor(bus)
prom_ingestor = PrometheusIngestor(bus)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ────────────────────────────────────────────────────────────
    logger.info("Starting %s [%s]", settings.app.name, settings.app.env)

    # Initialise DB tables (dev only — use Alembic in production)
    if settings.app.env == "development":
        from storage.database import create_tables
        try:
            await create_tables()
        except Exception as exc:
            logger.warning("DB unavailable at startup — skipping table creation: %s", exc)

    # Start event bus dispatch loop
    await bus.start()
    app.state.bus = bus

    # Start Kafka consumer (mock or real)
    if settings.kafka.use_mock:
        from ingestion.mock_kafka_consumer import MockKafkaConsumer
        kafka = MockKafkaConsumer(bus, target_tps=50.0)
    else:
        from ingestion.kafka_consumer import KafkaConsumer
        kafka = KafkaConsumer(bus)

    await kafka.start()
    app.state.kafka = kafka

    # Wire classification pipeline onto the bus
    from classification.pipeline import pipeline
    await bus.subscribe(pipeline.handle)
    app.state.classification_pipeline = pipeline

    # Wire metrics layer: classification → aggregator + client profiler → writer
    from metrics.aggregator import aggregator
    from metrics.client_profiler import client_profiler
    from metrics.writer import writer

    await writer.start()
    aggregator.add_handler(writer.handle_metric_point)
    client_profiler.add_handler(writer.handle_client_snapshot)

    pipeline.add_handler(aggregator.handle)
    pipeline.add_handler(client_profiler.handle)

    await aggregator.start()
    await client_profiler.start()

    # Wire anomaly pipeline: aggregator + client_profiler → anomaly detectors
    from anomaly.pipeline import anomaly_pipeline
    aggregator.add_handler(anomaly_pipeline.handle_metric)
    client_profiler.add_handler(anomaly_pipeline.handle_snapshot)
    app.state.anomaly_pipeline = anomaly_pipeline

    # Wire scoring + alerting: anomaly signals → risk scorer → alert manager
    from scoring.risk_scorer import risk_scorer
    from alerting.alert_manager import alert_manager
    from alerting.notifiers.log_notifier import LogNotifier

    alert_manager.add_notifier(LogNotifier())

    # Optional webhook notifier (only if configured)
    webhook_url = __import__("os").environ.get("WEBHOOK_URL")
    if webhook_url:
        from alerting.notifiers.webhook_notifier import WebhookNotifier
        webhook_secret = __import__("os").environ.get("WEBHOOK_SECRET")
        alert_manager.add_notifier(WebhookNotifier(url=webhook_url, secret=webhook_secret))
        logger.info("WebhookNotifier registered → %s", webhook_url)

    risk_scorer.add_handler(alert_manager.handle)
    anomaly_pipeline.add_handler(risk_scorer.handle)

    app.state.risk_scorer = risk_scorer
    app.state.alert_manager = alert_manager

    app.state.aggregator = aggregator
    app.state.client_profiler = client_profiler
    app.state.writer = writer

    # Setup and start polling scheduler
    setup_jobs(es_ingestor, prom_ingestor)
    scheduler = get_scheduler()
    scheduler.start()
    app.state.scheduler = scheduler

    logger.info("All services started")
    yield

    # ── Shutdown ───────────────────────────────────────────────────────────
    logger.info("Shutting down...")
    scheduler.shutdown(wait=False)
    await kafka.stop()
    await bus.stop()
    await aggregator.stop()
    await client_profiler.stop()
    await writer.stop()
    await es_ingestor.stop()
    await prom_ingestor.stop()
    logger.info("Shutdown complete")


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(
    title=settings.app.name,
    version="0.1.0",
    description="Production-grade API Monitoring and Anomaly Detection Platform",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ───────────────────────────────────────────────────────────────
from api.router import router as api_router
app.include_router(api_router)


@app.get("/ping", tags=["System"])
async def ping():
    return {"status": "ok", "service": settings.app.name, "env": settings.app.env}


# ---------------------------------------------------------------------------
# Dev server entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.app.host,
        port=settings.app.port,
        reload=settings.app.env == "development",
        log_level=settings.app.log_level.lower(),
    )
