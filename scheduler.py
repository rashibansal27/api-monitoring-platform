"""
APScheduler setup — wires all periodic polling jobs.

Jobs:
  elasticsearch_poll   — fetches new ES log hits
  prometheus_poll      — runs PromQL queries

Intervals come from constants.py and are configurable per environment.
"""

import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from common.constants import ES_POLL_INTERVAL, PROMETHEUS_POLL_INTERVAL
from config.settings import get_settings

logger = logging.getLogger(__name__)

_scheduler: AsyncIOScheduler | None = None


def get_scheduler() -> AsyncIOScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = AsyncIOScheduler(timezone="UTC")
    return _scheduler


def setup_jobs(
    es_ingestor,
    prometheus_ingestor,
) -> None:
    """
    Register polling jobs on the scheduler.
    Call this once during application startup after ingestors are initialised.
    """
    settings = get_settings()
    if not settings.app.enable_scheduler:
        logger.info("Scheduler disabled via APP_ENABLE_SCHEDULER=false")
        return

    scheduler = get_scheduler()

    scheduler.add_job(
        es_ingestor.run,
        trigger="interval",
        seconds=ES_POLL_INTERVAL,
        id="elasticsearch_poll",
        name="Elasticsearch log poll",
        replace_existing=True,
        misfire_grace_time=10,
    )
    logger.info("Scheduled ES poll every %ds", ES_POLL_INTERVAL)

    scheduler.add_job(
        prometheus_ingestor.run,
        trigger="interval",
        seconds=PROMETHEUS_POLL_INTERVAL,
        id="prometheus_poll",
        name="Prometheus metrics poll",
        replace_existing=True,
        misfire_grace_time=10,
    )
    logger.info("Scheduled Prometheus poll every %ds", PROMETHEUS_POLL_INTERVAL)
