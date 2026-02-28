"""
Mock Kafka consumer backed by asyncio.Queue.

Used in local development and CI when a real Kafka cluster is not available.
Generates realistic synthetic payment transaction events at a configurable rate.

Swap this for kafka_consumer.py by setting KAFKA_USE_MOCK=false in .env.
"""

import asyncio
import logging
import random
import uuid
from datetime import datetime, timezone

from common.schemas import LogEvent
from ingestion.base import EventBus

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Synthetic data configuration
# ---------------------------------------------------------------------------

_API_NAMES = [
    "UPI_COLLECT",
    "UPI_PAY",
    "IMPS_TRANSFER",
    "NEFT_TRANSFER",
    "RTGS_TRANSFER",
    "PAYMENT_GATEWAY",
]

_API_PATHS = {
    "UPI_COLLECT": "/api/v1/upi/collect",
    "UPI_PAY": "/api/v1/upi/pay",
    "IMPS_TRANSFER": "/api/v1/imps/transfer",
    "NEFT_TRANSFER": "/api/v1/neft/transfer",
    "RTGS_TRANSFER": "/api/v1/rtgs/transfer",
    "PAYMENT_GATEWAY": "/api/v1/payment/initiate",
}

_MERCHANTS = [f"MERCH_{i:04d}" for i in range(1, 21)]  # 20 merchants

_GEOS = [
    ("IN", "Mumbai"), ("IN", "Delhi"), ("IN", "Bangalore"),
    ("IN", "Hyderabad"), ("IN", "Chennai"), ("IN", "Pune"),
    ("SG", "Singapore"), ("AE", "Dubai"), ("US", "New York"),
]

# Weighted outcomes: (http_status, response_body_error_code, weight)
_OUTCOME_WEIGHTS = [
    (200, None,             70),    # Success
    (200, "RR",              6),    # Insufficient funds
    (200, "OTP_INVALID",     4),    # OTP failure
    (200, "U30",             3),    # Limit exceeded
    (200, "B1",              2),    # Invalid VPA
    (200, "DUPE_TXN",        2),    # Duplicate
    (200, "AML001",          1),    # AML rejection
    (500, None,              5),    # Server error
    (502, None,              3),    # Bad gateway
    (504, None,              2),    # Gateway timeout
    (408, None,              2),    # Request timeout
]

_OUTCOMES, _WEIGHTS = zip(*[(o[:2], o[2]) for o in _OUTCOME_WEIGHTS])


def _weighted_choice(outcomes, weights):
    total = sum(weights)
    r = random.uniform(0, total)
    cumulative = 0
    for outcome, weight in zip(outcomes, weights):
        cumulative += weight
        if r <= cumulative:
            return outcome
    return outcomes[-1]


def _generate_event(burst: bool = False) -> LogEvent:
    """Generate a single synthetic payment LogEvent."""
    api_name = random.choice(_API_NAMES)
    merchant_id = random.choice(_MERCHANTS)
    country, city = random.choice(_GEOS)

    status_code, error_code = _weighted_choice(_OUTCOMES, _WEIGHTS)

    # Response time: burst → higher latency
    base_latency = random.lognormvariate(4.5, 0.8)  # ~90ms median
    if burst:
        base_latency *= random.uniform(3, 8)

    response_body = None
    if status_code == 200 and error_code:
        response_body = {
            "responseCode": error_code,
            "message": f"Transaction declined: {error_code}",
            "transactionId": str(uuid.uuid4()),
        }
    elif status_code == 200:
        response_body = {
            "responseCode": "00",
            "message": "SUCCESS",
            "transactionId": str(uuid.uuid4()),
        }

    return LogEvent(
        timestamp=datetime.now(timezone.utc),
        source="mock",
        api_name=api_name,
        api_path=_API_PATHS.get(api_name, "/"),
        http_method="POST",
        http_status_code=status_code,
        response_time_ms=round(base_latency, 2),
        merchant_id=merchant_id,
        client_id=f"CLIENT_{random.randint(1, 100):04d}",
        transaction_id=str(uuid.uuid4()),
        transaction_amount=round(random.lognormvariate(7.5, 1.2), 2),  # ~₹1800 median
        currency="INR",
        request_geo_country=country,
        request_geo_city=city,
        response_body=response_body,
        error_message=f"HTTP {status_code}" if status_code >= 400 else None,
        trace_id=str(uuid.uuid4()),
        raw={},
    )


class MockKafkaConsumer:
    """
    Generates synthetic payment events at a target TPS and publishes
    them to the EventBus. Simulates normal traffic with occasional
    anomaly injections (burst, error spike, latency spike).
    """

    def __init__(
        self,
        bus: EventBus,
        target_tps: float = 50.0,
    ) -> None:
        self._bus = bus
        self._target_tps = target_tps
        self._running = False
        self._task: asyncio.Task | None = None

        # Anomaly injection state
        self._inject_burst = False
        self._inject_error_spike = False
        self._inject_latency_spike = False

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._produce_loop())
        logger.info(
            "MockKafkaConsumer started at %.1f TPS", self._target_tps
        )

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()

    async def inject_anomaly(
        self,
        burst: bool = False,
        error_spike: bool = False,
        latency_spike: bool = False,
        duration_seconds: float = 30.0,
    ) -> None:
        """
        Inject a transient anomaly for testing detectors.
        Resets automatically after duration_seconds.
        """
        self._inject_burst = burst
        self._inject_error_spike = error_spike
        self._inject_latency_spike = latency_spike
        logger.warning(
            "Anomaly injected: burst=%s error_spike=%s latency_spike=%s for %.0fs",
            burst, error_spike, latency_spike, duration_seconds,
        )
        await asyncio.sleep(duration_seconds)
        self._inject_burst = False
        self._inject_error_spike = False
        self._inject_latency_spike = False
        logger.info("Anomaly injection ended")

    async def _produce_loop(self) -> None:
        interval = 1.0 / self._target_tps
        while self._running:
            event = _generate_event(burst=self._inject_burst)

            # Error spike: force HTTP 500 on 40% of events
            if self._inject_error_spike and random.random() < 0.4:
                event = event.model_copy(update={"http_status_code": 500, "response_body": None})

            # Latency spike: multiply response time
            if self._inject_latency_spike:
                event = event.model_copy(
                    update={"response_time_ms": event.response_time_ms * random.uniform(5, 15)}
                )

            try:
                await self._bus.publish(event)
            except Exception:
                logger.exception("MockKafkaConsumer: publish failed")

            await asyncio.sleep(interval)
