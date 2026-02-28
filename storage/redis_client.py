"""
Redis connection pool and sliding-window helpers.

Key schema (all keys prefixed with "apm:"):
  apm:tps:{api}:{window_s}            ZSET  member=event_id, score=unix_ts
  apm:err:tech:{api}:{window_s}       ZSET  sliding window of technical error timestamps
  apm:err:biz:{api}:{window_s}        ZSET  sliding window of business error timestamps
  apm:latency:{api}                   LIST  last N response_time_ms (capped at MAX_LATENCY_SAMPLES)
  apm:client:{merchant}:txn_count     STRING  TTL=24h
  apm:client:{merchant}:geos          SET    recent geo country codes, TTL=24h
  apm:baseline:{api}:{metric}:{h}:{d} STRING  cached baseline float, TTL=1h
  apm:anomaly:active:{api}            HASH   signal_id → JSON, TTL=1h
  apm:alert:cooldown:{api}:{type}     STRING  TTL=cooldown_seconds
"""

import json
import logging
from contextlib import asynccontextmanager
from typing import Any, Optional

import redis.asyncio as aioredis

from common.constants import (
    REDIS_ANOMALY_SIGNAL_TTL,
    REDIS_BASELINE_CACHE_TTL,
    REDIS_CLIENT_STATE_TTL,
    REDIS_METRIC_TTL,
)
from config.settings import get_settings

logger = logging.getLogger(__name__)

_PREFIX = "apm:"
_MAX_LATENCY_SAMPLES = 1000     # Cap per-API latency list length

# ---------------------------------------------------------------------------
# Connection pool — single instance
# ---------------------------------------------------------------------------
_settings = get_settings().redis

_pool = aioredis.ConnectionPool.from_url(
    _settings.url,
    max_connections=20,
    decode_responses=True,
)


def get_redis() -> aioredis.Redis:
    """Return a Redis client backed by the shared connection pool."""
    return aioredis.Redis(connection_pool=_pool)


async def ping() -> bool:
    """Health-check the Redis connection."""
    try:
        r = get_redis()
        return await r.ping()
    except Exception as exc:
        logger.error("Redis ping failed: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Key builders
# ---------------------------------------------------------------------------

def _k(*parts: str) -> str:
    return _PREFIX + ":".join(parts)


def tps_key(api_name: str, window_s: int) -> str:
    return _k("tps", api_name, str(window_s))

def tech_err_key(api_name: str, window_s: int) -> str:
    return _k("err", "tech", api_name, str(window_s))

def biz_err_key(api_name: str, window_s: int) -> str:
    return _k("err", "biz", api_name, str(window_s))

def latency_key(api_name: str) -> str:
    return _k("latency", api_name)

def client_txn_key(merchant_id: str) -> str:
    return _k("client", merchant_id, "txn_count")

def client_geo_key(merchant_id: str) -> str:
    return _k("client", merchant_id, "geos")

def baseline_key(api_name: str, metric: str, hour: int, dow: int) -> str:
    return _k("baseline", api_name, metric, str(hour), str(dow))

def anomaly_active_key(api_name: str) -> str:
    return _k("anomaly", "active", api_name)

def alert_cooldown_key(api_name: str, anomaly_type: str) -> str:
    return _k("alert", "cooldown", api_name, anomaly_type)


# ---------------------------------------------------------------------------
# Sliding window helpers
# ---------------------------------------------------------------------------

async def record_request(
    r: aioredis.Redis,
    api_name: str,
    timestamp: float,
    event_id: str,
    window_s: int,
) -> None:
    """
    Add a request to the TPS sliding window ZSET.
    Trims entries older than window_s seconds.
    """
    key = tps_key(api_name, window_s)
    cutoff = timestamp - window_s
    pipe = r.pipeline()
    pipe.zadd(key, {event_id: timestamp})
    pipe.zremrangebyscore(key, "-inf", cutoff)
    pipe.expire(key, REDIS_METRIC_TTL)
    await pipe.execute()


async def get_request_count(
    r: aioredis.Redis,
    api_name: str,
    window_s: int,
    now: float,
) -> int:
    """Count requests in the last window_s seconds."""
    key = tps_key(api_name, window_s)
    cutoff = now - window_s
    return await r.zcount(key, cutoff, "+inf")


async def record_error(
    r: aioredis.Redis,
    key: str,
    timestamp: float,
    event_id: str,
    window_s: int,
) -> None:
    """Generic sliding window error counter (tech or biz)."""
    cutoff = timestamp - window_s
    pipe = r.pipeline()
    pipe.zadd(key, {event_id: timestamp})
    pipe.zremrangebyscore(key, "-inf", cutoff)
    pipe.expire(key, REDIS_METRIC_TTL)
    await pipe.execute()


async def get_error_count(
    r: aioredis.Redis,
    key: str,
    window_s: int,
    now: float,
) -> int:
    cutoff = now - window_s
    return await r.zcount(key, cutoff, "+inf")


# ---------------------------------------------------------------------------
# Latency buffer
# ---------------------------------------------------------------------------

async def push_latency(
    r: aioredis.Redis,
    api_name: str,
    latency_ms: float,
) -> None:
    """Push a latency value; trim the list to MAX_LATENCY_SAMPLES."""
    key = latency_key(api_name)
    pipe = r.pipeline()
    pipe.lpush(key, latency_ms)
    pipe.ltrim(key, 0, _MAX_LATENCY_SAMPLES - 1)
    pipe.expire(key, REDIS_METRIC_TTL)
    await pipe.execute()


async def get_latencies(
    r: aioredis.Redis,
    api_name: str,
    count: int = _MAX_LATENCY_SAMPLES,
) -> list[float]:
    """Return up to `count` recent latency values as floats."""
    key = latency_key(api_name)
    raw: list[str] = await r.lrange(key, 0, count - 1)
    return [float(v) for v in raw]


# ---------------------------------------------------------------------------
# Client-level counters
# ---------------------------------------------------------------------------

async def increment_client_txn(
    r: aioredis.Redis,
    merchant_id: str,
    amount: Optional[float] = None,
) -> None:
    pipe = r.pipeline()
    pipe.incr(client_txn_key(merchant_id))
    pipe.expire(client_txn_key(merchant_id), REDIS_CLIENT_STATE_TTL)
    await pipe.execute()


async def record_client_geo(
    r: aioredis.Redis,
    merchant_id: str,
    country_code: str,
) -> None:
    key = client_geo_key(merchant_id)
    pipe = r.pipeline()
    pipe.sadd(key, country_code)
    pipe.expire(key, REDIS_CLIENT_STATE_TTL)
    await pipe.execute()


async def get_client_geos(r: aioredis.Redis, merchant_id: str) -> set[str]:
    return await r.smembers(client_geo_key(merchant_id))


# ---------------------------------------------------------------------------
# Baseline cache
# ---------------------------------------------------------------------------

async def set_baseline(
    r: aioredis.Redis,
    api_name: str,
    metric: str,
    hour: int,
    dow: int,
    value: float,
) -> None:
    key = baseline_key(api_name, metric, hour, dow)
    await r.set(key, value, ex=REDIS_BASELINE_CACHE_TTL)


async def get_baseline(
    r: aioredis.Redis,
    api_name: str,
    metric: str,
    hour: int,
    dow: int,
) -> Optional[float]:
    key = baseline_key(api_name, metric, hour, dow)
    val = await r.get(key)
    return float(val) if val is not None else None


# ---------------------------------------------------------------------------
# Active anomaly signals
# ---------------------------------------------------------------------------

async def set_active_anomaly(
    r: aioredis.Redis,
    api_name: str,
    signal_id: str,
    signal_data: dict[str, Any],
) -> None:
    key = anomaly_active_key(api_name)
    pipe = r.pipeline()
    pipe.hset(key, signal_id, json.dumps(signal_data, default=str))
    pipe.expire(key, REDIS_ANOMALY_SIGNAL_TTL)
    await pipe.execute()


async def get_active_anomalies(
    r: aioredis.Redis,
    api_name: str,
) -> dict[str, dict]:
    key = anomaly_active_key(api_name)
    raw: dict[str, str] = await r.hgetall(key)
    return {k: json.loads(v) for k, v in raw.items()}


async def clear_anomaly(r: aioredis.Redis, api_name: str, signal_id: str) -> None:
    await r.hdel(anomaly_active_key(api_name), signal_id)


# ---------------------------------------------------------------------------
# Alert cooldown
# ---------------------------------------------------------------------------

async def set_alert_cooldown(
    r: aioredis.Redis,
    api_name: str,
    anomaly_type: str,
    ttl_seconds: int,
) -> None:
    key = alert_cooldown_key(api_name, anomaly_type)
    await r.set(key, "1", ex=ttl_seconds)


async def is_alert_in_cooldown(
    r: aioredis.Redis,
    api_name: str,
    anomaly_type: str,
) -> bool:
    key = alert_cooldown_key(api_name, anomaly_type)
    return await r.exists(key) > 0
