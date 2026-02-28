"""
FastAPI dependency injection helpers.

Provides:
  get_db()        → AsyncSession   (one per request, auto-commit/rollback)
  get_redis()     → aioredis.Redis (shared pool)
  get_anomaly_pipeline() → AnomalyPipeline stats (from app.state)
  parse_time_range()    → (from_dt, to_dt) validated datetime pair
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Annotated, Optional

from fastapi import Depends, HTTPException, Query, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from storage.database import get_db as _get_db
from storage.redis_client import get_redis as _get_redis


# ---------------------------------------------------------------------------
# Database session
# ---------------------------------------------------------------------------

async def get_db(request: Request):
    """
    Yields an AsyncSession for the request lifetime.
    Commits on clean exit, rolls back on exception.
    """
    async for session in _get_db():
        yield session


# ---------------------------------------------------------------------------
# Redis
# ---------------------------------------------------------------------------

def get_redis(request: Request):
    """Returns the shared Redis client (pool-backed, no per-request overhead)."""
    return _get_redis()


# ---------------------------------------------------------------------------
# App-state accessors
# ---------------------------------------------------------------------------

def get_anomaly_pipeline(request: Request):
    return getattr(request.app.state, "anomaly_pipeline", None)


def get_alert_manager(request: Request):
    return getattr(request.app.state, "alert_manager", None)


# ---------------------------------------------------------------------------
# Time range parsing
# ---------------------------------------------------------------------------

def parse_time_range(
    hours: int = Query(default=24, ge=1, le=168, description="Lookback hours (1–168)"),
) -> tuple[datetime, datetime]:
    """
    Dependency that converts a `hours` query param into a (from_dt, to_dt) pair.
    Both datetimes are timezone-aware UTC.
    """
    to_dt = datetime.now(timezone.utc)
    from_dt = to_dt - timedelta(hours=hours)
    return from_dt, to_dt


TimeRange = Annotated[tuple[datetime, datetime], Depends(parse_time_range)]


# ---------------------------------------------------------------------------
# Common query parameter validation
# ---------------------------------------------------------------------------

def validate_api_name(
    api_name: Optional[str] = Query(
        default=None,
        description="Filter to a specific API (e.g. UPI_COLLECT, IMPS_TRANSFER)",
    )
) -> Optional[str]:
    return api_name


def validate_limit(
    limit: int = Query(default=200, ge=1, le=5000, description="Max data points"),
) -> int:
    return limit
