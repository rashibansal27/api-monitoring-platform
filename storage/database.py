"""
SQLAlchemy async engine, session factory, and FastAPI dependency.

Usage:
  # In FastAPI route:
  async def my_route(db: AsyncSession = Depends(get_db)):
      ...

  # Direct use:
  async with get_session() as session:
      session.add(obj)
      await session.commit()

TimescaleDB hypertable setup:
  After running alembic migrations (which create the tables via ORM),
  run create_hypertables() once to convert metric_points and
  client_snapshots to hypertables.
"""

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from config.settings import get_settings
from storage.models import Base

logger = logging.getLogger(__name__)

_settings = get_settings().database

# ---------------------------------------------------------------------------
# Engine — single instance for the process lifetime
# ---------------------------------------------------------------------------
engine = create_async_engine(
    _settings.async_url,
    pool_size=_settings.pool_size,
    max_overflow=_settings.max_overflow,
    echo=_settings.echo_sql,
    pool_pre_ping=True,         # Detect stale connections
    pool_recycle=3600,          # Recycle connections every hour
)

# ---------------------------------------------------------------------------
# Session factory
# ---------------------------------------------------------------------------
AsyncSessionFactory = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,     # Keep ORM objects accessible after commit
    autoflush=False,
    autocommit=False,
)


@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Context manager for a scoped async DB session.

    Usage:
        async with get_session() as session:
            session.add(record)
            await session.commit()
    """
    async with AsyncSessionFactory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI Depends()-compatible generator.
    Yields a session and commits on clean exit, rolls back on exception.
    """
    async with get_session() as session:
        yield session


# ---------------------------------------------------------------------------
# Schema management helpers
# ---------------------------------------------------------------------------

async def create_tables() -> None:
    """Create all ORM-mapped tables (dev/test only — use Alembic in production)."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables created")


async def create_hypertables() -> None:
    """
    Convert metric_points and client_snapshots to TimescaleDB hypertables.
    Idempotent — safe to call multiple times.
    Run once after initial table creation.
    """
    hypertable_ddl = [
        """
        SELECT create_hypertable(
            'metric_points', 'window_start',
            if_not_exists => TRUE,
            chunk_time_interval => INTERVAL '1 day'
        );
        """,
        """
        SELECT create_hypertable(
            'client_snapshots', 'window_start',
            if_not_exists => TRUE,
            chunk_time_interval => INTERVAL '1 day'
        );
        """,
        # Compression policy: compress chunks older than 7 days
        """
        SELECT add_compression_policy(
            'metric_points',
            compress_after => INTERVAL '7 days',
            if_not_exists => TRUE
        );
        """,
        """
        SELECT add_compression_policy(
            'client_snapshots',
            compress_after => INTERVAL '7 days',
            if_not_exists => TRUE
        );
        """,
    ]
    async with engine.begin() as conn:
        for ddl in hypertable_ddl:
            try:
                await conn.execute(__import__("sqlalchemy").text(ddl))
                logger.info("Executed hypertable DDL: %s", ddl.split("\n")[1].strip())
            except Exception as exc:
                logger.warning("Hypertable DDL skipped (may already exist): %s", exc)
    logger.info("TimescaleDB hypertables ready")


async def dispose_engine() -> None:
    """Dispose the engine — call during application shutdown."""
    await engine.dispose()
    logger.info("Database engine disposed")
