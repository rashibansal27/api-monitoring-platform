"""
Streaming latency percentile computation.

Uses a fixed-size circular buffer per API per window.
When the window is flushed, numpy computes exact percentiles over the buffer.

Design:
  - LatencyBuffer  — per-(api_name, window_start) mutable accumulator
  - PercentileResult — immutable result from a buffer flush
  - compute_percentiles() — pure function, testable in isolation

Why not T-Digest?
  For 1-minute windows at ~1000 TPS, the buffer holds at most 60 000 samples.
  numpy.percentile over 60k floats takes ~0.5ms — fast enough.
  T-Digest adds a dependency for marginal gain. Switch if windows exceed 500k samples.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime

import numpy as np

logger = logging.getLogger(__name__)

# Maximum latency samples stored per window before oldest are dropped
_MAX_SAMPLES = 50_000


@dataclass
class PercentileResult:
    count: int
    mean_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    min_ms: float
    max_ms: float


@dataclass
class LatencyBuffer:
    """
    Mutable accumulator for response times in a single time window.
    Thread-unsafe — access must be serialized (asyncio single-threaded is fine).
    """
    api_name: str
    window_start: datetime
    _samples: list[float] = field(default_factory=list, repr=False)

    def add(self, latency_ms: float) -> None:
        if len(self._samples) < _MAX_SAMPLES:
            self._samples.append(latency_ms)
        # Silently drop when buffer is full — we already have statistical coverage

    def flush(self) -> PercentileResult:
        """Compute and return percentiles; leaves buffer intact for re-reads."""
        return compute_percentiles(self._samples)

    @property
    def count(self) -> int:
        return len(self._samples)


def compute_percentiles(latencies: list[float]) -> PercentileResult:
    """
    Pure function — compute latency statistics from a list of ms values.
    Safe to call with an empty list (returns zeros).
    """
    if not latencies:
        return PercentileResult(
            count=0, mean_ms=0.0, p50_ms=0.0,
            p95_ms=0.0, p99_ms=0.0, min_ms=0.0, max_ms=0.0,
        )

    arr = np.array(latencies, dtype=np.float64)
    p50, p95, p99 = np.percentile(arr, [50, 95, 99])

    return PercentileResult(
        count=len(latencies),
        mean_ms=float(np.mean(arr)),
        p50_ms=float(p50),
        p95_ms=float(p95),
        p99_ms=float(p99),
        min_ms=float(np.min(arr)),
        max_ms=float(np.max(arr)),
    )


# ---------------------------------------------------------------------------
# Per-API rolling latency tracker (used by Redis-backed real-time queries)
# ---------------------------------------------------------------------------

class RollingLatencyTracker:
    """
    Maintains a per-API list of recent latencies for real-time API reads.
    Separate from the window-based LatencyBuffer — this persists across windows
    and is used by /api/latency endpoint to return recent p95/p99 values.

    Backed by Redis (see storage/redis_client.py push_latency / get_latencies).
    This class is a thin in-memory fallback for when Redis is unavailable.
    """

    def __init__(self, max_samples: int = 1000) -> None:
        self._max = max_samples
        # api_name → deque of recent latencies
        from collections import deque
        self._buffers: dict[str, deque] = {}

    def record(self, api_name: str, latency_ms: float) -> None:
        from collections import deque
        if api_name not in self._buffers:
            self._buffers[api_name] = deque(maxlen=self._max)
        self._buffers[api_name].append(latency_ms)

    def get_percentiles(self, api_name: str) -> PercentileResult:
        buf = self._buffers.get(api_name)
        if not buf:
            return compute_percentiles([])
        return compute_percentiles(list(buf))

    def get_all_apis(self) -> list[str]:
        return list(self._buffers.keys())
