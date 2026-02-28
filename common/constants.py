

"""
Platform-wide constants, thresholds, and scoring weights.
All tunable values live here — no magic numbers in business logic.
"""

# ---------------------------------------------------------------------------
# Scoring weights (must sum to 1.0)
# ---------------------------------------------------------------------------
SCORE_WEIGHT_ERROR_RATE: float = 0.4
SCORE_WEIGHT_TXN_PATTERN: float = 0.3
SCORE_WEIGHT_LATENCY: float = 0.2
SCORE_WEIGHT_CLIENT_SPIKE: float = 0.1

assert abs(
    SCORE_WEIGHT_ERROR_RATE
    + SCORE_WEIGHT_TXN_PATTERN
    + SCORE_WEIGHT_LATENCY
    + SCORE_WEIGHT_CLIENT_SPIKE
    - 1.0
) < 1e-9, "Scoring weights must sum to 1.0"


# ---------------------------------------------------------------------------
# Severity thresholds (risk_score → Severity)
# ---------------------------------------------------------------------------
SEVERITY_CRITICAL_THRESHOLD: float = 0.85
SEVERITY_HIGH_THRESHOLD: float = 0.65
SEVERITY_MEDIUM_THRESHOLD: float = 0.40
SEVERITY_LOW_THRESHOLD: float = 0.20
# Below LOW → Severity.INFO


# ---------------------------------------------------------------------------
# Aggregation windows (seconds)
# ---------------------------------------------------------------------------
WINDOW_1MIN: int = 60
WINDOW_5MIN: int = 300
WINDOW_15MIN: int = 900
WINDOW_1HOUR: int = 3_600
WINDOW_24HOUR: int = 86_400
WINDOW_7DAY: int = 604_800

# Primary real-time aggregation window used by metrics engine
PRIMARY_AGGREGATION_WINDOW: int = WINDOW_1MIN


# ---------------------------------------------------------------------------
# Baseline configuration
# ---------------------------------------------------------------------------
ROLLING_BASELINE_WINDOW: int = WINDOW_7DAY         # 7-day rolling baseline
SEASONAL_BASELINE_LOOKBACK_DAYS: int = 28           # Same hour/DOW over 4 weeks
MIN_SAMPLES_FOR_BASELINE: int = 30                  # Below this, no anomaly detection


# ---------------------------------------------------------------------------
# Z-score detector
# ---------------------------------------------------------------------------
ZSCORE_ANOMALY_THRESHOLD: float = 3.0              # |Z| > 3.0 → anomaly
ZSCORE_WARNING_THRESHOLD: float = 2.0              # |Z| > 2.0 → warning


# ---------------------------------------------------------------------------
# EWMA detector
# ---------------------------------------------------------------------------
EWMA_ALPHA: float = 0.1                            # Smoothing factor (0 < α < 1)
EWMA_ANOMALY_THRESHOLD_MULTIPLIER: float = 3.0     # deviation > 3 * ewma_std → anomaly


# ---------------------------------------------------------------------------
# Rolling percentile detector
# ---------------------------------------------------------------------------
PERCENTILE_LOWER_BAND: float = 5.0                 # Below p5 → anomaly (TPS drop)
PERCENTILE_UPPER_BAND: float = 95.0                # Above p95 → anomaly (TPS spike)
PERCENTILE_WINDOW_SIZE: int = 100                  # Number of recent observations


# ---------------------------------------------------------------------------
# Isolation Forest detector
# ---------------------------------------------------------------------------
ISOLATION_FOREST_CONTAMINATION: float = 0.05       # Expected anomaly fraction
ISOLATION_FOREST_N_ESTIMATORS: int = 100
ISOLATION_FOREST_RETRAIN_INTERVAL: int = WINDOW_1HOUR  # Retrain every hour


# ---------------------------------------------------------------------------
# Burst / silence detection (client level)
# ---------------------------------------------------------------------------
BURST_MULTIPLIER: float = 3.0                      # current_tps > 3x baseline → burst
SILENCE_FRACTION: float = 0.1                      # current_tps < 10% baseline → silence
MIN_BASELINE_TPS_FOR_SILENCE: float = 1.0          # Ignore silence if baseline is tiny


# ---------------------------------------------------------------------------
# Alert deduplication / cooldown
# ---------------------------------------------------------------------------
ALERT_COOLDOWN_SECONDS: int = 300                  # Suppress same alert for 5 min
ALERT_DEDUP_WINDOW_SECONDS: int = 60               # Merge signals within 1 min


# ---------------------------------------------------------------------------
# Ingestion polling intervals (seconds)
# ---------------------------------------------------------------------------
ES_POLL_INTERVAL: int = 30                         # Poll Elasticsearch every 30s
PROMETHEUS_POLL_INTERVAL: int = 60                 # Poll Prometheus every 60s


# ---------------------------------------------------------------------------
# Latency SLO thresholds (milliseconds)
# ---------------------------------------------------------------------------
LATENCY_P95_SLO_MS: float = 500.0                 # p95 > 500ms → warn
LATENCY_P99_SLO_MS: float = 1_000.0               # p99 > 1s → critical
LATENCY_SPIKE_MULTIPLIER: float = 2.0              # current p95 > 2x baseline → anomaly


# ---------------------------------------------------------------------------
# Error rate thresholds (absolute — independent of baseline)
# ---------------------------------------------------------------------------
TECHNICAL_ERROR_RATE_WARN: float = 0.01            # >1% technical errors → warn
TECHNICAL_ERROR_RATE_CRITICAL: float = 0.05        # >5% → critical
BUSINESS_ERROR_RATE_WARN: float = 0.05             # >5% business errors → warn
BUSINESS_ERROR_RATE_CRITICAL: float = 0.15         # >15% → critical


# ---------------------------------------------------------------------------
# Client-level thresholds
# ---------------------------------------------------------------------------
CLIENT_FAILURE_RATIO_WARN: float = 0.20            # >20% failure ratio → warn
CLIENT_FAILURE_RATIO_CRITICAL: float = 0.50        # >50% → critical
CLIENT_GEO_ENTROPY_SPIKE: float = 2.0              # Sudden geo diversity spike


# ---------------------------------------------------------------------------
# Redis TTLs (seconds)
# ---------------------------------------------------------------------------
REDIS_METRIC_TTL: int = WINDOW_7DAY                # Keep raw counters for 7 days
REDIS_CLIENT_STATE_TTL: int = WINDOW_24HOUR        # Client sliding window state
REDIS_ANOMALY_SIGNAL_TTL: int = WINDOW_1HOUR       # Active anomaly signals
REDIS_BASELINE_CACHE_TTL: int = WINDOW_1HOUR       # Cached baseline values
